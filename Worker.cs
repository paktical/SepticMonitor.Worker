using MQTTnet;
using MQTTnet.Protocol;
using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace SepticMonitor.Worker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly MqttOptions _mqtt;
    private readonly DeviceOptions _device;
    private readonly MonitorOptions _monitorOptions;
    private readonly IDbContextFactory<MonitorDbContext> _dbFactory;

    private IMqttClient? _mqttClient;
    private PumpMonitor? _monitor;

    public Worker(
        ILogger<Worker> logger,
        IOptions<MqttOptions> mqtt,
        IOptions<DeviceOptions> device,
        IOptions<MonitorOptions> monitorOptions,
        IDbContextFactory<MonitorDbContext> dbFactory)
    {
        _logger         = logger;
        _mqtt           = mqtt.Value;
        _device         = device.Value;
        _monitorOptions = monitorOptions.Value;
        _dbFactory      = dbFactory;
    }

    private async Task PublishAlertAsync(string kind, string detail)
    {
        if (_mqttClient is null || !_mqttClient.IsConnected)
        {
            _logger.LogWarning("Cannot publish alert - MQTT not connected.");
            return;
        }

        // Map kind -> severity
        string severity = kind switch
        {
            "FAILURE"  => "critical",
            "OFFLINE"  => "critical",
            "RECOVERY" => "info",
            _          => "warning"
        };

        var alert = new AlertMessage
        {
            DeviceId     = _device.Id,
            Kind         = kind,
            Detail       = detail,
            Severity     = severity,
            TimestampUtc = DateTime.UtcNow.ToString("o")
        };

        var json = JsonSerializer.Serialize(alert);

        var msg = new MqttApplicationMessageBuilder()
            .WithTopic(_mqtt.AlertTopic)
            .WithPayload(json)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce) // QoS 1 - don't lose alerts
            .WithRetainFlag(_mqtt.RetainAlerts)   // last alert stays available for late subscribers
            .Build();

        await _mqttClient.PublishAsync(msg);
        _logger.LogInformation("Alert published to {Topic}: {Json}", _mqtt.AlertTopic, json);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using (var db = await _dbFactory.CreateDbContextAsync(stoppingToken))
        {
            await db.Database.EnsureCreatedAsync(stoppingToken);
            _logger.LogInformation("Database ready.");
        }

        _monitor = new PumpMonitor(_logger, _monitorOptions);
        _monitor.OnStateEvent += async (kind, detail) =>
        {
            // Announce the alert over MQTT (headless-friendly, decoupled)
            try
            {
                await PublishAlertAsync(kind, detail);
            }
            catch (Exception ex)
            {
                _logger.LogError("Alert publish failed: {Message}", ex.Message);
            }

            // Persist the event
            try
            {
                using var db = _dbFactory.CreateDbContext();
                db.Events.Add(new Event
                {
                    OccurredUtc = DateTime.UtcNow,
                    DeviceId    = _device.Id,
                    Kind        = kind,
                    Detail      = detail
                });
                db.SaveChanges();
            }
            catch (Exception ex)
            {
                _logger.LogError("DB write (event) failed: {Message}", ex.Message);
            }
        };

        var factory = new MqttClientFactory();
        _mqttClient = factory.CreateMqttClient();

        // Called every time a message arrives
        _mqttClient.ApplicationMessageReceivedAsync += e =>
        {
            var topic = e.ApplicationMessage.Topic;
            var payload = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);

            // The status topic carries plain text ("online"/"offline"), not JSON
            if (topic.EndsWith("/status"))
            {
                _logger.LogInformation("STATUS: device is {Status}", payload);
                return Task.CompletedTask;
            }

            // Telemetry topic carries JSON — parse it
            if (topic.EndsWith("/telemetry"))
            {
                try
                {
                    var t = JsonSerializer.Deserialize<TelemetryMessage>(payload);
                    if (t is null)
                    {
                        _logger.LogWarning("Telemetry parsed to null: {Payload}", payload);
                        return Task.CompletedTask;
                    }

                    var tempStr = t.ChipTempC.HasValue
                        ? $"{t.ChipTempC:F1}C"
                        : "n/a";

                    _logger.LogInformation(
                        "{Device} | pump={Pump} | spread={Spread} | {Amps:F3}A | temp={Temp} | rssi={Rssi} | up={Up}s",
                        t.DeviceId, t.PumpState, t.SpreadRaw, t.CurrentRms, tempStr, t.WifiRssi, t.UptimeSeconds);
                
                    _monitor?.OnTelemetry(t);  

try
                    {
                        using var db = _dbFactory.CreateDbContext();
                        db.Readings.Add(new Reading
                        {
                            ReceivedUtc   = DateTime.UtcNow,
                            DeviceId      = t.DeviceId,
                            SpreadRaw     = t.SpreadRaw,
                            CurrentRms    = t.CurrentRms,
                            PumpState     = t.PumpState,
                            ChipTempC     = t.ChipTempC,
                            WifiRssi      = t.WifiRssi,
                            UptimeSeconds = t.UptimeSeconds
                        });
                        db.SaveChanges();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError("DB write (reading) failed: {Message}", ex.Message);
                    }

                }
                catch (JsonException ex)
                {
                    _logger.LogError("Bad JSON: {Message} | payload={Payload}", ex.Message, payload);
                }
                return Task.CompletedTask;
            }

            // Anything else
            _logger.LogInformation("MQTT [{Topic}]: {Payload}", topic, payload);
            return Task.CompletedTask;
        };

        var optionsBuilder = new MqttClientOptionsBuilder()
            .WithTcpServer(_mqtt.Host, _mqtt.Port)
            .WithClientId(_mqtt.ClientId);

        // Only send credentials if the broker actually requires them.
        if (!string.IsNullOrWhiteSpace(_mqtt.Username))
            optionsBuilder = optionsBuilder.WithCredentials(_mqtt.Username, _mqtt.Password ?? "");

        var options = optionsBuilder.Build();

        var reconnectDelay = TimeSpan.FromSeconds(_mqtt.ReconnectDelaySeconds);

        // Connect (retry loop in case the broker isn't up yet)
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                _logger.LogInformation("Connecting to broker {Host}:{Port}...", _mqtt.Host, _mqtt.Port);
                await _mqttClient.ConnectAsync(options, stoppingToken);

                await _mqttClient.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(_mqtt.TopicFilter)
                        .Build(),
                    stoppingToken);

                _logger.LogInformation("Connected and subscribed to {Filter}", _mqtt.TopicFilter);
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError("Connect failed: {Message}. Retrying in {Delay}s.",
                    ex.Message, _mqtt.ReconnectDelaySeconds);
                await Task.Delay(reconnectDelay, stoppingToken);
            }
        }

        // Keep the service alive; messages arrive via the event handler above
        while (!stoppingToken.IsCancellationRequested)
        {
            _monitor?.CheckOffline();
            await Task.Delay(_monitorOptions.PollInterval, stoppingToken);
        }
    }
}