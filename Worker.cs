using MQTTnet;
using MQTTnet.Protocol;
using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;

namespace SepticMonitor.Worker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;

    // --- match your broker + ESP32 ---
    private const string BrokerHost = "localhost";   // broker runs on this same PC
    private const int    BrokerPort = 1883;
    private const string TopicFilter = "septic/#";
    private const string TopicAlert  = "septic/hp80/alert";
    // ---------------------------------

    private IMqttClient? _mqttClient;
    private PumpMonitor? _monitor;

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
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
            DeviceId     = "hp80-01",
            Kind         = kind,
            Detail       = detail,
            Severity     = severity,
            TimestampUtc = DateTime.UtcNow.ToString("o")
        };

        var json = JsonSerializer.Serialize(alert);

        var msg = new MqttApplicationMessageBuilder()
            .WithTopic(TopicAlert)
            .WithPayload(json)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce) // QoS 1 - don't lose alerts
            .WithRetainFlag(true)   // last alert stays available for late subscribers
            .Build();

        await _mqttClient.PublishAsync(msg);
        _logger.LogInformation("Alert published to {Topic}: {Json}", TopicAlert, json);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
 using (var db = new MonitorDbContext())
        {
            db.Database.EnsureCreated();
            _logger.LogInformation("Database ready.");
        }

        _monitor = new PumpMonitor(_logger);
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
                using var db = new MonitorDbContext();
                db.Events.Add(new Event
                {
                    OccurredUtc = DateTime.UtcNow,
                    DeviceId    = "hp80-01",
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
                        using var db = new MonitorDbContext();
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

        var options = new MqttClientOptionsBuilder()
            .WithTcpServer(BrokerHost, BrokerPort)
            .WithClientId("septic-worker")
            .Build();

        // Connect (retry loop in case the broker isn't up yet)
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                _logger.LogInformation("Connecting to broker {Host}:{Port}...", BrokerHost, BrokerPort);
                await _mqttClient.ConnectAsync(options, stoppingToken);

                await _mqttClient.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(TopicFilter)
                        .Build(),
                    stoppingToken);

                _logger.LogInformation("Connected and subscribed to {Filter}", TopicFilter);
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError("Connect failed: {Message}. Retrying in 3s.", ex.Message);
                await Task.Delay(3000, stoppingToken);
            }
        }

        // Keep the service alive; messages arrive via the event handler above
        while (!stoppingToken.IsCancellationRequested)
        {
             _monitor?.CheckOffline();
            await Task.Delay(1000, stoppingToken);
        }
    }
}