namespace SepticMonitor.Worker;

public class MqttOptions
{
    public const string SectionName = "Mqtt";

    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 1883;
    public string ClientId { get; set; } = "septic-worker";
    public string TopicFilter { get; set; } = "septic/#";
    public string AlertTopic { get; set; } = "septic/hp80/alert";

    // Leave both null for an anonymous broker. Never commit real values -
    // put them in appsettings.Local.json or user secrets.
    public string? Username { get; set; }
    public string? Password { get; set; }

    public int ReconnectDelaySeconds { get; set; } = 3;

    // Retain the last alert so late subscribers see it immediately.
    public bool RetainAlerts { get; set; } = true;
}

public class DeviceOptions
{
    public const string SectionName = "Device";

    public string Id { get; set; } = "hp80-01";
}

public class MonitorOptions
{
    public const string SectionName = "Monitor";

    // Pump stopped this long counts as a failure.
    public double FailGraceSeconds { get; set; } = 5;

    // No telemetry for this long counts as the device being offline.
    public double OfflineGraceSeconds { get; set; } = 30;

    // How often the offline check runs.
    public double PollIntervalSeconds { get; set; } = 1;

    public TimeSpan FailGrace => TimeSpan.FromSeconds(FailGraceSeconds);
    public TimeSpan OfflineGrace => TimeSpan.FromSeconds(OfflineGraceSeconds);
    public TimeSpan PollInterval => TimeSpan.FromSeconds(PollIntervalSeconds);
}
