using System.Text.Json.Serialization;

namespace SepticMonitor.Worker;

public class AlertMessage
{
    [JsonPropertyName("deviceId")]
    public string DeviceId { get; set; } = "";

    [JsonPropertyName("kind")]
    public string Kind { get; set; } = "";

    [JsonPropertyName("detail")]
    public string Detail { get; set; } = "";

    [JsonPropertyName("severity")]
    public string Severity { get; set; } = "info";

    [JsonPropertyName("timestampUtc")]
    public string TimestampUtc { get; set; } = "";
}
