using System.Text.Json.Serialization;

namespace SepticMonitor.Worker;

public class TelemetryMessage
{
    [JsonPropertyName("deviceId")]
    public string? DeviceId { get; set; }

    [JsonPropertyName("fwVersion")]
    public string? FwVersion { get; set; }

    [JsonPropertyName("uptimeSeconds")]
    public long UptimeSeconds { get; set; }

    [JsonPropertyName("wifiRssi")]
    public int WifiRssi { get; set; }

    [JsonPropertyName("spreadRaw")]
    public int SpreadRaw { get; set; }

    [JsonPropertyName("currentRms")]
    public double CurrentRms { get; set; }

    [JsonPropertyName("pumpState")]
    public string? PumpState { get; set; }

    // These two are optional — only present when the temp reading is valid
    [JsonPropertyName("chipTempC")]
    public double? ChipTempC { get; set; }

    [JsonPropertyName("chipTempF")]
    public double? ChipTempF { get; set; }
}