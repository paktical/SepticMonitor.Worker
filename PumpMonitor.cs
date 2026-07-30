namespace SepticMonitor.Worker;

public enum PumpHealth
{
    Unknown,
    Running,
    Failed,     // stopped longer than the grace period
    Offline     // no telemetry received recently
}

public class PumpMonitor
{
    private readonly ILogger _logger;

    // --- tunables ---
    private readonly TimeSpan _failGrace = TimeSpan.FromSeconds(5);
    private readonly TimeSpan _offlineGrace = TimeSpan.FromSeconds(30);  // silent this long = offline
    // ----------------

    private PumpHealth _health = PumpHealth.Unknown;
    private DateTime _lastMessageUtc = DateTime.MinValue;
    private DateTime? _stoppedSinceUtc = null;   // when the pump first went STOPPED

    public PumpMonitor(ILogger logger) => _logger = logger;

    // Fired whenever a notable state transition occurs
    public event Action<string, string>? OnStateEvent;   // (kind, detail)

    // Call this every time a telemetry message arrives
    public void OnTelemetry(TelemetryMessage t)
    {
        _lastMessageUtc = DateTime.UtcNow;
        bool running = string.Equals(t.PumpState, "RUNNING", StringComparison.OrdinalIgnoreCase);

        if (running)
        {
            _stoppedSinceUtc = null;
            if (_health != PumpHealth.Running)
            {
                // Announce recovery from ANY prior bad state (Failed or Offline)
                if (_health == PumpHealth.Failed)
                {
                    _logger.LogWarning("RECOVERY: pump is RUNNING again after a failure.");
                    OnStateEvent?.Invoke("RECOVERY", "Pump running again after failure");
                }
                else if (_health == PumpHealth.Offline)
                {
                    _logger.LogWarning("RECOVERY: device back online, pump is RUNNING.");
                    OnStateEvent?.Invoke("RECOVERY", "Device back online, pump running");
                }
                else
                {
                    // First time we've seen it, or coming from Unknown — informational only
                    _logger.LogInformation("Pump is RUNNING.");
                }
                _health = PumpHealth.Running;
            }
        }
        else // STOPPED
        {
            // Start the stopwatch the first time we see STOPPED
            _stoppedSinceUtc ??= DateTime.UtcNow;

            var stoppedFor = DateTime.UtcNow - _stoppedSinceUtc.Value;

            if (stoppedFor >= _failGrace && _health != PumpHealth.Failed)
            {
                _health = PumpHealth.Failed;
                _logger.LogError("PUMP FAILURE: stopped for {Minutes:F1} minutes.", stoppedFor.TotalMinutes);
                OnStateEvent?.Invoke("FAILURE", $"Stopped for {stoppedFor.TotalMinutes:F1} minutes");
            }
            else if (_health != PumpHealth.Failed)
            {
                _logger.LogInformation("Pump STOPPED for {Seconds:F0}s (grace {Grace:F0}s).",
                    stoppedFor.TotalSeconds, _failGrace.TotalSeconds);
            }
        }
    }

    // Call this on a timer to catch the "device went silent" case
    public void CheckOffline()
    {
        if (_lastMessageUtc == DateTime.MinValue) return; // never heard from it yet

        var silentFor = DateTime.UtcNow - _lastMessageUtc;
        if (silentFor >= _offlineGrace && _health != PumpHealth.Offline)
        {
            _health = PumpHealth.Offline;
            _logger.LogError("DEVICE OFFLINE: no telemetry for {Seconds:F0}s.", silentFor.TotalSeconds);
            OnStateEvent?.Invoke("OFFLINE", $"No telemetry for {silentFor.TotalSeconds:F0}s");
        }
    }
}