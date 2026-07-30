using Microsoft.EntityFrameworkCore;

namespace SepticMonitor.Worker;

// One row per telemetry message received
public class Reading
{
    public int Id { get; set; }
    public DateTime ReceivedUtc { get; set; }
    public string? DeviceId { get; set; }
    public int SpreadRaw { get; set; }
    public double CurrentRms { get; set; }
    public string? PumpState { get; set; }
    public double? ChipTempC { get; set; }
    public int WifiRssi { get; set; }
    public long UptimeSeconds { get; set; }
}

// One row per state transition (failure, offline, recovery, etc.)
public class Event
{
    public int Id { get; set; }
    public DateTime OccurredUtc { get; set; }
    public string? DeviceId { get; set; }
    public string Kind { get; set; } = "";     // e.g. "FAILURE", "OFFLINE", "RECOVERY"
    public string? Detail { get; set; }
}

public class MonitorDbContext : DbContext
{
    // Connection string comes from configuration (ConnectionStrings:MonitorDb).
    public MonitorDbContext(DbContextOptions<MonitorDbContext> options) : base(options) { }

    public DbSet<Reading> Readings => Set<Reading>();
    public DbSet<Event> Events => Set<Event>();
}