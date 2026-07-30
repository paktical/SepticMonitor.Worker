using Microsoft.EntityFrameworkCore;
using SepticMonitor.Worker;

var builder = Host.CreateApplicationBuilder(args);

// Layered config: appsettings.json (committed defaults) -> appsettings.{Environment}.json
// -> appsettings.Local.json (git-ignored, machine-specific) -> user secrets -> environment
// variables. Later sources win, so Local can override anything without touching the repo.
builder.Configuration.AddJsonFile("appsettings.Local.json", optional: true, reloadOnChange: true);

builder.Services.Configure<MqttOptions>(builder.Configuration.GetSection(MqttOptions.SectionName));
builder.Services.Configure<DeviceOptions>(builder.Configuration.GetSection(DeviceOptions.SectionName));
builder.Services.Configure<MonitorOptions>(builder.Configuration.GetSection(MonitorOptions.SectionName));

var connectionString = builder.Configuration.GetConnectionString("MonitorDb")
                       ?? "Data Source=septic-monitor.db";
builder.Services.AddDbContextFactory<MonitorDbContext>(o => o.UseSqlite(connectionString));

builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
