namespace Zilean.Tests.Fixtures;

public class DmmHashlistFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16.3-alpine3.20")
        .WithDatabase("zilean")
        .WithUsername("postgres")
        .WithPassword("postgres")
        .Build();

    public ZileanConfiguration Configuration { get; } = new();
    public ServiceProvider Services { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();
        Configuration.Database.ConnectionString = _postgres.GetConnectionString();
        Services = new ServiceCollection()
            .AddDbContextFactory<ZileanDbContext>(options => options.UseNpgsql(Configuration.Database.ConnectionString))
            .BuildServiceProvider();

        await using var database = await Services.GetRequiredService<IDbContextFactory<ZileanDbContext>>().CreateDbContextAsync();
        await database.Database.MigrateAsync();
    }

    public async Task DisposeAsync()
    {
        await Services.DisposeAsync();
        await _postgres.DisposeAsync();
    }
}
