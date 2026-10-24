using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Zilean.Database.Services;
using Zilean.Scraper.Features.Ingestion.Processing;
using Zilean.Shared.Features.Python;

namespace Zilean.Tests.Tests;

public class DmmHashlistIngestionTests(DmmHashlistFixture fixture) : IClassFixture<DmmHashlistFixture>, IAsyncLifetime
{
    private const string ReportedPage = "82f68a88-16aa-4173-9f6c-6e1bad74b32d.html";
    private const string ReportedPayload = "82f68a88-16aa-4173-9f6c-6e1bad74b32d.txt";
    private const string InlinePage = "22cdc626-68ab-4a6d-925b-ca81996a812a.html";
    private const string ReportedHash = "3cc725c6f4e88d71901caac2370cc9ce49b7c6a7";
    private static readonly string[] _inlineHashes =
    [
        "56db7a265a3a6128eda320c5ad965f0c8abe48df",
        "a16c9807a95c023dd365fbd56ef833be691039eb",
        "c721438a3f1b12e1dc49dac893e738c5ee6e50c2",
        "9ae97ec6f123c392ddd69eb31dd51eb95299d902",
        "4fe49e60369a9c68bd586fd52cd120660310f9b8",
        "f361b8c9ec58134a4a731a56258295f81a8cf1d5",
        "b6262856c257faed6427b7b2da262cdc3e6b0f16",
        "4bbeeb21397a972381f75ac9d58554671e1348f0",
        "0b9f33f834e699c8b260eac9ed0c4675e4ae8671",
        "f6db5fb27cc19665b0adca12c4cc477aaec0931c",
        "110954eab4d62d177cd4ae4af85de1529764af18",
        "4cd7b210a34c5eeb0320c25485b0d164bf0b32c5",
        "c124493e0fcc74e86dd81ba7bbc65d6d44a1dd89",
        "0f3a826fb3a7168998fef41f6d29afd09841e62c",
        "82156fc96a5a4c3d61d53d29b2f5aa04fb18ee18"
    ];

    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"zilean-dmm-{Guid.NewGuid():N}");
    private readonly ITorrentInfoService _torrents = Substitute.For<ITorrentInfoService>();
    private readonly HashSet<string> _observedHashes = [];
    private ZileanDbContext _database = null!;
    private DmmService _pages = null!;
    private TorrentParser _parser = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_directory);
        var factory = fixture.Services.GetRequiredService<IDbContextFactory<ZileanDbContext>>();
        _database = await factory.CreateDbContextAsync();
        await DeleteOwnPages();
        _pages = new DmmService(factory);
        // Every hash below is reported as already existing, so the parser is never called.
        // Build a runtime that faults deterministically instead of loading CPython, as
        // IngestionPipelineTests does.
        var savedPython = Environment.GetEnvironmentVariable("ZILEAN_PYTHON_PYLIB");
        Environment.SetEnvironmentVariable("ZILEAN_PYTHON_PYLIB", "");
        try
        {
            var runtime = new PythonRuntimeService(NullLogger<PythonRuntimeService>.Instance, fixture.Configuration);
            _parser = new TorrentParser(runtime, NullLogger<TorrentParser>.Instance, fixture.Configuration);
        }
        finally
        {
            Environment.SetEnvironmentVariable("ZILEAN_PYTHON_PYLIB", savedPython);
        }
        _torrents.GetBlacklistedItems().Returns(Task.FromResult(new HashSet<string>()));
        // These real hashes already exist downstream. Exercise decoding and page persistence
        // without making this ingestion-boundary regression depend on RTN/Plex metadata.
        var existing = _inlineHashes.Append(ReportedHash).ToHashSet(StringComparer.Ordinal);
        _torrents.GetExistingInfoHashesAsync(Arg.Do<List<string>>(_observedHashes.UnionWith))
            .Returns(Task.FromResult(existing));
    }

    public async Task DisposeAsync()
    {
        await DeleteOwnPages();
        await _database.DisposeAsync();
        Directory.Delete(_directory, true);
    }

    [Fact]
    public async Task StoredPayloadImportsTheExactReleaseFromTheReportedPage()
    {
        // Byte-for-byte inputs from Thoroslives/zilean#25, Git blobs648e1353/f309f0bb.
        CopyFixture(ReportedPage, ReportedPage);
        CopyFixture($"lists/{ReportedPayload}", $"repo/lists/{ReportedPayload}");

        await Process(ReportedPage);

        await AssertPageCount(ReportedPage, 1);
        _observedHashes.Should().BeEquivalentTo(new[] { ReportedHash });
    }

    [Fact]
    public async Task InlineLegacyPagePreservesAllOriginalReleases()
    {
        // Real legacy inline-form client publication, Git blob07980a7e.
        CopyFixture(InlinePage, InlinePage);

        await Process(InlinePage);

        await AssertPageCount(InlinePage, _inlineHashes.Length);
        _observedHashes.Should().BeEquivalentTo(_inlineHashes);
    }

    [Fact]
    public async Task StoredLegacyHtmlPreservesAllOriginalReleases()
    {
        CopyFixture(ReportedPage, ReportedPage);
        // The producer's migration stores an old HTML page whole, not only its fragment.
        // Reuse the captured page bytes instead of inventing another encoder/HTML format.
        CopyFixture(InlinePage, $"repo/lists/{ReportedPayload}");

        await Process(ReportedPage);

        await AssertPageCount(ReportedPage, _inlineHashes.Length);
        _observedHashes.Should().BeEquivalentTo(_inlineHashes);
    }

    [Fact]
    public async Task MissingPayloadDoesNotPoisonThePageOrPreventFollowingPages()
    {
        CopyFixture(ReportedPage, ReportedPage);
        CopyFixture(InlinePage, InlinePage);

        await Process(ReportedPage, InlinePage);

        (await _database.ParsedPages.AnyAsync(page => page.Page == ReportedPage)).Should().BeFalse(
            "a failed payload read must remain eligible for the next normal sync");
        await AssertPageCount(InlinePage, _inlineHashes.Length);
        _observedHashes.Should().BeEquivalentTo(_inlineHashes);

        CopyFixture($"lists/{ReportedPayload}", $"repo/lists/{ReportedPayload}");
        await Process(ReportedPage);

        await AssertPageCount(ReportedPage, 1);
        _observedHashes.Should().BeEquivalentTo(_inlineHashes.Append(ReportedHash));
    }

    [Fact]
    public async Task UpgradeRetriesOldFailedPagesWithoutDroppingSuccessfulPages()
    {
        var databaseName = $"dmm_retry_{Guid.NewGuid():N}";
        await using var admin = new NpgsqlConnection(fixture.Configuration.Database.ConnectionString);
        await admin.OpenAsync();
        await using (var create = new NpgsqlCommand($"CREATE DATABASE \"{databaseName}\"", admin))
        {
            await create.ExecuteNonQueryAsync();
        }

        try
        {
            var connectionString = new NpgsqlConnectionStringBuilder(fixture.Configuration.Database.ConnectionString)
            {
                Database = databaseName
            }.ConnectionString;
            var options = new DbContextOptionsBuilder<ZileanDbContext>().UseNpgsql(connectionString).Options;
            await using var database = new ZileanDbContext(options);
            await database.GetService<IMigrator>().MigrateAsync("20260406122118_SearchV7BookThreshold");
            database.ParsedPages.Add(new() { Page = ReportedPage, EntryCount = 0 });
            database.ParsedPages.Add(new() { Page = InlinePage, EntryCount = _inlineHashes.Length });
            await database.SaveChangesAsync();

            await database.Database.MigrateAsync();

            var remaining = await database.ParsedPages.AsNoTracking().ToListAsync();
            remaining.Should().ContainSingle().Which.Should().BeEquivalentTo(new
            {
                Page = InlinePage,
                EntryCount = _inlineHashes.Length
            });
        }
        finally
        {
            await using var drop = new NpgsqlCommand($"DROP DATABASE \"{databaseName}\" WITH (FORCE)", admin);
            await drop.ExecuteNonQueryAsync();
        }
    }

    private async Task Process(params string[] pages)
    {
        var processor = new DmmFileEntryProcessor(
            _pages, _torrents, _parser, NullLoggerFactory.Instance, fixture.Configuration);
        await processor.LoadParsedPages(CancellationToken.None);
        var files = new List<string>(pages.Length);
        foreach (var page in pages)
        {
            files.Add(Path.Combine(_directory, page));
        }
        await processor.ProcessFilesAsync(files, CancellationToken.None);
    }

    private async Task AssertPageCount(string name, int expected)
    {
        var page = await _database.ParsedPages.AsNoTracking().SingleAsync(page => page.Page == name);
        page.EntryCount.Should().Be(expected);
    }

    private Task<int> DeleteOwnPages() => _database.ParsedPages
        .Where(page => page.Page == ReportedPage || page.Page == InlinePage)
        .ExecuteDeleteAsync();

    private void CopyFixture(string source, string destination)
    {
        var target = Path.Combine(_directory, destination);
        Directory.CreateDirectory(Path.GetDirectoryName(target)!);
        File.Copy(Path.Combine(AppContext.BaseDirectory, "Fixtures", "DmmHashlists", source), target);
    }
}
