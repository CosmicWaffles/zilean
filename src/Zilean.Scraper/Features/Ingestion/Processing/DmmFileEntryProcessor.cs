namespace Zilean.Scraper.Features.Ingestion.Processing;

/// <summary>
/// Processes extracted DMM hashlist HTML files, parsing LZ-string-encoded JSON entries
/// through a bounded channel and bulk-upserting torrent metadata.
/// </summary>
/// <param name="dmmService">The DMM service for database operations.</param>
/// <param name="torrentInfoService">The torrent info service for bulk-upsert operations.</param>
/// <param name="parseTorrentNameService">The torrent name parser for RTN parsing.</param>
/// <param name="loggerFactory">The logger factory for creating loggers.</param>
/// <param name="configuration">The application configuration.</param>
public partial class DmmFileEntryProcessor(
    DmmService dmmService,
    ITorrentInfoService torrentInfoService,
    TorrentParser parseTorrentNameService,
    ILoggerFactory loggerFactory,
    ZileanConfiguration configuration) : GenericProcessor<ExtractedDmmEntry>(loggerFactory, torrentInfoService, parseTorrentNameService, configuration)
{
    [GeneratedRegex("""<iframe src="https:\/\/debridmediamanager.com\/hashlist#(.*)"></iframe>""")]
    private static partial Regex HashCollectionMatcher { get; }
    private List<string> _filesToProcess = [];
    private readonly ObjectPool<List<ExtractedDmmEntry>> _torrentsListPool = new DefaultObjectPoolProvider().Create<List<ExtractedDmmEntry>>();
    /// <summary>
    /// Gets the mapping of already-imported DMM page filenames to their entry counts.
    /// </summary>
    public ConcurrentDictionary<string, int> ExistingPages { get; private set; } = [];
    /// <summary>
    /// Gets the mapping of newly processed DMM page filenames to their entry counts.
    /// </summary>
    public ConcurrentDictionary<string, int> NewPages { get; set; } = [];
    /// <summary>
    /// Returns the input entry unchanged — DMM entries are already <see cref="ExtractedDmmEntry"/>.
    /// </summary>
    /// <param name="input">The input entry to transform.</param>
    /// <returns>The same <see cref="ExtractedDmmEntry"/> instance.</returns>
    protected override ExtractedDmmEntry TransformToTorrent(ExtractedDmmEntry input) => input;

    /// <summary>
    /// Processes the specified DMM HTML files, parsing each page and bulk-upserting
    /// extracted torrent entries.
    /// </summary>
    /// <param name="files">The list of HTML file paths to process.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>A task representing the async operation.</returns>
    public async Task ProcessFilesAsync(List<string> files, CancellationToken cancellationToken)
    {
        var sw = Stopwatch.StartNew();
        _filesToProcess = files;
        _processedCounts.Reset();
        _logger.LogInformation("Processing {Count} DMM Files", _filesToProcess.Count);
        await ProcessAsync(ProduceEntriesAsync, cancellationToken);
        _processedCounts.WriteOutput(_configuration, sw, NewPages);
        sw.Stop();
    }

    private async Task ProduceEntriesAsync(ChannelWriter<Task<ExtractedDmmEntry>> writer, CancellationToken cancellationToken)
    {
        try
        {
            var totalFiles = _filesToProcess.Count;
            var processedFiles = 0;
            var skippedFiles = 0;
            var newTorrentsFound = 0;
            var lastProgressLog = Stopwatch.StartNew();
            var parsedPagesBuffer = new List<ParsedPages>();

            foreach (var file in _filesToProcess)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Processing canceled.");
                    break;
                }

                var fileName = Path.GetFileName(file);
                if (ExistingPages.TryGetValue(fileName, out _) || NewPages.TryGetValue(fileName, out _))
                {
                    skippedFiles++;
                    processedFiles++;
                    continue;
                }

                try
                {
                    var (torrents, page) = await ProcessPageAsync(file, fileName, cancellationToken);
                    if (page is not null)
                    {
                        NewPages.TryAdd(fileName, page.EntryCount);
                        parsedPagesBuffer.Add(page);
                    }
                    newTorrentsFound += torrents.Count;
                    foreach (var torrent in torrents)
                    {
                        await writer.WriteAsync(Task.FromResult(torrent), cancellationToken);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing file: {FileName}", fileName);
                }

                // Flush the ParsedPages buffer outside the per-file catch so DB write failures
                // propagate instead of being swallowed as per-file parse errors.
                if (parsedPagesBuffer.Count >= _configuration.Parsing.BatchSize)
                {
                    await dmmService.AddPagesToIngestedAsync(parsedPagesBuffer, cancellationToken);
                    parsedPagesBuffer.Clear();
                }

                processedFiles++;

                // Log progress every 60 seconds
                if (lastProgressLog.Elapsed.TotalSeconds >= 60)
                {
                    var percentage = totalFiles > 0 ? (double)processedFiles / totalFiles * 100 : 0;
                    _logger.LogInformation("DMM sync progress: {Processed}/{Total} files ({Percentage:F1}%), {Skipped} skipped, {NewTorrents} new torrents found",
                        processedFiles, totalFiles, percentage, skippedFiles, newTorrentsFound);
                    lastProgressLog.Restart();
                }
            }

            // Final progress log
            if (processedFiles > 0)
            {
                _logger.LogInformation("DMM sync complete: {Processed}/{Total} files processed, {Skipped} skipped, {NewTorrents} new torrents found",
                    processedFiles, totalFiles, skippedFiles, newTorrentsFound);
            }

            if (parsedPagesBuffer.Count > 0)
            {
                await dmmService.AddPagesToIngestedAsync(parsedPagesBuffer, cancellationToken);
                parsedPagesBuffer.Clear();
            }

            writer.Complete();
        }
        catch (Exception ex)
        {
            // Complete the channel with the error so the consumer's ReadAllAsync unblocks
            // instead of hanging Task.WhenAll in ProcessAsync when a DB flush throws.
            writer.Complete(ex);
            throw;
        }
    }

    private async Task<(List<ExtractedDmmEntry> Torrents, ParsedPages? Page)> ProcessPageAsync(string filePath, string filenameOnly, CancellationToken cancellationToken)
    {
        if (!File.Exists(filePath))
        {
            return ([], new ParsedPages { EntryCount = 0, Page = filenameOnly });
        }

        var pageSource = await File.ReadAllTextAsync(filePath, cancellationToken);
        var match = HashCollectionMatcher.Match(pageSource);

        if (!match.Success)
        {
            return ([], new ParsedPages { EntryCount = 0, Page = filenameOnly });
        }

        var torrents = _torrentsListPool.Get();

        try
        {
            var encodedPayload = match.Groups[1].Value;
            if (encodedPayload.StartsWith("id=", StringComparison.Ordinal))
            {
                if (!Guid.TryParseExact(encodedPayload.AsSpan(3), "D", out var listId))
                {
                    throw new InvalidDataException("The stored DMM hashlist ID is not a UUID.");
                }

                var payloadPath = Path.Combine(
                    Path.GetDirectoryName(filePath)!, "repo", "lists", $"{listId:D}.txt");
                var storedSource = await File.ReadAllTextAsync(payloadPath, cancellationToken);
                var storedMatch = HashCollectionMatcher.Match(storedSource);
                encodedPayload = storedMatch.Success ? storedMatch.Groups[1].Value : storedSource.Trim();
            }

            var decodedJson = Decompressor.FromEncodedUriComponent(encodedPayload);

            var utf8Bytes = Encoding.UTF8.GetBytes(decodedJson);
            var span = new ReadOnlySpan<byte>(utf8Bytes);
            var reader = new Utf8JsonReader(span);

            ParseTorrents(ref reader, torrents);

            if (torrents.Count == 0)
            {
                return ([], new ParsedPages { EntryCount = 0, Page = filenameOnly });
            }

            var sanitizedTorrents = torrents
                .Where(x => x.Filesize > 0)
                .GroupBy(x => x.InfoHash)
                .Select(group => group.FirstOrDefault())
                .Where(x => !string.IsNullOrEmpty(x.Filename))
                .OfType<ExtractedDmmEntry>()
                .ToList();

            return (sanitizedTorrents, new ParsedPages { EntryCount = sanitizedTorrents.Count, Page = filenameOnly });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error parsing file: {FilePath}", filePath);
            // Do not record a failed page: leaving it out of ParsedPages means the next
            // sync retries it (for example a stored list whose lists/<id>.txt was missing).
            return ([], null);
        }
        finally
        {
            torrents.Clear();
            _torrentsListPool.Return(torrents);
        }
    }

    private static ExtractedDmmEntry? ParsePageContent(ref Utf8JsonReader reader)
    {
        string? filename = null;
        long filesize = 0;
        string? hash = null;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                break;
            }

            if (reader.TokenType == JsonTokenType.PropertyName)
            {
                var propertyName = reader.GetString();
                reader.Read();

                switch (propertyName)
                {
                    case "filename":
                        filename = reader.GetString();
                        break;
                    case "bytes":
                        filesize = reader.GetInt64();
                        break;
                    case "hash":
                        hash = reader.GetString();
                        break;
                }
            }
        }

        return filename == null || hash == null
            ? null
            : new ExtractedDmmEntry(hash, filename.Replace(".", " ", StringComparison.Ordinal), filesize, null);
    }

    /// <summary>
    /// Loads previously parsed DMM pages from the database into <see cref="ExistingPages"/>.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>A task representing the async operation.</returns>
    public async Task LoadParsedPages(CancellationToken cancellationToken)
    {
        var parsedPages = await dmmService.GetIngestedPagesAsync(cancellationToken);
        if (parsedPages.Count != 0)
        {
            ExistingPages = new ConcurrentDictionary<string, int>(parsedPages.ToDictionary(x => x.Page, x => x.EntryCount));
        }

        _logger.LogInformation("Loaded {Count} previously parsed pages", ExistingPages.Count);
    }


    [SuppressMessage("Style", "IDE0010:Add missing cases")]
    private static void ParseTorrents(ref Utf8JsonReader reader, List<ExtractedDmmEntry> torrents)
    {
        if (!reader.Read())
        {
            return;
        }

        // ReSharper disable once SwitchStatementMissingSomeEnumCasesNoDefault
        switch (reader.TokenType)
        {
            case JsonTokenType.StartArray:
                ParseArray(ref reader, torrents);
                return;
            case JsonTokenType.StartObject:
                ParseNestedTorrents(ref reader, torrents);
                break;
        }
    }

    private static void ParseNestedTorrents(ref Utf8JsonReader reader, List<ExtractedDmmEntry> torrents)
    {
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.PropertyName && reader.GetString() == "torrents")
            {
                reader.Read();
                if (reader.TokenType == JsonTokenType.StartArray)
                {
                    ParseArray(ref reader, torrents);
                }
                break;
            }

            if (reader.TokenType == JsonTokenType.EndObject)
            {
                break;
            }
        }
    }

    private static void ParseArray(ref Utf8JsonReader reader, List<ExtractedDmmEntry> torrents)
    {
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.StartObject)
            {
                var entry = ParsePageContent(ref reader);
                if (entry != null)
                {
                    torrents.Add(entry);
                }
            }
            else if (reader.TokenType == JsonTokenType.EndArray)
            {
                break;
            }
        }
    }
}
