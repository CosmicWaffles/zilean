using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Zilean.Database.Migrations;

/// <inheritdoc />
public partial class RetryFailedDmmHashlists : Migration
{
    /// <inheritdoc />
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        // Unsupported stored lists were cached as successful empty pages.
        // Keep successful pages; only rebuild the derived negative cache.
        migrationBuilder.Sql("""DELETE FROM "ParsedPages" WHERE "EntryCount" = 0;""");
    }

    /// <inheritdoc />
    protected override void Down(MigrationBuilder migrationBuilder)
    {
        throw new NotSupportedException(
            "The derived parsed-page cache cannot be restored; rerun DMM sync to rebuild it.");
    }
}
