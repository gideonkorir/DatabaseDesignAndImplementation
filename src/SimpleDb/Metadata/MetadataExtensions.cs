using SimpleDb.Record;

namespace SimpleDb.Metadata;

public static class MetadataExtensions
{
    public static IEnumerable<string> GetTableNames(this TableManager tableMgr, Tx.Transaction tx)
    {
        ArgumentNullException.ThrowIfNull(tableMgr);

        ArgumentNullException.ThrowIfNull(tx);

        using var scan = new TableScan(tx, TableManager.TableCatalog, tableMgr.GetTableLayout(TableManager.TableCatalog, tx));
        List<string> tableNames = new();
        while (scan.Next())
        {
            string tableName = scan.GetString("tblname") ?? throw new InvalidOperationException("Table name is null");
            tableNames.Add(tableName);
        }
        return tableNames;
    }
}