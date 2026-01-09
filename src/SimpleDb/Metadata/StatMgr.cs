using System.Transactions;
using SimpleDb.Record;

namespace SimpleDb.Metadata;

public class StatMgr
{
    private long numberOfCalls = -1;
    private readonly CatalogManager _tableMgr;

    private readonly Dictionary<string, StatInfo> _tableStats = new(StringComparer.OrdinalIgnoreCase);

    private readonly ReaderWriterLockSlim _tableLock = new();

    public StatMgr(CatalogManager tableMgr, Tx.Transaction tx)
    {
        ArgumentNullException.ThrowIfNull(tx);
        _tableMgr = tableMgr ?? throw new ArgumentNullException(nameof(tableMgr));
        RefreshStats(tx);
    }

    public StatInfo GetStatInfo(string tableName, Tx.Transaction tx)
    {
        long calls = Interlocked.Increment(ref numberOfCalls);
        //refresh stats every 100 calls
        if (calls % 100 == 0)
        {
            RefreshStats(tx);
        }

        _tableLock.EnterReadLock();
        try
        {
            return _tableStats.TryGetValue(tableName, out var stats)
                ? stats
                : new StatInfo(0, 0); //not found - must be new table.
        }
        finally
        {
            if (_tableLock.IsReadLockHeld)
                _tableLock.ExitReadLock();
        }
    }

    public IndexStatInfo GetIndexStats(IndexDefinition indexDefinition, Tx.Transaction tx)
    {
        ArgumentNullException.ThrowIfNull(indexDefinition);
        StatInfo tableStats = GetStatInfo(indexDefinition.TableName, tx);
        Layout indexLayout = _tableMgr.GetIndexLayout(indexDefinition, tx);
        return new IndexStatInfo(indexDefinition, tableStats, indexLayout, tx.BlockSize);
    }

    private void RefreshStats(Tx.Transaction tx)
    {
        _tableLock.EnterWriteLock();//first time - get stats for all tables
        try
        {
            foreach (var tbl in _tableMgr.GetTableNames(tx))
            {
                RefreshStats(tbl, tx);
            }
        }
        finally
        {
            if (_tableLock.IsWriteLockHeld)
                _tableLock.ExitWriteLock();
        }
    }

    private StatInfo RefreshStats(string tableName, Tx.Transaction tx)
    {
        int numberOfRecords = 0;
        int numberOfBlocks = 0;
        using var scan = new TableScan(tx, tableName, _tableMgr.GetTableLayout(tableName, tx));
        while (scan.Next())
        {
            numberOfRecords++;
            numberOfBlocks = scan.RID.BlockId + 1; //block number is 0-based
        }
        var newStats = new StatInfo(numberOfBlocks, numberOfRecords);
        _tableStats[tableName] = newStats; //add or update
        return newStats;
    }
}