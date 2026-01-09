using SimpleDb.Record;

namespace SimpleDb.Metadata;

public readonly record struct IndexStatInfo
{
    private readonly string _fieldName;
    public int IndexRecordsPerBlock { get; }
    public int BlocksAccessed { get; }
    public int RecordsOutput { get; }

    public StatInfo TableStats { get; }
    public IndexStatInfo(IndexDefinition def, StatInfo indexedTableStats, Layout indexLayout, int blockSize)
    {
        TableStats = indexedTableStats;
        _fieldName = def.FieldName;
        //Estimate the number of block accesses required to find all index records having a particular search key
        //Use table metadata to estimate the size of the index file and number of index records per block.
        //Use this information to to get the traversal cost method of the appropriate index type which provides the estimate
        int indexRecordsPerBlock = blockSize / indexLayout.SlotSize;
        IndexRecordsPerBlock = indexRecordsPerBlock;
        //here if owning table has 1000 records then we can estimate the # of blocks in the index as
        // [number of records in original table] / [index records per block]
        //this works because each row in the original table has an entry in the index (Not covering filtered indexes!)
        int estimatedNumberOfBlocks = indexedTableStats.RecordCount / indexRecordsPerBlock;
        int blocksAccessed = estimatedNumberOfBlocks / indexRecordsPerBlock; //TODO: fix

        //This assumes even distribution of records based on the values being indexed.
        //It works very well for unique values since the # of unique values == number of records.
        RecordsOutput = indexedTableStats.RecordCount / indexedTableStats.DistinctValues(def.FieldName);
    }

    public readonly int DistinctValues(string fieldName)
        => string.Equals(_fieldName, fieldName) ? 1 : TableStats.DistinctValues(fieldName);
}