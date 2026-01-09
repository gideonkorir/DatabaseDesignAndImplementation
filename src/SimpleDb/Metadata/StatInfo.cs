namespace SimpleDb.Metadata;

public record struct StatInfo(int BlockCount, int RecordCount)
{
    public readonly int RecordsPerBlock => BlockCount == 0 ? 0 : RecordCount / BlockCount;

    public readonly int DistinctValues(string fieldName)
        => 1 + RecordCount / 3;
}
