namespace SimpleDb.Metadata;

public record struct StatInfo(int BlockCount, int RecordCount)
{
    public int RecordsPerBlock => BlockCount == 0 ? 0 : RecordCount / BlockCount;

    public int DistinctValues(string fieldName)
        => 1 + RecordCount / 3;
}