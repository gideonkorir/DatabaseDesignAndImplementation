namespace SimpleDb.Tx.Recovery
{
    public enum LogRecordType
    {
        Checkpoint,
        Start,
        Commit,
        Rollback,
        SetInt,
        SetString
    }

}
