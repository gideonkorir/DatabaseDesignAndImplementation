namespace SimpleDb.Tx.Recovery
{
    public record struct LogRecordHeader(LogRecordType RecordType, int TxNumber);

}
