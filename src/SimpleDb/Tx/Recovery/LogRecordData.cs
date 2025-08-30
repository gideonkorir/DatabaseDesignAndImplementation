using SimpleDb.Files;

namespace SimpleDb.Tx.Recovery
{
    public record struct LogRecordData<T>(BlockId BlockId, int Offset, T PrevValue, T NewValue);

}
