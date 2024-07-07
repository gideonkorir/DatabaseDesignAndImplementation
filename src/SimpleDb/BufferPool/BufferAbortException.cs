using SimpleDb.Files;

namespace SimpleDb.BufferPool
{
    public class BufferAbortException(BlockId blockId) : Exception($"Unable to acquire buffer for block: {blockId}")
    {
    }
}
