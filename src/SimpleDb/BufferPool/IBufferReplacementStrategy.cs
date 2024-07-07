using SimpleDb.Files;

namespace SimpleDb.BufferPool
{
    public interface IBufferReplacementStrategy
    {
        int FreeBufferCount { get; }
        Buffer? Pin(BlockId blockId);

        void Unpin(Buffer buffer);

        IEnumerable<Buffer> GetBuffers();
    }
}
