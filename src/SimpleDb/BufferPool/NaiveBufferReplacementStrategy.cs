using SimpleDb.Files;

namespace SimpleDb.BufferPool
{
    public class NaiveBufferReplacementStrategy(int numBuffers, Func<int, Buffer> createBuffers) : IBufferReplacementStrategy
    {
        private int _freeBufferCount = numBuffers;
        private readonly Buffer[] _bufferPool = Enumerable.Range(0, numBuffers).Select(createBuffers).ToArray();

        public int FreeBufferCount => _freeBufferCount;

        public IEnumerable<Buffer> GetBuffers()
            => _bufferPool;

        public Buffer? Pin(BlockId blockId)
        {
            Buffer? bufferForSameBlock = null, unpinnedBuffer = null;

            foreach (Buffer item in _bufferPool)
            {
                if (item.BlockId.HasValue && item.BlockId.Value.Equals(blockId))
                {
                    //same block use this
                    bufferForSameBlock = item;
                    break;
                }
                else if (unpinnedBuffer is null && !item.Pinned)
                {
                    //we still keep going as we may find
                    //a buffer for the same block.
                    unpinnedBuffer = item;
                }
            }

            if (bufferForSameBlock is null && unpinnedBuffer is null)
                return null;

            Buffer buffer = (bufferForSameBlock ?? unpinnedBuffer)!;
            //if we are using the unpinned buffer then
            //we need to decrement the free buffer count.
            if (object.ReferenceEquals(buffer, unpinnedBuffer))
                _freeBufferCount -= 1;

            buffer.Assign(blockId);
            buffer.Pin();
            return buffer;
        }

        public void Unpin(Buffer buffer)
        {
            buffer.Unpin();
            if (!buffer.Pinned)
            {
                //if there are waiting threads notify them
                _freeBufferCount += 1;
            }
        }
    }
}
