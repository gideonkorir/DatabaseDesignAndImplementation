using SimpleDb.Files;

namespace SimpleDb.BufferPool
{
    /// <summary>
    /// Strategy based on least recently modified
    /// </summary>
    /// <param name="numBuffers"></param>
    /// <param name="bufferFactory"></param>
    public class LRMBufferReplacementStrategy(int numBuffers, Func<int, Buffer> bufferFactory) : IBufferReplacementStrategy
    {
        /// <summary>
        /// This is a min heap. The item with the lowest priority i.e., lowest datetime will be top of the heap & is the first one
        /// to be dequeued.
        /// </summary>
        private readonly PriorityQueue<Buffer, DateTime> _availableBuffers = new(
            Enumerable.Range(0, numBuffers)
            .Select(c => bufferFactory(c))
            .Select(c => (c, c.LastModified.GetValueOrDefault())
            ));
        private readonly HashSet<Buffer> _pinnedBuffers = [];

        public int FreeBufferCount => _availableBuffers.Count;

        public IEnumerable<Buffer> GetBuffers()
        {
            foreach ((Buffer buffer, _) in _availableBuffers.UnorderedItems)
            {
                yield return buffer;
            }

            foreach (Buffer buffer in _pinnedBuffers)
                yield return buffer;
        }

        public Buffer? Pin(BlockId blockId)
        {
            Buffer? buffer = _pinnedBuffers.FirstOrDefault(c => c.BlockId == blockId);
            if (buffer == null && !_availableBuffers.TryDequeue(out buffer, out DateTime priority))
            {
                return null;
            }
            buffer.Pin();
            buffer.Assign(blockId);
            _pinnedBuffers.Add(buffer);
            return buffer;
        }

        public void Unpin(Buffer buffer)
        {
            buffer.Unpin();
            if (!buffer.Pinned)
            {
                _pinnedBuffers.Remove(buffer);
                _availableBuffers.Enqueue(buffer, buffer.LastModified.GetValueOrDefault());
            }
        }
    }
}
