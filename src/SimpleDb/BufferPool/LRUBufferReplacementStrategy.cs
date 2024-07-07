using SimpleDb.Files;
using System.Diagnostics.CodeAnalysis;

namespace SimpleDb.BufferPool
{
    /// <summary>
    /// This strategy will try pin the oldest unpinned buffer.
    /// </summary>
    /// <param name="numBuffers"></param>
    /// <param name="bufferFactory"></param>
    public class LRUBufferReplacementStrategy(int numBuffers, Func<int, Buffer> bufferFactory) : IBufferReplacementStrategy
    {
        private readonly Queue<Buffer> _availableBuffers = new(Enumerable.Range(0, numBuffers).Select(c => bufferFactory(c)));
        private readonly HashSet<Buffer> _pinnedBuffers = new(numBuffers, new RefEqualityComparer());
        public int FreeBufferCount => _availableBuffers.Count;

        public IEnumerable<Buffer> GetBuffers()
        {
            foreach (var buffer in _availableBuffers)
                yield return buffer;
            foreach (var buffer in _pinnedBuffers)
                yield return buffer;
        }

        public Buffer? Pin(BlockId blockId)
        {
            Buffer? buffer = _pinnedBuffers.FirstOrDefault(c => c.BlockId == blockId);
            if(buffer == null && !_availableBuffers.TryDequeue(out buffer))
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
            if(!buffer.Pinned)
            {
                _pinnedBuffers.Remove(buffer);
                _availableBuffers.Enqueue(buffer);
            }
        }

        private class RefEqualityComparer : IEqualityComparer<Buffer>
        {
            public bool Equals(Buffer? x, Buffer? y)
                => ReferenceEquals(x, y);

            public int GetHashCode([DisallowNull] Buffer obj)
            {
                return obj.GetHashCode();
            }
        }
    }
}
