using SimpleDb.Files;

namespace SimpleDb.BufferPool
{
    public class ClockBufferReplacementStrategy(int numBuffers, Func<int, Buffer> createBuffers) : IBufferReplacementStrategy
    {
        private readonly Buffer[] _buffers = Enumerable.Range(0, numBuffers).Select(createBuffers).ToArray();
        private readonly Dictionary<BlockId, int> _pinnedBuffers = [];
        private int _clockIndex = 0;

        public int FreeBufferCount => _buffers.Length - _pinnedBuffers.Count;

        public IEnumerable<Buffer> GetBuffers()
        {
            foreach (var buffer in _buffers)
            {
                yield return buffer;
            }
        }

        public Buffer? Pin(BlockId blockId)
        {
            if(_pinnedBuffers.TryGetValue(blockId, out var index))
                return _buffers[index];
            Buffer? buffer = null;
            int usedBufferIndex = -1;
            for(int i=0; i< _buffers.Length; i++)
            {
                int slot = (_clockIndex + i) % _buffers.Length;
                if (!_buffers[slot].Pinned)
                {
                    buffer = _buffers[slot];
                    usedBufferIndex = slot;
                    _clockIndex = (slot + 1) % _buffers.Length;
                    break;
                }
            }

            if (buffer == null)
                return null;

            _pinnedBuffers.Add(blockId, usedBufferIndex);
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
                if(buffer.BlockId.HasValue)
                    _pinnedBuffers.Remove(buffer.BlockId.Value);
            }
        }
    }
}
