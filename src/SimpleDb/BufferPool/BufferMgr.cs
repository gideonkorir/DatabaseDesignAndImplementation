using SimpleDb.Files;
using SimpleDb.Wal;

namespace SimpleDb.BufferPool
{
    public class BufferMgr
    {
        private static readonly TimeSpan s_maxTime = TimeSpan.FromSeconds(1);

        private readonly object _lock = new();
        private readonly IBufferReplacementStrategy _replacementStrategy;
        public int FreeBufferCount => _replacementStrategy.FreeBufferCount;

        public BufferMgr(FileManager fileManager, LogManager logManager, int poolSize, BufferReplacementStrategy replacementStrategy) 
        {
            _replacementStrategy = replacementStrategy switch
            {
                BufferReplacementStrategy.Naive => new NaiveBufferReplacementStrategy(poolSize, index => new Buffer(fileManager, logManager)),
                BufferReplacementStrategy.LRU => new LRUBufferReplacementStrategy(poolSize, index => new Buffer(fileManager, logManager)),
                BufferReplacementStrategy.LRM => new LRMBufferReplacementStrategy(poolSize, index => new Buffer(fileManager, logManager)),
                BufferReplacementStrategy.Clock => new ClockBufferReplacementStrategy(poolSize, index => new Buffer(fileManager, logManager)),
                _ => throw new NotImplementedException(nameof(replacementStrategy))
            };
        }

        public void FlushAll(int txNumber)
        {
            lock (_lock)
            {

                foreach (Buffer buffer in _replacementStrategy.GetBuffers())
                    if (buffer.ModifyingTransaction == txNumber)
                        buffer.Flush();
            }
        }

        public void Unpin(Buffer buffer)
        {
            lock (_lock)
            {
                _replacementStrategy.Unpin(buffer);
                if(!buffer.Pinned)
                    Monitor.PulseAll(_lock); //notify all waiters we have a newly unpinned block
            }
        }

        public Buffer Pin(BlockId blockId)
        {
            CancellationTokenSource tokenSource = new(s_maxTime);
            Buffer? buffer;
            lock (_lock)
            {
                do
                {
                    buffer = _replacementStrategy.Pin(blockId);
                    if (buffer == null)
                        Monitor.Wait(_lock, 100); //if we don't find the buffer we release lock and wait a bit
                }
                while (!tokenSource.IsCancellationRequested && buffer is null);
            }
            if(buffer is null)
            {
                throw new BufferAbortException(blockId);
            }
            return buffer;
        }
    }
}
