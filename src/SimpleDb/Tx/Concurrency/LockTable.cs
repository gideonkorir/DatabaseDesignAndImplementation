using System.Runtime.CompilerServices;
using SimpleDb.Files;

namespace SimpleDb.Tx.Concurrency
{
    public class LockTable
    {
        private readonly object _lock = new();
        private readonly Dictionary<BlockId, int> _locks = [];


        public void SLock(BlockId blockId)
        {
            lock (_lock)
            {
                DateTime startTime = DateTime.UtcNow;
                while(HasXLock(blockId) && !HasTimedOut(startTime))
                {
                    if(!Monitor.TryEnter(_lock, 100)) //wait for 100ms
                    {
                        continue;
                    }
                }
                if (HasXLock(blockId))
                    throw new AcquireLockFailedException($"Failed to acquire s lock for bloc {blockId}");

                int lockValue = GetLockValue(blockId);
                _locks[blockId] = lockValue + 1;
            }
        }

        public void XLock(BlockId blockId)
        {
            lock (_lock)
            {
                DateTime startTime = DateTime.UtcNow; //https://github.com/dotnet/coreclr/pull/9736
                while (HasOtherSLocks(blockId) && !HasTimedOut(startTime))
                {
                    //release the lock and try re-acquire it after 100 ms
                    _ = Monitor.Wait(_lock, 100);
                }

                if (HasOtherSLocks(blockId))
                    throw new AcquireLockFailedException($"Failed to acquire XLock for block {blockId}");

                _locks[blockId] = -1;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool HasTimedOut(DateTime startTime)
            => DateTime.UtcNow.Subtract(startTime).TotalSeconds >= 10_000;

        public void UnLock(BlockId blockId)
        {
            lock (_lock)
            {
                int v = GetLockValue(blockId);
                if(v > 1)
                {
                    _locks[blockId] = v - 1;
                }
                else
                {
                    _locks.Remove(blockId);
                    Monitor.PulseAll(_lock);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HasXLock(BlockId blockId)
        {
            int v = GetLockValue(blockId);
            return v < 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HasOtherSLocks(BlockId blockId)
        {
            int v = GetLockValue(blockId);
            return v > 1;
        }

        private int GetLockValue(BlockId blockId)
        {
            lock(_lock)
            {
                if(!_locks.TryGetValue(blockId, out int x))
                {
                    x = 0;
                }
                return x;
            }
        }
    }
}
