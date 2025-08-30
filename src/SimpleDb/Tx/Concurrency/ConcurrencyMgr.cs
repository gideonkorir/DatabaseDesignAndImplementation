using SimpleDb.Files;

namespace SimpleDb.Tx.Concurrency
{
    public class ConcurrencyMgr
    {
        private static readonly LockTable _lockTable = new();
        private readonly Dictionary<BlockId, char> _locks = [];

        public void SLock(BlockId blockId)
        {
            if(_locks.TryAdd(blockId, 'S'))
            {
                _lockTable.SLock(blockId);
            }
        }

        public void XLock(BlockId blockId)
        {
            if (!HasXLock(blockId))
            {
                //The call to SLock will block until there is no other transaction
                //with an XLock on the block pointed to by BlockId.
                //The XLock is incompatible with the S lock however, in this case we 
                //are upgrading from S to X. The call in LockTable.HasOtherSLocks will
                //be true for us when all other S locks have been released.
                SLock(blockId);
                _lockTable.XLock(blockId);
                _locks[blockId] = 'X';
            }
        }

        public void Release()
        {
            foreach(BlockId blockId in _locks.Keys)
            {
                _lockTable.UnLock(blockId);
            }
            _locks.Clear();
        }

        bool HasXLock(BlockId blockId)
            => _locks.TryGetValue(blockId, out var x) && x == 'X';
    }
}
