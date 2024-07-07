using SimpleDb.Files;
using SimpleDb.Wal;

namespace SimpleDb.BufferPool
{
    public class Buffer(FileManager fileManager, LogManager logManager)
    {
        private int _pins = 0, _txNum = -1, _lsn = -1;

        private BlockId? _blockId;

        public Page Page { get; } = new Page(fileManager.BlockSize);

        public BlockId? BlockId => _blockId;

        public bool Pinned => _pins > 0;

        public int ModifyingTransaction => _txNum;

        public int Lsn => _lsn;
        public DateTime? LastModified { get; private set; } = null;

        public void SetModified(int txNumber, int lsn)
        {
            _txNum = txNumber;
            LastModified = DateTime.UtcNow;
            if(lsn >= 0)
            {
                _lsn = lsn;
            }
        }

        public void Assign(BlockId blockId)
        {
            Flush();
            _blockId = blockId;
            fileManager.Read(_blockId.Value, Page);
            _pins = 0;
        }

        public void Pin() => _pins++;

        public void Unpin() => _pins--;

        public void Flush()
        {
            if (_txNum >= 0)
            {
                var prevBlock = _blockId!.Value;
                logManager.Flush(_lsn);
                fileManager.Write(prevBlock, Page);
                _txNum = -1;
            }
        }
    }
}
