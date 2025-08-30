using Microsoft.Extensions.Logging;
using SimpleDb.BufferPool;
using SimpleDb.Files;
using SimpleDb.Tx.Concurrency;
using SimpleDb.Tx.Recovery;
using SimpleDb.Wal;

namespace SimpleDb.Tx
{
    public class Transaction
    {
        private static int _nextTxId;
        private static readonly int EOF = -1;

        private readonly int _txNum;
        private readonly FileManager _fileManager;
        private readonly BufferMgr _bufferMgr;
        private readonly BufferList _myBuffers;
        private readonly RecoveryMgr _recoveryMgr;
        private readonly ConcurrencyMgr _concurrencyMgr;
        private readonly ILogger<Transaction> _logger;

        public int BlockSize => _fileManager.BlockSize;
        public int AvailableBuffers => _bufferMgr.FreeBufferCount;

        public Transaction(FileManager fileManager, LogManager logManager, BufferMgr bufferMgr, ILogger<Transaction> logger)
        {
            _txNum = Interlocked.Increment(ref _nextTxId);
            _fileManager = fileManager;
            _bufferMgr = bufferMgr;
            _myBuffers = new(bufferMgr);
            _recoveryMgr = new(this, _txNum, logManager, bufferMgr);
            _concurrencyMgr = new();
            _logger = logger;
        }
        public int TxNumber => _txNum;

        public void Commit()
        {
            _recoveryMgr.Commit();
            if(_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Committing transaction {TransactionId}", _txNum);
            }
            _concurrencyMgr.Release();
            _myBuffers.UnpinAll();
        }

        public void Rollback()
        {
            _recoveryMgr.Rollback();
            if (_logger.IsEnabled(LogLevel.Warning))
            {
                _logger.LogWarning("Rolling back transaction {TransactionId}", _txNum);
            }
            _concurrencyMgr.Release();
            _myBuffers.UnpinAll();
        }

        public void Recover()
        {
            _bufferMgr.FlushAll(_txNum);
            _recoveryMgr.Recover();
        }

        public void Pin(BlockId blockId)
            => _myBuffers.Pin(blockId);
        public void Unpin(BlockId blockId)
            => _myBuffers.Unpin(blockId);

        public int GetInt(BlockId blockId, int offset)
        {
            _concurrencyMgr.SLock(blockId);
            var buffer = _myBuffers.GetBuffer(blockId, out _);
            return buffer.Page.GetInt32(offset);
        }

        public void SetInt(BlockId blockId, int offset, int value, bool shouldLog)
        {
            _concurrencyMgr.XLock(blockId);
            var buffer = _myBuffers.GetBuffer(blockId, out _);
            int lsn = -1;
            if (shouldLog)
            {
                lsn = _recoveryMgr.LogSetInt32(buffer, offset, value);
            }
            buffer.Page.SetInt32(offset, value);
            buffer.SetModified(_txNum, lsn);
        }

        public string GetString(BlockId blockId, int offset)
        {
            _concurrencyMgr.SLock(blockId);
            var buffer = _myBuffers.GetBuffer(blockId, out _);
            return buffer.Page.GetString(offset);
        }

        public void SetString(BlockId blockId, int offset, string value, bool shouldLog)
        {
            _concurrencyMgr.XLock(blockId);
            var buffer = _myBuffers.GetBuffer(blockId, out _);
            int lsn = -1;
            if (shouldLog)
            {
                lsn = _recoveryMgr.LogSetString(buffer, offset, value);
            }
            buffer.Page.SetString(offset, value);
            buffer.SetModified(_txNum, lsn);
        }

        public int FileSizeInBlocks(string fileName)
        {
            var block = new BlockId(fileName, EOF);
            _concurrencyMgr.SLock(block);
            return _fileManager.LengthInBlocks(fileName);
        }

        public BlockId Append(string fileName)
        {
            _concurrencyMgr.XLock(new BlockId(fileName, EOF));
            return _fileManager.Append(fileName);
        }
    }
}
