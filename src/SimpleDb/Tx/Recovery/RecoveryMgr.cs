using SimpleDb.BufferPool;
using SimpleDb.Wal;
using System.Diagnostics;
using MyBuffer = SimpleDb.BufferPool.Buffer;

namespace SimpleDb.Tx.Recovery
{
    public class RecoveryMgr(Transaction tx, int txNum, LogManager logMgr, BufferMgr bufferMgr)
    {
        public void Commit()
        {
            //By flushing the buffers before writing the commit log record,
            //we only need to support undo operations during recovery. There is
            //no need for re-do because all transactions that have committed will
            //have had their buffers written to disk.
            bufferMgr.FlushAll(txNum);
            byte[] logRecord = LogRecord.Commit(txNum);
            int lsn = logMgr.Append(logRecord);
            logMgr.Flush(lsn);
        }

        public void Rollback()
        {
            foreach (Memory<byte> record in logMgr)
            {
                LogRecordHeader header = LogRecord.GetLogRecordHeader(record.Span);
                if (header.TxNumber == txNum)
                {
                    LogRecord.DoRollback(tx, record.Span);
                    //when we hit start we break
                    if (header.RecordType == LogRecordType.Start)
                    {
                        break;
                    }
                }
            }

            bufferMgr.FlushAll(txNum);
            byte[] logRecord = LogRecord.Rollback(txNum);
            int lsn = logMgr.Append(logRecord);
            logMgr.Flush(lsn);
        }

        public void Recover()
        {
            //Check the comment in Commit()
            //There is no need for redo transactions.
            //[I feel I don't like it because it flushes the buffers before each commit
            //leading to slower commits]
            HashSet<int> completedTx = [];
            foreach(Memory<byte> record in logMgr)
            {
                var header = LogRecord.GetLogRecordHeader(record.Span);
                if (header.RecordType == LogRecordType.Checkpoint)
                    break;
                if(header.RecordType == LogRecordType.Commit || header.RecordType == LogRecordType.Rollback)
                {
                    completedTx.Add(header.TxNumber);
                }
                else if(!completedTx.Contains(header.TxNumber))
                {
                    LogRecord.DoRollback(tx, record.Span);
                }                
            }

            bufferMgr.FlushAll(txNum);
            byte[] logRecord = LogRecord.Checkpoint(txNum);
            int lsn = logMgr.Append(logRecord);
            logMgr.Flush(lsn);
        }       

        public int LogSetInt32(MyBuffer buffer, int offset, int newValue)
        {
            var oldValue = buffer.Page.GetInt32(offset);
            byte[] logRecord = LogRecord.SetInt(txNum, buffer.BlockId!.Value, offset, oldValue, newValue);
            int lsn = logMgr.Append(logRecord);
            return lsn;
        }

        public int LogSetString(MyBuffer buffer, int offset, string newValue)
        {
            var oldValue = buffer.Page.GetString(offset);
            byte[] logRecord = LogRecord.SetString(txNum, buffer.BlockId!.Value, offset, oldValue, newValue);
            int lsn = logMgr.Append(logRecord);
            return lsn;
        }
    }
}
