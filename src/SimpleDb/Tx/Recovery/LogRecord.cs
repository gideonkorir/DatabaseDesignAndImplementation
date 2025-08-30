using Microsoft.Extensions.Logging;
using SimpleDb.Files;
using System.Buffers.Binary;
using System.Dynamic;
using System.Text;

namespace SimpleDb.Tx.Recovery
{
    public static class LogRecord
    {
        /// <summary>
        /// Number of bytes at the beginning of the returned array
        /// that has the transaction info
        /// </summary>
        public static readonly int TransactionInfoBytes = 5;

        public static byte[] StartRecord(int txNumber) 
            => GetBytes(LogRecordType.Start, txNumber);

        public static byte[] Checkpoint(int txNumber)
            => GetBytes(LogRecordType.Checkpoint, txNumber);

        public static byte[] Commit(int txNumber)
            => GetBytes(LogRecordType.Commit, txNumber);

        public static byte[] Rollback(int txNumber)
            => GetBytes(LogRecordType.Rollback, txNumber);

        private static byte[] GetBytes(LogRecordType recordType, int txNumber)
        {
            var b = new byte[5];
            AddHeader(b, recordType, txNumber);
            return b;
        }

        public static byte[] SetInt(int txNumber, BlockId block, int offset, int oldValue, int newValue)
        {
            //we will write
            int fNameByteCount = Encoding.UTF8.GetByteCount(block.FileName);
            byte[] data = new byte[25 + fNameByteCount];
            AddHeader(data, LogRecordType.SetInt, txNumber);
            //1st 4 bytes == file name length
            //2nd {byteCount} bytes == file name
            //3rd 4 bytes == block id
            //4th 4 bytes == offset in little endian
            //5th 4 bytes == value in little endian
            BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(5), fNameByteCount);
            Encoding.UTF8.GetBytes(block.FileName, data.AsSpan(9, fNameByteCount));
            BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(9 + fNameByteCount), block.BlockNumber);
            BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(13 + fNameByteCount), offset);
            BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(17 + fNameByteCount), oldValue);
            BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(21 + fNameByteCount), newValue);

            return data;
        }

        public static byte[] SetString(int txNumber, BlockId block, int offset, string oldValue, string newValue)
        {
            //we will write
            int fNameByteCount = Encoding.UTF8.GetByteCount(block.FileName);
            int oldValueByteCount = Encoding.UTF8.GetByteCount(oldValue);
            int newValueByteCount = Encoding.UTF8.GetByteCount(newValue);
            //5 bytes for header
            //4 bytes offset
            //2 * 4 byte lengths
            //value bytes lengths
            //4 byte file name length + 4 byte block id
            //file name
            byte[] data = new byte[25 + fNameByteCount + oldValueByteCount + newValueByteCount];
            AddHeader(data, LogRecordType.SetString, txNumber);
            //1st 4 bytes == file name length
            //2nd {byteCount} bytes == file name
            //3rd 4 bytes == block id
            //4th 4 bytes == offset in little endian
            //5th 4 bytes == value in little endian
            BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(5), fNameByteCount);
            Encoding.UTF8.GetBytes(block.FileName, data.AsSpan(9, fNameByteCount));
            BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(9 + fNameByteCount), block.BlockNumber);
            BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(13 + fNameByteCount), offset);
            BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(17 + fNameByteCount), oldValueByteCount);
            Encoding.UTF8.GetBytes(oldValue, data.AsSpan(21 + fNameByteCount));
            BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(21 + fNameByteCount + oldValueByteCount), newValueByteCount);
            Encoding.UTF8.GetBytes(newValue, data.AsSpan(25 + fNameByteCount + oldValueByteCount));
            return data;
        }

        private static void AddHeader(Span<byte> data, LogRecordType recordType, int txNumber)
        {
            data[0] = (byte)recordType;
            BinaryPrimitives.WriteInt32LittleEndian(data[1..], txNumber);
        }



        public static LogRecordHeader GetLogRecordHeader(Span<byte> logRecord)
        {
            if (logRecord.Length < TransactionInfoBytes)
                throw new ArgumentException($"The log record must be at least {TransactionInfoBytes} bytes in length");

            LogRecordType recordType = (LogRecordType)logRecord[0];
            int txNumber = BinaryPrimitives.ReadInt32LittleEndian(logRecord[1..]);

            return new LogRecordHeader(recordType, txNumber);
        }


        public static LogRecordData<int> GetIntLogRecordData(Span<byte> data)
        {
            //get length of file name
            int fNameByteLength = BinaryPrimitives.ReadInt32LittleEndian(data);
            //get the bytes for the name
            string name = Encoding.UTF8.GetString(data.Slice(4, fNameByteLength));
            //get the block #
            int blockNumber = BinaryPrimitives.ReadInt32LittleEndian(data[(4 + fNameByteLength)..]);
            int offset = BinaryPrimitives.ReadInt32LittleEndian(data[(8 + fNameByteLength)..]);
            int oldValue = BinaryPrimitives.ReadInt32LittleEndian(data[(12 + fNameByteLength)..]);
            int newValue = BinaryPrimitives.ReadInt32LittleEndian(data[(16 + fNameByteLength)..]);
            return new LogRecordData<int>(
                new BlockId(name, blockNumber),
                offset,
                oldValue,
                newValue
                );
        }

        public static LogRecordData<string> GetStringLogRecordData(Span<byte> data)
        {
            //get length of file name
            int fNameByteLength = BinaryPrimitives.ReadInt32LittleEndian(data);
            //get the bytes for the name
            string name = Encoding.UTF8.GetString(data.Slice(4, fNameByteLength));
            //get the block #
            int blockNumber = BinaryPrimitives.ReadInt32LittleEndian(data[(4 + fNameByteLength)..]);
            int offset = BinaryPrimitives.ReadInt32LittleEndian(data[(8 + fNameByteLength)..]);
            int oldValueLength = BinaryPrimitives.ReadInt32LittleEndian(data[(12 + fNameByteLength)..]);
            string oldValue = Encoding.UTF8.GetString(data.Slice(16 + fNameByteLength, oldValueLength));
            _ = BinaryPrimitives.ReadInt32LittleEndian(data[(16 + fNameByteLength + oldValueLength)..]);
            string newValue = Encoding.UTF8.GetString(data[(20 + fNameByteLength + oldValueLength)..]);
            return new LogRecordData<string>(
                new BlockId(name, blockNumber),
                offset,
                oldValue,
                newValue
                );
        }

        public static void DoRollback(Transaction tx, Span<byte> logRecord)
        {
            var header = LogRecord.GetLogRecordHeader(logRecord);
            switch(header.RecordType)
            {
                case LogRecordType.Start:
                case LogRecordType.Checkpoint:
                case LogRecordType.Commit:
                case LogRecordType.Rollback:
                default:
                    break;
                case LogRecordType.SetInt:
                    var intData = GetIntLogRecordData(logRecord[TransactionInfoBytes..]);
                    tx.Pin(intData.BlockId);
                    tx.SetInt(intData.BlockId, intData.Offset, intData.PrevValue, false);
                    break;
                case LogRecordType.SetString:
                    var strData = GetStringLogRecordData(logRecord[TransactionInfoBytes..]);
                    tx.Pin(strData.BlockId);
                    tx.SetString(strData.BlockId, strData.Offset, strData.PrevValue, false);
                    break;
            }
        }

        public static void Dump(ILogger logger, string action, byte[] record, LogLevel level = LogLevel.Debug)
        {
            if(logger.IsEnabled(level))
            {
                var header = GetLogRecordHeader(record);

                logger.Log(level, "[{action}]. {Type} log record for transaction {TransactionId} with data {data}",
                    action,
                    header.RecordType.ToString(),
                    header.TxNumber,
                    header.RecordType switch
                    {
                        LogRecordType.SetInt => GetIntLogRecordData(record.AsSpan(TransactionInfoBytes)),
                        LogRecordType.SetString => GetStringLogRecordData(record.AsSpan(TransactionInfoBytes)),
                        _ => string.Empty
                    }
                    );
            }
        }
    }

}
