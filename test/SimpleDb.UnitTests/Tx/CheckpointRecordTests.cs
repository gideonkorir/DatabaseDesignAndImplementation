using SimpleDb.Files;
using SimpleDb.Tx.Recovery;

namespace SimpleDb.UnitTests.Tx
{
    public class CheckpointRecordTests
    {
        [Fact]
        public void StartRecord_Created_OK()
        {
            int tx = Random.Shared.Next();
            byte[] log = LogRecord.StartRecord(tx);
            Assert.Equal(LogRecord.TransactionInfoBytes, log.Length);

            var header = LogRecord.GetLogRecordHeader(log);
            Assert.Equal(LogRecordType.Start, header.RecordType);
            Assert.Equal(tx, header.TxNumber);
        }

        [Fact]
        public void CommitRecord_Created_OK()
        {
            int tx = Random.Shared.Next();
            byte[] log = LogRecord.Commit(tx);
            Assert.Equal(LogRecord.TransactionInfoBytes, log.Length);

            var header = LogRecord.GetLogRecordHeader(log);
            Assert.Equal(LogRecordType.Commit, header.RecordType);
            Assert.Equal(tx, header.TxNumber);
        }

        [Fact]
        public void RollbackRecord_Created_OK()
        {
            int tx = Random.Shared.Next();
            byte[] log = LogRecord.Rollback(tx);
            Assert.Equal(LogRecord.TransactionInfoBytes, log.Length);

            var header = LogRecord.GetLogRecordHeader(log);
            Assert.Equal(LogRecordType.Rollback, header.RecordType);
            Assert.Equal(tx, header.TxNumber);
        }


        [Fact]
        public void CheckpointRecord_Created_OK()
        {
            int tx = Random.Shared.Next();
            byte[] log = LogRecord.Checkpoint(tx);
            Assert.Equal(LogRecord.TransactionInfoBytes, log.Length);

            var header = LogRecord.GetLogRecordHeader(log);
            Assert.Equal(LogRecordType.Checkpoint, header.RecordType);
            Assert.Equal(tx, header.TxNumber);
        }


        [Fact]
        public void IntLogRecord_Created_OK()
        {
            var block = new BlockId($"/somefile/{Guid.NewGuid()}/myfile.txt", Random.Shared.Next());
            int txNum = Random.Shared.Next();
            byte[] log = LogRecord.SetInt(txNum, block, 4, 32, 89);

            var header = LogRecord.GetLogRecordHeader(log);
            Assert.Equal(LogRecordType.SetInt, header.RecordType);
            Assert.Equal(txNum, header.TxNumber);

            var data = LogRecord.GetIntLogRecordData(log.AsSpan(LogRecord.TransactionInfoBytes));
            Assert.Equal(block.FileName, data.BlockId.FileName);
            Assert.Equal(block.BlockNumber, data.BlockId.BlockNumber);
            Assert.Equal(4, data.Offset);
            Assert.Equal(32, data.PrevValue);
            Assert.Equal(89, data.NewValue);
        }


        [Fact]
        public void StringLogRecord_Created_Ok()
        {
            var block = new BlockId($"/somefile/{Guid.NewGuid()}/myfile.txt", Random.Shared.Next());
            int txNum = Random.Shared.Next();
            String s = "abceakfdjidfjadsjfkajfajifk2jiwjjwe";
            String s2 = Guid.NewGuid().ToString();
            byte[] log = LogRecord.SetString(txNum, block, 4, s, s2);

            var header = LogRecord.GetLogRecordHeader(log);
            Assert.Equal(LogRecordType.SetString, header.RecordType);
            Assert.Equal(txNum, header.TxNumber);

            var data = LogRecord.GetStringLogRecordData(log.AsSpan(LogRecord.TransactionInfoBytes));
            Assert.Equal(block.FileName, data.BlockId.FileName);
            Assert.Equal(block.BlockNumber, data.BlockId.BlockNumber);
            Assert.Equal(4, data.Offset);
            Assert.Equal(s, data.PrevValue);
            Assert.Equal(s2, data.NewValue);
        }
    }
}
