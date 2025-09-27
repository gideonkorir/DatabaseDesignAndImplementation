using Microsoft.Extensions.Logging.Abstractions;
using SimpleDb.BufferPool;
using SimpleDb.Tx;
using Xunit.Abstractions;
using Meziantou.Extensions.Logging.Xunit;
using Microsoft.Extensions.Logging;
using SimpleDb.Files;
using SimpleDb.Tx.Recovery;

namespace SimpleDb.UnitTests.Tx
{
    public class TransactionTest : AbstractTxTest
    {
        public TransactionTest(ITestOutputHelper testOutputHelper) 
            :base(testOutputHelper)
        {
        }

        [Fact]
        public void CommitTransactionLogs()
        {
            var blockId = new BlockId("commit.db", 0);
            _tx.Append(blockId.FileName);
            _tx.Pin(blockId);
            _tx.SetInt(blockId, 0, 14, true);
            _tx.SetString(blockId, 4, "this is a string", true);
            _tx.Commit();

            using var iterator = _fixture.LogManager.GetEnumerator();
            Assert.True(iterator.MoveNext());
            LogRecordHeader header = LogRecord.GetLogRecordHeader(iterator.Current.Span);
            Assert.Equal(LogRecordType.Commit, header.RecordType);
            Assert.Equal(_tx.TxNumber, header.TxNumber);

            Assert.True(iterator.MoveNext());
            header = LogRecord.GetLogRecordHeader(iterator.Current.Span);
            Assert.Equal(LogRecordType.SetString, header.RecordType);
            Assert.Equal(_tx.TxNumber, header.TxNumber);
            var strBody = LogRecord.GetStringLogRecordData(iterator.Current.Span[LogRecord.TransactionInfoBytes..]);
            Assert.Equal(4, strBody.Offset);
            Assert.Equal(blockId, strBody.BlockId);
            Assert.Equal(string.Empty, strBody.PrevValue);
            Assert.Equal("this is a string", strBody.NewValue);

            Assert.True(iterator.MoveNext());
            header = LogRecord.GetLogRecordHeader(iterator.Current.Span);
            Assert.Equal(header.RecordType, header.RecordType);
            Assert.Equal(_tx.TxNumber, header.TxNumber);
            var intBody = LogRecord.GetIntLogRecordData(iterator.Current.Span[LogRecord.TransactionInfoBytes..]);
            Assert.Equal(0, intBody.Offset);
            Assert.Equal(blockId, intBody.BlockId);
            Assert.Equal(0, intBody.PrevValue);
            Assert.Equal(14, intBody.NewValue);
        }

    }
}
