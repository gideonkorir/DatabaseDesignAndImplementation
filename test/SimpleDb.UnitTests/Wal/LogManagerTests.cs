using SimpleDb.Files;
using SimpleDb.Wal;
using System.Text;
using Xunit.Abstractions;

namespace SimpleDb.UnitTests.Wal
{
    public class LogManagerTests
    {
        private readonly int _blockSize = 4096;
        private readonly LogManager _logManager;
        private readonly ITestOutputHelper _testOutputHelper;

        public LogManagerTests(ITestOutputHelper testOutputHelper)
        {
            //don't use xunit style fixture otherwise the tests will
            //overwrite each others data.
            var fx = new DbFixture();
            _logManager = fx.LogManager;
            _blockSize = fx.FileManager.BlockSize;
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public void LogManagerWillAppendLogs()
        {
            string[] _records =
            {
                "DELETE PAGE 1",
                "APPEND PAGE 2",
                "UPDATE PAGE 1"
            };
            foreach (string _record in _records)
            {
                _ = _logManager.Append(GetTextBytes(_record));
            }

            //iterate should appear in reverse
            int index = _records.Length - 1;
            foreach(Memory<byte> memory in _logManager)
            {
                string v = Encoding.UTF8.GetString(memory.Span);

                Assert.Equal(_records[index], v);
                index--;
            }
        }

        [Fact]
        public void LogManagerWillAppendMultipleBlocks()
        {
            //remove the 1st 4 bytes for the count
            //and split by 3. The 1st 4 bytes track the next
            //insert position in the page.
            int byteLength = (_blockSize - 4) / 3;

            //below should give 8 blocks because we are fitting 2 random {byteLength} bytes
            //in the blocks. 17/3 + 1. It's 2 cause of the length of the bytes is also appended
            //to the page
            int count = 17;
            int lsn = -1;
            Stack<byte[]> bytesWritten = new(17);
            for(int i = 0; i < count; i++)
            {
                byte[] bytes = new byte[byteLength];
                Random.Shared.NextBytes(bytes);
                int cLsn = _logManager.Append(bytes);
                Assert.True(cLsn > lsn);
                lsn = cLsn;
                bytesWritten.Push(bytes);
            }

            //now read the records in reverse
            int iterCount = 0;            
            foreach(var rec in _logManager)
            {
                Assert.Equal(byteLength, rec.Length);
                _testOutputHelper.WriteLine("Comparing bytes for log record index: " +  iterCount);
                iterCount += 1;
                byte[] bytes = bytesWritten.Pop();
                Assert.Equal(rec.ToArray(), bytes);
            }

            Assert.Empty(bytesWritten); //ensure all log records have been processed

        }

        static byte[] GetTextBytes(string text)
            => Encoding.UTF8.GetBytes(text);
    }
}
