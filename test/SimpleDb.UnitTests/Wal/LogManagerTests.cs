using SimpleDb.Files;
using SimpleDb.Wal;
using System.Text;

namespace SimpleDb.UnitTests.Wal
{
    public class LogManagerTests : IClassFixture<LogManagerFixture>
    {
        private readonly LogManager _logManager;
        private readonly FileManager _fileManager;

        public LogManagerTests(LogManagerFixture logManager)
        {
            _logManager = logManager.LogManager;
            _fileManager = logManager.FileManager;
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
            int byteLength = (_fileManager.BlockSize - 4) / 3; //put max of 2 items in each block

            byte[] bytes = new byte[byteLength];
            int count = 17; //should give 6 blocks
            int lsn = -1;
            for(int i = 0; i < count; i++)
            {
                Random.Shared.NextBytes(bytes);
                int cLsn = _logManager.Append(bytes);
                Assert.True(cLsn > lsn);
                lsn = cLsn;
            }

            //now read the records in reverse
            int iterCount = 0;
            foreach(var rec in _logManager)
            {
                Assert.Equal(bytes.Length, rec.Length);
                iterCount += 1;
            }

            Assert.Equal(count-1, iterCount);

        }

        static byte[] GetTextBytes(string text)
            => Encoding.UTF8.GetBytes(text);
    }
}
