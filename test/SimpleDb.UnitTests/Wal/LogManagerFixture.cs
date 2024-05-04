using SimpleDb.Files;
using SimpleDb.Wal;

namespace SimpleDb.UnitTests.Wal
{
    public class LogManagerFixture
    {
        public LogManager LogManager { get; }

        public FileManager FileManager { get; }

        public LogManagerFixture()
        {
            var path = Path.Join(AppContext.BaseDirectory, "logs");
            var dir = new DirectoryInfo(path);
            if(dir.Exists)
            {
                dir.Delete(true);
            }

            FileManager = new FileManager(dir, 4096);
            LogManager = new LogManager(FileManager, "test.log");
        }
    }
}
