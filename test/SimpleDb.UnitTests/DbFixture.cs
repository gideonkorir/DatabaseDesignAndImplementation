using SimpleDb.Files;
using SimpleDb.Wal;

namespace SimpleDb.UnitTests
{
    public class DbFixture
    {
        public LogManager LogManager { get; }

        public FileManager FileManager { get; }

        public Guid Id { get; } = Guid.NewGuid();

        public DbFixture()
        {
            var path = Path.Join(AppContext.BaseDirectory, "db", Id.ToString());
            var dir = new DirectoryInfo(path);
            if (dir.Exists)
            {
                dir.Delete(true);
            }

            FileManager = new FileManager(dir, 4096);
            LogManager = new LogManager(FileManager, "test.log");
        }
    }
}
