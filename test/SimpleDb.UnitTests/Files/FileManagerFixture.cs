using SimpleDb.Files;

namespace SimpleDb.UnitTests.Files
{
    public class FileManagerFixture : IDisposable
    {
        public FileManager FileManager { get; }

        public int BlockSize = 4096;

        public FileManagerFixture()
        {
            DirectoryInfo df = new(Path.Join(AppContext.BaseDirectory, "db"));
            if(df.Exists)
            {
                df.Delete(true);
            }
            FileManager = new (df, BlockSize);
        }

        public void Dispose()
        {
            FileManager.Dispose();
        }
    }
}
