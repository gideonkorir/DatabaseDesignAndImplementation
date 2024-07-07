using SimpleDb.Files;

namespace SimpleDb.UnitTests.Files
{
    public class FileManagerTests : IClassFixture<DbFixture>
    {
        private readonly FileManager _fileManager;

        public FileManagerTests(DbFixture fixture)
        {
            _fileManager = fixture.FileManager;
        }

        [Fact]
        public void FileManager_Creates_New_File()
        {
            BlockId blockId = new("people", 0);
            _fileManager.Write(blockId, new Page(_fileManager.BlockSize));
            Assert.True(File.Exists(Path.Join(_fileManager.Directory.FullName, "people")));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(5)]
        [InlineData(12)]
        public void FileManager_Writes_To_File(int offset)
        {
            BlockId blockId = new("pops", offset);
            byte[] b = new byte[_fileManager.BlockSize];
            Random.Shared.NextBytes(b);
            Page p = new(b);
            _fileManager.Write(blockId, p);
            Page p2 = new(_fileManager.BlockSize);
            _fileManager.Read(blockId, p2);
            Assert.Equal(p.Bytes.ToArray(), p2.Bytes.ToArray());
        }
    }
}
