using System.Collections.Concurrent;

namespace SimpleDb.Files
{
    public class FileManager : IDisposable
    {
        private readonly Func<string, FileStream> _fsFactory;

        public DirectoryInfo Directory { get; }
        public int BlockSize { get; }

        readonly ConcurrentDictionary<string, FileStream> _files = new(StringComparer.OrdinalIgnoreCase);

        public FileManager(DirectoryInfo directory, int blockSize)
        {
            ArgumentNullException.ThrowIfNull(directory);
            if (blockSize <= 0 || blockSize % 2 != 0)
                throw new ArgumentException($"The block size must be <= 0 and a multiple of 2");

            if (!directory.Exists)
            {
                directory.Create();
            }
            Directory = directory;
            BlockSize = blockSize;
            _fsFactory = CreateFs;

            foreach(var file in Directory.EnumerateFiles("temp*"))
            {
                file.Delete();
            }
        }

        public void Read(BlockId blockId, Page page)
        {
            FileStream fs = _files.GetOrAdd(blockId.FileName, _fsFactory);
            lock (fs)
            {
                fs.Seek(BlockSize * blockId.BlockNumber, SeekOrigin.Begin);
                fs.ReadExactly(page.Bytes.Span);
            }
        }

        public void Write(BlockId blockId, Page page)
        {
            FileStream fs = _files.GetOrAdd(blockId.FileName, _fsFactory);
            lock(fs)
            {
                fs.Seek(BlockSize * blockId.BlockNumber, SeekOrigin.Begin);
                fs.Write(page.Bytes.Span);
            }
        }

        public BlockId Append(string fileName)
        {
            FileStream fs = _files.GetOrAdd(fileName, _fsFactory);
            int nextBlockId = (int)(fs.Length / BlockSize);
            fs.SetLength(fs.Length + BlockSize);
            fs.Flush();
            return new BlockId(fileName, nextBlockId);
        }

        public int LengthInBlocks(string fileName)
        {
            FileStream fs = _files.GetOrAdd(fileName, _fsFactory);
            int size = (int)(fs.Length % BlockSize == 0 ? fs.Length / BlockSize : (fs.Length / BlockSize) + 1);
            return size;
        }

        private FileStream CreateFs(string name)
        {
            name = Path.Join(Directory.FullName, name);
            FileStream fs = new(name, new FileStreamOptions
            {
                Mode = FileMode.OpenOrCreate,
                Access = FileAccess.ReadWrite,
                Share = FileShare.None,
                Options = FileOptions.RandomAccess
            });
            return fs;
        }

        public void Dispose()
        {
            var array = _files.Values.ToArray();
            _files.Clear();
            foreach (var file in array)
            {
                file.Dispose();
            }
            GC.SuppressFinalize(this);
        }
    }
}
