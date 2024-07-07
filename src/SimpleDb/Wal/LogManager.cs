using SimpleDb.Files;
using System.Collections;

namespace SimpleDb.Wal
{
    public class LogManager : IEnumerable<Memory<byte>>
    {
        private readonly Page _logPage;
        private BlockId _currentBlock;
        private readonly FileManager _fileManager;
        private readonly string _logFile;
        private int _latestLsn, _latestSavedLsn;

        public LogManager(FileManager fileManager, string logFile)
        {
            _fileManager = fileManager;
            _logFile = logFile;
            _logPage = new Page(fileManager.BlockSize);
            _latestLsn = _latestSavedLsn = 0;
            int length = fileManager.LengthInBlocks(logFile);
            if(length == 0)
            {
                _currentBlock = AppendNewBlock();
            }
            else
            {
                //set current block to the last block
                int blockIndex = length - 1;
                _currentBlock = new BlockId(logFile, blockIndex);
                fileManager.Read(_currentBlock, _logPage);
            }
        }

        //Appends a log record to the log buffer.
        //Log records are written right to left. The 
        //0th integer (boundary) contains the index of the start
        //of the last log record written
        //Log records are stored in reverse order to support reverse
        //iteration
        public int Append(Span<byte> logRecord)
        {
            //boundary is the index to where we start saving
            //the log record
            int boundary = _logPage.GetInt32(0);
            int recordSize = logRecord.Length;
            int bytesNeeded = recordSize + 4; //we will append the length
            if(boundary - bytesNeeded < 4) //the 1st 4 bytes should always be there
            {
                //the record doesn't fit
                Flush();
                _currentBlock = AppendNewBlock();
                boundary = _logPage.GetInt32(0);
            }
            int recordPosition = boundary - bytesNeeded;
            //this will write length as 4 byte integer + actual bytes
            _logPage.WriteBytes(recordPosition, logRecord);
            _logPage.SetInt32(0, recordPosition);
            _latestLsn += 1;
            return _latestLsn;
        }


        private BlockId AppendNewBlock()
        {
            BlockId blockId = _fileManager.Append(_logFile);
            _logPage.SetInt32(0, _fileManager.BlockSize);
            _fileManager.Write(blockId, _logPage);
            return blockId;
        }

        public void Flush(int lsn)
        {
            if(lsn >= _latestSavedLsn)
            {
                Flush();
            }
        }

        private void Flush()
        {
            _fileManager.Write(_currentBlock, _logPage);
            _latestSavedLsn = _latestLsn;
        }

        public IEnumerator<Memory<byte>> GetEnumerator()
        {
            Flush();
            return new LogIterator(_fileManager, _currentBlock);
        }

        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();

        private class LogIterator
            : IEnumerator<Memory<byte>>
        {
            private readonly FileManager _fileManager;
            private readonly Page _page;

            private BlockId _currentBlock;
            private readonly byte[] _currentBytes;
            private int _boundary, _currentPos, _bytesRead;

            public LogIterator(FileManager fileManager, BlockId current)
            {
                _fileManager = fileManager;
                _currentBlock = current;
                _page = new(fileManager.BlockSize);
                _currentBytes = new byte[fileManager.BlockSize];
                MoveToBlock();
            }
            public Memory<byte> Current => _currentBytes.AsMemory(0, _bytesRead);

            object IEnumerator.Current => Current;

            public bool MoveNext()
            {
                if(_currentPos < _fileManager.BlockSize || _currentBlock.BlockNumber > 0)
                {
                    if(_currentPos == _fileManager.BlockSize)
                    {
                        //move to previous block because we are at the end of that page
                        _currentBlock = new BlockId(_currentBlock.FileName, _currentBlock.BlockNumber - 1);
                        MoveToBlock();
                    }
                    _bytesRead = _page.GetBytes(_currentPos, _currentBytes);
                    _currentPos += _bytesRead + 4; //add the size of the length;
                    return true;
                }
                return false;
            }

            private void MoveToBlock()
            {
                _fileManager.Read(_currentBlock, _page);
                _boundary = _page.GetInt32(0);
                _currentPos = _boundary;
            }

            public void Reset()
            {
                throw new NotImplementedException();
            }

            public void Dispose()
            {
                //nothing to do here
            }
        }
    }
}
