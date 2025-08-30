using SimpleDb.Files;
using SimpleDb.Query;
using SimpleDb.Tx;
using System.Runtime.CompilerServices;

namespace SimpleDb.Record
{
    public class TableScan : IUpdateScan
    {
        private bool _disposed = false;

        private readonly Transaction tx;
        private readonly string tblName;
        private readonly Layout layout;

        private readonly string _fileName;
        private RecordPage _rp;
        private int _currentSlot = -1;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
        public TableScan(Transaction tx, string tblName, Layout layout)
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
        {
            if (string.IsNullOrWhiteSpace(tblName))
            {
                throw new ArgumentException($"'{nameof(tblName)}' cannot be null or whitespace.", nameof(tblName));
            }

            this.tx = tx ?? throw new ArgumentNullException(nameof(tx));
            this.tblName = tblName;
            this.layout = layout ?? throw new ArgumentNullException(nameof(layout));
            _fileName = $"{tblName}.tbl";
            if (tx.FileSizeInBlocks(_fileName) == 0)
            {
                MoveToNewBlock();
            }
            else
            {
                MoveToBlock(0);
            }
        }

        public RID RID => new(_rp.BlockId.BlockNumber, _currentSlot);

        public void BeforeFirst()
        {
            MoveToBlock(0);
        }

        public void Delete()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _rp.Delete(_currentSlot);
        }

        public void Dispose()
        {
            if(!_disposed)
            {
                UnpinRecordPage();
                GC.SuppressFinalize(this);
                _disposed = true;
            }
        }

        public int GetInt32(string fieldName)
        {
            CheckFieldType(fieldName, SchemaFieldType.I32);
            return _rp.GetInt(_currentSlot, fieldName);
        }

        public string GetString(string fieldName)
        {
            CheckFieldType(fieldName, SchemaFieldType.String);
            return _rp.GetString(_currentSlot, fieldName);
        }

        public Constant GetValue(string fieldName)
        {
            Schema.FieldInfo info = layout.Schema[fieldName];
            return info.FieldType switch
            {
                SchemaFieldType.I32 => new Constant(GetInt32(fieldName)),
                SchemaFieldType.String => new Constant(GetString(fieldName)),
                _ => throw new NotImplementedException($"Unknow schema field type: '{info.FieldType}'")
            };
        }

        public void Insert()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _currentSlot = _rp.InsertAfter(_currentSlot);
            while(_currentSlot < 0)
            {
                if(AtLastBlock()) 
                { 
                    MoveToNewBlock(); 
                }
                else
                {
                    MoveToBlock(_rp.BlockId.BlockNumber + 1);
                }
                _currentSlot = _rp.InsertAfter(_currentSlot);
            }
        }

        public void MoveToRID(RID rid)
        {
            UnpinRecordPage();
            var blk = new BlockId(_fileName, rid.BlockId);
            _rp = new RecordPage(tx, blk, layout);
            _currentSlot = rid.Slot;
        }

        public bool Next()
        {
            _currentSlot = _rp.NextAfter(_currentSlot);
            //if we got -1 move to next block
            while(_currentSlot < 0)
            {
                if (AtLastBlock())
                    return false;
                MoveToBlock(_rp.BlockId.BlockNumber + 1);
                _currentSlot = _rp.NextAfter(_currentSlot);
            }
            return true;
        }

        public void SetValue(string fieldName, int value)
        {
            CheckFieldType(fieldName, SchemaFieldType.I32);
           _rp.SetInt(_currentSlot, fieldName, value);
        }

        public void SetValue(string fieldName, string value)
        {
            CheckFieldType(fieldName, SchemaFieldType.String);
            _rp.SetString(_currentSlot, fieldName, value);
        }

        public void SetValue(string fieldName, Constant value)
        {
            CheckFieldType(fieldName, value.FieldType);
            switch (value.FieldType)
            {
                case SchemaFieldType.I32:
                    SetValue(fieldName, value.IntValue.GetValueOrDefault());
                    break;
                case SchemaFieldType.String:
                    SetValue(fieldName, value.StringValue ?? string.Empty);
                    break;
                default:
                    throw new InvalidOperationException($"Unexpected constant field type '{value.FieldType}'");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckFieldType(string fieldName, SchemaFieldType fieldType)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            Schema.FieldInfo info = layout.Schema[fieldName];
            if (info.FieldType != fieldType)
                throw new InvalidOperationException($"The field type '{fieldType}' does not match the schema field type '{info.FieldType}' for field '{fieldName}'");
        }

        public bool TryGetInt32(string fieldName, out int value)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if(layout.Schema.TryGetField(fieldName, out Schema.FieldInfo info) && info.FieldType == SchemaFieldType.I32)
            {
                value = GetInt32(fieldName);
                return true;
            }
            value = 0;
            return false;
        }

        public bool TryGetString(string fieldName, out string? value)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (layout.Schema.TryGetField(fieldName, out Schema.FieldInfo info) && info.FieldType == SchemaFieldType.String)
            {
                value = GetString(fieldName);
                return true;
            }
            value = null;
            return false;
        }

        private void MoveToBlock(int blkNum)
        {
            UnpinRecordPage();
            var blk = new BlockId(_fileName, blkNum);
            _rp = new RecordPage(tx,blk, layout);
            _currentSlot = -1;
        }

        private void MoveToNewBlock()
        {
            UnpinRecordPage();
            var blk = tx.Append(_fileName);
            _rp = new RecordPage(tx, blk, layout);
            _rp.Format();
            _currentSlot = -1;
        }

        private void UnpinRecordPage()
        {
            if (_rp != null) { tx.Unpin(_rp.BlockId); }
        }

        private bool AtLastBlock()
        {
            return _rp.BlockId.BlockNumber == tx.FileSizeInBlocks(_fileName);
        }
    }
}
