using SimpleDb.Files;
using SimpleDb.Tx;
using System.Runtime.CompilerServices;

namespace SimpleDb.Record
{
    public class RecordPage
    {
        public static readonly int Empty = 0, Used = 1;

        private readonly Transaction _transaction;
        private readonly BlockId _blockId;
        private readonly Layout _layout;

        public BlockId BlockId => _blockId;

        public RecordPage(Transaction transaction, BlockId blockId, Layout layout)
        {
            _transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
            _blockId = blockId;
            _layout = layout ?? throw new ArgumentNullException(nameof(layout));
            _transaction.Pin(_blockId);
        }

        public int GetInt(int slot, string fieldName)
            => _transaction.GetInt(BlockId, FieldPos(slot, fieldName));

        public string GetString(int slot, string fieldName)
            => _transaction.GetString(BlockId, FieldPos(slot, fieldName));

        public void SetInt(int slot, string fieldName, int value) 
            => _transaction.SetInt(BlockId, FieldPos(slot, fieldName), value, true);

        public void SetString(int slot, string fieldName, string value) 
            => _transaction.SetString(BlockId, FieldPos(slot, fieldName), value, true);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int FieldPos(int slot, string fieldName)
            => Offset(slot)+ _layout.GetOffset(fieldName);

        public void Delete(int slot)
            => SetFlag(slot, Empty);

        /// <summary>
        /// Uses the layout to format a new block of records.
        /// These values should not be logged because the old values are meaningless.
        /// </summary>
        public void Format()
        {
            int slot = 0;
            while (IsValidSlot(slot))
            {
                _transaction.SetInt(BlockId, Offset(slot), Empty, false);
                foreach(var (fieldName, info) in _layout.Schema)
                {
                    int fieldPos = FieldPos(slot, fieldName);
                    if (info.FieldType == SchemaFieldType.I32)
                        _transaction.SetInt(BlockId, fieldPos, 0, false);
                    else
                        _transaction.SetString(BlockId, fieldPos, "", false);
                }
                slot++;
            }
        }

        public int NextAfter(int slot)
            => SearchAfter(slot, Used);

        public int InsertAfter(int slot)
        {
            int newSlot = SearchAfter(slot, Empty);
            if(newSlot >= 0)
            {
                SetFlag(newSlot, Used);
            }
            return newSlot;
        }

        private void SetFlag(int slot, int flag) 
            => _transaction.SetInt(BlockId, Offset(slot), flag, true);

        private int SearchAfter(int slot, int flag)
        {
            slot++;
            while (IsValidSlot(slot))
            {
                int inUse = _transaction.GetInt(BlockId, Offset(slot));
                if (inUse == flag)
                    return slot;
                slot++;
            }
            return -1;
        }

        //clear trick, if offset of the next slot is after block size
        //then this slot will overflow.
        private bool IsValidSlot(int slot)
            => Offset(slot + 1) <= _transaction.BlockSize;

        private int Offset(int slot)
            => slot * _layout.SlotSize;
    }
}
