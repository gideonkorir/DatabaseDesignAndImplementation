using SimpleDb.BufferPool;
using SimpleDb.Files;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BPBuffer = SimpleDb.BufferPool.Buffer;

namespace SimpleDb.Tx
{
    /// <summary>
    /// Manages the transaction's currently pinned buffers.
    /// </summary>
    public class BufferList(BufferMgr bufferMgr)
    {
        private readonly Dictionary<BlockId, TrackedBufferInfo> _buffers = [];

        public BPBuffer GetBuffer(BlockId blockId, out int pins)
        {
            ref TrackedBufferInfo bufferInfo = ref CollectionsMarshal.GetValueRefOrNullRef(_buffers, blockId);
            if (Unsafe.IsNullRef(ref bufferInfo))
                throw new InvalidOperationException($"Buffer for block {blockId} was not found");
            pins = bufferInfo.Pins;
            return bufferInfo.Buffer;
        }

        public void Pin(BlockId blockId)
        {
            var buffer = bufferMgr.Pin(blockId);
            ref TrackedBufferInfo bufferInfo = ref CollectionsMarshal.GetValueRefOrAddDefault(_buffers, blockId, out var value);
            bufferInfo.Buffer = buffer;
            bufferInfo.Pins += 1;
        }

        public void Unpin(BlockId blockId)
        {
            ref TrackedBufferInfo bufferInfo = ref CollectionsMarshal.GetValueRefOrNullRef(_buffers, blockId);
            if (Unsafe.IsNullRef(ref bufferInfo))
                throw new InvalidOperationException($"Buffer for block {blockId} is not pinned by the transaction");
            var buffer = bufferInfo.Buffer;
            bufferMgr.Unpin(buffer);
            bufferInfo.Pins -= 1;
            if (bufferInfo.Pins == 0)
            {
                _buffers.Remove(blockId);
            }
        }

        public void UnpinAll()
        {
            foreach (var bf in _buffers.Values)
            {
                for(var i=0; i<bf.Pins; i++)
                    bufferMgr.Unpin(bf.Buffer); //unpin as many times as we are pinned
            }
            _buffers.Clear();
        }

        private struct TrackedBufferInfo
        {
            public required BPBuffer Buffer;
            public int Pins;
        }
    }
}
