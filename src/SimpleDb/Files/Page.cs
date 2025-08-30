using System.Buffers.Binary;
using System.Text;

namespace SimpleDb.Files
{
    public class Page(Memory<byte> bytes)
    {
        public Memory<byte> Bytes => bytes;
        public Page(int size)
            : this(new Memory<byte>(new byte[size]))
        {

        }
        public int GetInt32(int offset)
        {
            Span<byte> span = bytes.Span.Slice(offset, 4);
            return BinaryPrimitives.ReadInt32BigEndian(span);
        }

        public void SetInt32(int offset, int value)
        {
            Span<byte> span = stackalloc byte[4];
            BinaryPrimitives.WriteInt32BigEndian(span, value);
            bytes.Span[offset] = span[0];
            bytes.Span[offset + 1] = span[1];
            bytes.Span[offset + 2] = span[2];
            bytes.Span[offset + 3] = span[3];
        }

        public int GetBytes(int offset, Span<byte> span)
        {
            int length = GetInt32(offset);
            if(length > span.Length)
            {
                throw new ArgumentException($"The length of bytes at offset {offset} is {length} but got span of length {span.Length}");
            }
            bytes.Span.Slice(offset + 4, length).CopyTo(span);
            return length;
        }

        public void WriteBytes(int offset, Span<byte> span)
        {
            SetInt32(offset, span.Length);
            int newOffset = offset + 4;
            for(int i = 0; i < span.Length; i++)
            {
                bytes.Span[newOffset + i] = span[i];
            }
        }

        public void SetString(int offset, string value)
        {
            var charBytes = Encoding.UTF8.GetBytes(value);
            SetInt32(offset, charBytes.Length);
            WriteBytes(offset + 4, charBytes);
        }

        public string GetString(int offset)
        {
            int length = GetInt32(offset);
            byte[] b = new byte[length];
            GetBytes(offset + 4, b);
            return Encoding.UTF8.GetString(b);
        }

        public static int MaxLength(int stringLength)
            => Encoding.UTF8.GetMaxByteCount(stringLength) + 4; //4 for length
    }
}
