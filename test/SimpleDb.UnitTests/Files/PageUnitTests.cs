using SimpleDb.Files;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDb.UnitTests.Files
{
    public class PageUnitTests
    {
        [Fact]
        public void IntTest()
        {
            Page p = new(512);
            int[] offsets = new int[] { 0, 8, 28, 127 };
            foreach(int offset in offsets)
            {
                int v = Random.Shared.Next();
                p.SetInt32(offset, v);
                int rv = p.GetInt32(offset);
                Assert.Equal(v, rv);
            }
        }

        [Fact]
        public void BytesTest()
        {
            Page p = new(512);
            int[] offsets = { 0, 128, 96, 300 };
            byte[] bytes = new byte[128];
            byte[] results = new byte[bytes.Length];
            foreach(int offset in offsets)
            {
                Random.Shared.NextBytes(bytes);
                p.WriteBytes(offset, bytes);
                p.GetBytes(offset, results);
                Assert.Equal(bytes, results);
            }
        }

        [Fact]
        public void GetBytes_Throws_Small_Destination()
        {
            Page p = new(512);
            int[] offsets = { 0, 128, 96, 300 };
            byte[] bytes = new byte[128];
            byte[] results = new byte[bytes.Length - 1];
            foreach (int offset in offsets)
            {
                Random.Shared.NextBytes(bytes);
                p.WriteBytes(offset, bytes);
                Assert.Throws<ArgumentException>(() => p.GetBytes(offset, results));
            }
        }

        [Fact]
        public void WriteString()
        {
            Page p = new(512);

            Dictionary<int, string> values = new()
            {
                [0] = Guid.NewGuid().ToString(),
                [128] = DateTime.UtcNow.ToString("o"),
                [256] = "The lazy brown fox!",
                [300] = int.MaxValue.ToString()
            };

            foreach(var (offset, value) in values)
            {
                p.WriteString(offset, value);
                string v = p.GetString(offset);
                Assert.Equal(value, v);
            }
        }
    }
}
