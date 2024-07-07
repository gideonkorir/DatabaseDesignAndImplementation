using SimpleDb.BufferPool;
using SimpleDb.Files;

namespace SimpleDb.UnitTests.BufferPool
{
    public class BufferManagerTests : IClassFixture<DbFixture>
    {
        private readonly BufferMgr bufferMgr;
        private const int POOL_SIZE = 5;
        private static readonly string s_file = $"{Guid.NewGuid()}.db";
        public BufferManagerTests(DbFixture dbFixture) 
        {
            bufferMgr = new(dbFixture.FileManager, dbFixture.LogManager, POOL_SIZE);
        }

        [Fact]
        public void BufferManager_Returns_Buffer()
        {
            var blockId = new BlockId(s_file, 0);
            var buffer = bufferMgr.Pin(blockId);
            Assert.NotNull(buffer);
            Assert.Equal(blockId, buffer.BlockId!.Value);
        }

        [Fact]
        public void UnpinningBufferReleasesIt()
        {
            var block1 = new BlockId(s_file, 0);
            var block2 = new BlockId(s_file, 1);

            var buffer = bufferMgr.Pin(block1);
            Assert.Equal(POOL_SIZE - 1, bufferMgr.FreeBufferCount);
            var buffer2 = bufferMgr.Pin(block2);
            Assert.Equal(POOL_SIZE - 2, bufferMgr.FreeBufferCount);

            bufferMgr.Unpin(buffer);
            Assert.Equal(POOL_SIZE - 1, bufferMgr.FreeBufferCount);
            bufferMgr.Unpin(buffer2);
            Assert.Equal(POOL_SIZE, bufferMgr.FreeBufferCount);
        }

        [Fact]
        public void FailureToAcquireBufferThrows()
        {
            List<BlockId> blocks = [];
            for(int i = 0; i < POOL_SIZE; i++)
            {
                blocks.Add(new BlockId(s_file, i));
                bufferMgr.Pin(blocks[i]);
            }
            Assert.Throws<BufferAbortException>(() => bufferMgr.Pin(new BlockId(s_file, POOL_SIZE)));
        }
    }
}
