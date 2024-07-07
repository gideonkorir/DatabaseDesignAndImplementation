using SimpleDb.BufferPool;
using SimpleDb.Files;

namespace SimpleDb.UnitTests.BufferPool
{
    public class BufferManagerTests : IClassFixture<DbFixture>
    {
        private const int POOL_SIZE = 5;
        private static readonly string s_file = $"{Guid.NewGuid()}.db";
        private readonly DbFixture dbFixture;
        public BufferManagerTests(DbFixture dbFixture) 
        {
            this.dbFixture = dbFixture ?? throw new ArgumentNullException(nameof(dbFixture));
        }

        [Theory]
        [InlineData(BufferReplacementStrategy.Naive)]
        [InlineData(BufferReplacementStrategy.LRU)]
        [InlineData(BufferReplacementStrategy.LRM)]
        [InlineData(BufferReplacementStrategy.Clock)]
        public void BufferManager_Returns_Buffer(BufferReplacementStrategy replacementStrategy)
        {
            BufferMgr bufferMgr = new(dbFixture.FileManager, dbFixture.LogManager, POOL_SIZE, replacementStrategy);
            var blockId = new BlockId(s_file, 0);
            var buffer = bufferMgr.Pin(blockId);
            Assert.NotNull(buffer);
            Assert.Equal(blockId, buffer.BlockId!.Value);
        }

        [Theory]
        [InlineData(BufferReplacementStrategy.Naive)]
        [InlineData(BufferReplacementStrategy.LRU)]
        [InlineData(BufferReplacementStrategy.LRM)]
        [InlineData(BufferReplacementStrategy.Clock)]
        public void UnpinningBufferReleasesIt(BufferReplacementStrategy replacementStrategy)
        {
            BufferMgr bufferMgr = new(dbFixture.FileManager, dbFixture.LogManager, POOL_SIZE, replacementStrategy);
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

        [Theory]
        [InlineData(BufferReplacementStrategy.Naive)]
        [InlineData(BufferReplacementStrategy.LRU)]
        [InlineData(BufferReplacementStrategy.LRM)]
        [InlineData(BufferReplacementStrategy.Clock)]
        public void FailureToAcquireBufferThrows(BufferReplacementStrategy replacementStrategy)
        {
            BufferMgr bufferMgr = new(dbFixture.FileManager, dbFixture.LogManager, POOL_SIZE, replacementStrategy);
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
