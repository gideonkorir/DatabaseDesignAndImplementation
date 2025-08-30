using SimpleDb.BufferPool;
using SimpleDb.Files;
using SimpleDb.Tx;

namespace SimpleDb.UnitTests.Tx;

public class BufferListTests : IClassFixture<DbFixture>
{
    const int POOL_SIZE = 3;
    private static readonly string s_file = $"{Guid.NewGuid()}.db";
    private readonly DbFixture _fixture;
    private readonly BufferMgr _bufferMgr;
    private readonly BufferList _bufferList;



    public BufferListTests(DbFixture fixture)
    {
        _fixture = fixture;
        _bufferMgr = new BufferMgr(fixture.FileManager, fixture.LogManager, POOL_SIZE, BufferReplacementStrategy.LRU);
        _bufferList = new BufferList(_bufferMgr);
    }

    // Add your test methods here
    [Fact]
    public void BufferManagerPinsBuffer()
    {
        var blockId = new BlockId(s_file, 1);
        _bufferList.Pin(blockId);
        Assert.NotNull(_bufferList.GetBuffer(blockId, out var pins));
        Assert.Equal(1, pins);
    }
}
