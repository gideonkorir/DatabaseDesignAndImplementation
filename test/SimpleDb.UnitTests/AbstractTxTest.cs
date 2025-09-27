using Meziantou.Extensions.Logging.Xunit;
using Microsoft.Extensions.Logging;
using SimpleDb.BufferPool;
using SimpleDb.Tx;
using Xunit.Abstractions;

namespace SimpleDb.UnitTests;

public abstract class AbstractTxTest
{
    protected readonly DbFixture _fixture;
    protected readonly Transaction _tx;
    protected readonly BufferMgr _bufferMgr;
    protected readonly ILoggerFactory _loggerFactory;

    protected AbstractTxTest(ITestOutputHelper testOutputHelper)
    {
        _fixture = new DbFixture();
        _bufferMgr = new BufferMgr(_fixture.FileManager, _fixture.LogManager, 10, BufferReplacementStrategy.Clock);
        _loggerFactory = LoggerFactory.Create(configure =>
        {
            configure.AddProvider(new XUnitLoggerProvider(testOutputHelper));
        });
        _tx = NewTx();
    }

    protected Transaction NewTx()
    {
        ILogger<Transaction> logger = _loggerFactory.CreateLogger<Transaction>();
        var tx = new Transaction(_fixture.FileManager, _fixture.LogManager, _bufferMgr, logger);
        return tx;
    }
}