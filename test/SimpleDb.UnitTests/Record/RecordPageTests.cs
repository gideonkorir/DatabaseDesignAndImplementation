using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using SimpleDb.BufferPool;
using SimpleDb.Record;
using SimpleDb.Tx;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDb.UnitTests.Record
{
    public class RecordPageTests : IClassFixture<DbFixture>
    {
        private readonly DbFixture fixture;
        private readonly ILogger<Transaction> _logger;

        public RecordPageTests(DbFixture fixture)
        {
            this.fixture = fixture;
            _logger = NullLoggerFactory.Instance.CreateLogger<Transaction>();
        }

        [Fact]
        public void RecordPage_Formatting_Tests()
        {
            var pool = new BufferMgr(
                fixture.FileManager,
                fixture.LogManager,
                4096,
                BufferReplacementStrategy.LRU
                );
            var tx = new Transaction(fixture.FileManager, fixture.LogManager, pool, _logger);

            var schema = new Schema()
                .AddIntField("id")
                .AddStringField("name", 96);
            var layout = new Layout(schema);

            var page = new RecordPage(tx, new SimpleDb.Files.BlockId("tests.tbl", 0), layout);

            page.Format();

            int slot = page.InsertAfter(-1);
            Assert.Equal(0, slot);
            page.SetInt(slot, "id", 10);
            page.SetString(slot, "name", "Gideon Korir");
            slot = page.InsertAfter(-1);
            Assert.Equal(1, slot);
            page.SetInt(slot, "id", 50);
            page.SetString(slot, "name", "testing database design and implementation");

            //read the values
            Assert.Equal(10, page.GetInt(0, "id"));
            Assert.Equal("Gideon Korir", page.GetString(0, "name"));

        }
    }
}
