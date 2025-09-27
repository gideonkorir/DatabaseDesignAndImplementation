using SimpleDb.Files;
using SimpleDb.Record;

namespace SimpleDb.UnitTests.Record
{
    public class LayoutTests
    {
        [Fact]
        public void Schema_Offset_Tests()
        {
            var schema = new Schema()
                .AddIntField("id")
                .AddStringField("name", 100)
                .AddStringField("ref", 48);
            var layout = new Layout(schema);
            int slotSize = layout.SlotSize;
            Assert.Equal(4, layout.GetOffset("id"));
            Assert.Equal(4 + Layout.LengthInBytes(layout.Schema["id"]), layout.GetOffset("name"));
            //extra 4 + slot size + length of name.
            Assert.Equal(4 + Layout.LengthInBytes(layout.Schema["name"]) + Layout.LengthInBytes(layout.Schema["id"]), layout.GetOffset("ref"));
        }
    }
}
