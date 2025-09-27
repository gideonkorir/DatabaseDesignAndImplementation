using SimpleDb.Metadata;
using SimpleDb.Record;
using Xunit.Abstractions;

namespace SimpleDb.UnitTests.Metadata;


public class TableManagerUnitTests : AbstractTxTest
{
    public TableManagerUnitTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
    {
    }

    [Fact]
    public void Catalog_Tables_Are_Created()
    {
        TableManager mgr = new(_tx, true);

        var tableCatalog = Path.Join(_fixture.FileManager.Directory.FullName, $"{TableManager.TableCatalog}.tbl");
        var fieldCatalog = Path.Join(_fixture.FileManager.Directory.FullName, $"{TableManager.FieldCatalog}.tbl");

        Assert.True(File.Exists(tableCatalog));
        Assert.True(File.Exists(fieldCatalog));
    }

    [Fact]
    public void TableCatalog_Persisted_OK()
    {
        TableManager mgr = new(_tx, true);

        Schema schema = new Schema()
            .AddIntField("id")
            .AddStringField("name", 32)
            .AddIntField("age");

        mgr.CreateTable("students", schema, _tx);

        string view = "CREATE VIEW student_view AS SELECT id, name FROM students WHERE age > 18";
        mgr.CreateView("student_view", view, _tx);

        _tx.Commit();

        var tx = NewTx();

        Layout layout = mgr.GetTableLayout("students", tx);

        foreach (var field in schema)
        {
            Assert.True(layout.Schema.TryGetField(field.Name, out var fieldInfo));
            Assert.Equal(field.FieldType, fieldInfo.FieldType);
            Assert.Equal(field.Length, fieldInfo.Length);
            Assert.Equal(field.Ordinal, fieldInfo.Ordinal);
            Assert.Equal(layout.GetOffset(field.Name), layout.GetOffset(fieldInfo.Name));
        }
        
        string retrievedView = mgr.GetViewDef("student_view", tx);
        Assert.Equal(view, retrievedView, StringComparer.OrdinalIgnoreCase);
    }
}