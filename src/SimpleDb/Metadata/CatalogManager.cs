using SimpleDb.Record;
using SimpleDb.Tx;

namespace SimpleDb.Metadata;

public class CatalogManager
{
    public const int MAX_NAME = 16;
    private readonly Layout tcatLayout, fcatLayout, vcatLayout, idxcatLayout;

    public static readonly string
    TableCatalog = "tblcat",
    FieldCatalog = "fldcat",
    ViewCatalog = "viewcat",

    IndexCatalog = "idxcat";

    public CatalogManager(Transaction tx, bool isNew)
    {
        var tcatSchema = new Schema()
            .AddStringField("tblname", MAX_NAME)
            .AddIntField("slotsize");
        tcatLayout = new Layout(tcatSchema);

        var fcatSchema = new Schema()
            .AddStringField("tblname", MAX_NAME)
            .AddStringField("fldname", MAX_NAME)
            .AddIntField("ordinal")
            .AddIntField("type")
            .AddIntField("length")
            .AddIntField("offset");
        fcatLayout = new Layout(fcatSchema);

        var vcatSchema = new Schema()
            .AddStringField("viewname", MAX_NAME)
            .AddStringField("viewdef", 1000); //arbitrary length for now
        vcatLayout = new Layout(vcatSchema);

        var idxcatSchema = new Schema()
            .AddStringField("idxname", MAX_NAME)
            .AddStringField("tblname", MAX_NAME)
            .AddStringField("fldname", MAX_NAME);
        idxcatLayout = new Layout(idxcatSchema);

        if (isNew)
        {
            CreateTable(TableCatalog, tcatSchema, tx);
            CreateTable(FieldCatalog, fcatSchema, tx);
            CreateTable(ViewCatalog, vcatSchema, tx);
            CreateTable(IndexCatalog, idxcatSchema, tx);
        }
    }

    public void CreateTable(string tblName, Schema schema, Transaction tx)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tblName);
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(tx);

        Layout layout = new(schema);
        using (TableScan scan = new(tx, TableCatalog, tcatLayout))
        {
            scan.Insert();
            scan.SetValue("tblname", tblName);
            scan.SetValue("slotsize", layout.SlotSize);
        }

        using (TableScan scan = new(tx, FieldCatalog, fcatLayout))
        {
            foreach (var field in schema)
            {
                scan.Insert();
                scan.SetValue("tblname", tblName);
                scan.SetValue("fldname", field.Name);
                scan.SetValue("ordinal", field.Ordinal);
                scan.SetValue("type", (int)field.FieldType);
                scan.SetValue("length", field.Length);
                scan.SetValue("offset", layout.GetOffset(field.Name));
            }
        }
    }

    public Layout GetTableLayout(string tblName, Transaction tx)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tblName);
        ArgumentNullException.ThrowIfNull(tx);

        //check for system tables
        if (string.Equals(tblName, TableCatalog, StringComparison.OrdinalIgnoreCase))
            return tcatLayout;
        if (string.Equals(tblName, FieldCatalog, StringComparison.OrdinalIgnoreCase))
            return fcatLayout;
        if (string.Equals(tblName, ViewCatalog, StringComparison.OrdinalIgnoreCase))
            return vcatLayout;
        if (string.Equals(tblName, IndexCatalog, StringComparison.OrdinalIgnoreCase))
            return idxcatLayout;

        int slotsize = 0;
        using (TableScan scan = new(tx, TableCatalog, tcatLayout))
        {
            while (scan.Next())
            {
                if (scan.GetString("tblname").Equals(tblName, StringComparison.OrdinalIgnoreCase))
                {
                    slotsize = scan.GetInt32("slotsize");
                    break;
                }
            }
        }

        Schema schema = new();
        Dictionary<string, int> offsets = new(StringComparer.OrdinalIgnoreCase);

        using (TableScan scan = new(tx, FieldCatalog, fcatLayout))
        {
            while (scan.Next())
            {
                string name = scan.GetString("tblname");
                if (string.Equals(name, tblName, StringComparison.OrdinalIgnoreCase))
                {
                    string fldName = scan.GetString("fldname");
                    int ordinal = scan.GetInt32("ordinal");
                    int type = scan.GetInt32("type");
                    int length = scan.GetInt32("length");
                    int offset = scan.GetInt32("offset");
                    schema.AddFieldAtOrdinal(fldName, ordinal, (SchemaFieldType)type, length);
                    offsets.Add(fldName, offset);
                }

            }
        }

        return new Layout(schema, offsets, slotsize);
    }

    public void CreateView(string viewName, string viewDef, Transaction tx)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewDef);
        ArgumentNullException.ThrowIfNull(tx);

        using TableScan scan = new(tx, ViewCatalog, vcatLayout);
        scan.Insert();
        scan.SetValue("viewname", viewName);
        scan.SetValue("viewdef", viewDef);
    }

    public string GetViewDef(string viewName, Transaction tx)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);
        ArgumentNullException.ThrowIfNull(tx);

        using (TableScan scan = new(tx, ViewCatalog, vcatLayout))
        {
            while (scan.Next())
            {
                if (scan.GetString("viewname").Equals(viewName, StringComparison.OrdinalIgnoreCase))
                {
                    return scan.GetString("viewdef");
                }
            }
        }
        throw new ArgumentException($"The view '{viewName}' does not exist");
    }

    public void CreateIndex(string idxName, string tblName, string type, string fldName, Transaction tx)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(idxName);
        ArgumentException.ThrowIfNullOrWhiteSpace(tblName);
        ArgumentException.ThrowIfNullOrWhiteSpace(fldName);
        ArgumentNullException.ThrowIfNull(tx);

        using TableScan scan = new(tx, IndexCatalog, idxcatLayout);
        scan.Insert();
        scan.SetValue("idxname", idxName);
        scan.SetValue("tblname", tblName);
        scan.SetValue("indextype", type); //btree only for now
        scan.SetValue("fldname", fldName);
    }
    
    public IReadOnlyDictionary<string, IndexDefinition> GetIndexDefinition(string tableName, Transaction tx)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        ArgumentNullException.ThrowIfNull(tx);
        Dictionary<string, IndexDefinition> def = [];
        using (TableScan scan = new(tx, IndexCatalog, idxcatLayout))
        {
            while (scan.Next())
            {
                if (scan.GetString("tblName").Equals(tableName, StringComparison.OrdinalIgnoreCase))
                {
                    string name = scan.GetString("idxname");
                    string tblName = scan.GetString("tblname");
                    string type = scan.GetString("indextype");
                    string fldName = scan.GetString("fldname");
                    def.Add(name, new IndexDefinition(name, type, tblName, fldName));
                }
            }
        }
        return def;
    }

    public Layout GetIndexLayout(IndexDefinition indexDefinition, Tx.Transaction tx)
    {
        ArgumentNullException.ThrowIfNull(indexDefinition);
        Layout tableLayout = GetTableLayout(indexDefinition.TableName, tx);
        Schema s = new Schema()
            .AddIntField("block")
            .AddIntField("id"); //the rid of the record
        var field = tableLayout.GetField(indexDefinition.FieldName);
        if (field.FieldType == SchemaFieldType.I32)
            s.AddIntField("dataval");
        else
            s.AddStringField(indexDefinition.FieldName, Layout.LengthInBytes(field));
        return new Layout(s);
    }

    public Index Open(IndexDefinition indexDefinition, Tx.Transaction tx)
    {
        throw new NotImplementedException();
    }
}