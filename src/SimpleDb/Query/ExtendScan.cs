using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography.X509Certificates;
using SimpleDb.Files;
using SimpleDb.Record;

namespace SimpleDb.Query;

public class ExtendScan : IScan
{
    private bool _disposed;
    private readonly IScan scan;
    private readonly List<ExtendScanField> extensions;

    private Schema? _schema;

    public Schema Schema
    {
        get
        {
            if (_schema is null)
            {
                _schema = new Schema();
                _schema.CopyFrom(scan.Schema);
                foreach (ExtendScanField f in extensions)
                {
                    _schema.AddFieldFromConstant(f.FieldName, f.Value);
                }
            }
            return _schema;
        }
    }

    public ExtendScan(IScan scan, List<ExtendScanField> extensions)
    {
        this.scan = scan;
        this.extensions = extensions;
    }

    public void BeforeFirst() => scan.BeforeFirst();

    public bool Next() => scan.Next();

    public int GetInt32(string fieldName)
    {
        if (TryGetInt32(fieldName, out var x))
            return x;
        throw new InvalidOperationException($"Field {fieldName} of type I32 was not found");
    }

    public string GetString(string fieldName)
    {
        if (TryGetString(fieldName, out string? value))
            return value!;
        throw new InvalidOperationException($"Field {fieldName} of type String was not found");
    }

    public Constant GetValue(string fieldName)
    {
        foreach (var item in extensions)
        {
            if (item.FieldName.Equals(fieldName, StringComparison.OrdinalIgnoreCase))
            {
                return item.Value;
            }
        }
        return scan.GetValue(fieldName);
    }

    public bool TryGetInt32(string fieldName, out int value)
    {
        if (scan.TryGetInt32(fieldName, out value))
            return true;
        foreach (var item in extensions)
        {
            if (item.FieldName.Equals(fieldName, StringComparison.OrdinalIgnoreCase) && item.Value.FieldType == SchemaFieldType.I32)
            {
                value = item.Value.IntValue!.Value;
                return true;
            }
        }
        value = 0;
        return false;
    }

    public bool TryGetString(string fieldName, [NotNullWhen(true)] out string? value)
    {
        if (scan.TryGetString(fieldName, out value))
            return true;
        foreach (var item in extensions)
        {
            if (item.FieldName.Equals(fieldName, StringComparison.OrdinalIgnoreCase) && item.Value.FieldType == SchemaFieldType.String)
            {
                value = item.Value.StringValue!;
                return true;
            }
        }
        value = null;
        return false;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            scan.Dispose();
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }
}
