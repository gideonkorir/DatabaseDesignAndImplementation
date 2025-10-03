using System.Runtime.CompilerServices;
using SimpleDb.Record;

namespace SimpleDb.Query;

public class ProjectScan(IScan scan, IEnumerable<string> fieldNames) : IUpdateScan
{
    private bool _disposed = false;
    private readonly IUpdateScan? _updateScan = scan as IUpdateScan;
    private readonly HashSet<string> _fieldNames = new(fieldNames, StringComparer.OrdinalIgnoreCase);

    private Schema? _schema;

    public Schema Schema
    {
        get
        {
            if (_schema == null)
            {
                _schema = new Schema();
                foreach (var fieldName in _fieldNames)
                {
                    var field = scan.Schema[fieldName];
                    _schema.AddFieldAtOrdinal(fieldName, field.Ordinal, field.FieldType, field.Length);
                }
            }
            return _schema;
        }
    }

    public RID RID
    {
        get
        {
            CheckCanUpdate();
            return _updateScan!.RID;
        }
    }

    public void BeforeFirst() => scan.BeforeFirst();

    public int GetInt32(string fieldName)
    {
        CheckField(fieldName);
        return scan.GetInt32(fieldName);
    }

    public string GetString(string fieldName)
    {
        CheckField(fieldName);
        return scan.GetString(fieldName);
    }

    public Constant GetValue(string fieldName)
    {
        CheckField(fieldName);
        return scan.GetValue(fieldName);
    }

    public bool Next() => scan.Next();

    public bool TryGetInt32(string fieldName, out int value)
    {
        CheckField(fieldName);
        return scan.TryGetInt32(fieldName, out value);
    }

    public bool TryGetString(string fieldName, out string? value)
    {
        CheckField(fieldName);
        return scan.TryGetString(fieldName, out value);
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CheckField(string fieldName)
    {
        if (!_fieldNames.Contains(fieldName))
            throw new ArgumentException($"field {fieldName} not in projection");
    }

    public void SetValue(string fieldName, int value)
    {
        CheckCanUpdate();
        CheckField(fieldName);
        _updateScan!.SetValue(fieldName, value);
    }

    public void SetValue(string fieldName, string value)
    {
        CheckCanUpdate();
        CheckField(fieldName);
        _updateScan!.SetValue(fieldName, value);
    }

    public void SetValue(string fieldName, Constant value)
    {
        CheckCanUpdate();
        CheckField(fieldName);
        _updateScan!.SetValue(fieldName, value);
    }

    public void Insert()
    {
        CheckCanUpdate();
        _updateScan!.Insert();
    }

    public void Delete()
    {
        CheckCanUpdate();
        _updateScan!.Delete();
    }

    public void MoveToRID(RID rid)
    {
        CheckCanUpdate();
        _updateScan!.MoveToRID(rid);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CheckCanUpdate()
    {
        if (_updateScan == null)
            throw new NotSupportedException("The underlying scan is not updatable");
    }
}