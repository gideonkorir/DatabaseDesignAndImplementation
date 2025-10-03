using SimpleDb.Record;

namespace SimpleDb.Query;

public class ProductScan : IScan
{
    private bool _disposed = false;
    private readonly IScan _left, _right;

    private Schema? _schema;

    private bool _leftScanHasAnyRecords;

    public Schema Schema
    {
        get
        {
            if (_schema is null)
            {
                _schema = new Schema();
                _schema.CopyFrom(_left.Schema);
                _schema.CopyFrom(_right.Schema);
            }
            return _schema;
        }
    }

    public ProductScan(IScan left, IScan right)
    {
        _left = left;
        _right = right;
        _leftScanHasAnyRecords = _left.Next();
    }

    public void BeforeFirst()
    {
        _left.BeforeFirst();
        _leftScanHasAnyRecords = _left.Next(); //position left scan on first record
        _right.BeforeFirst();
    }
    public bool Next()
    {
        if (!_leftScanHasAnyRecords)
            return false; //we have nothing to do

        if (_right.Next())
            return true;
        _right.BeforeFirst();
        if (_left.Next())
            return _right.Next();
        return false;
    }
    public int GetInt32(string fieldName)
    {
        if (_left.TryGetInt32(fieldName, out var value) || _right.TryGetInt32(fieldName, out value))
            return value;
        throw new ArgumentException($"Field '{fieldName}' not found.");
    }

    public string GetString(string fieldName)
    {
        if (_left.TryGetString(fieldName, out var value) || _right.TryGetString(fieldName, out value))
            return value ?? throw new InvalidOperationException($"Field '{fieldName}' is null.");
        throw new ArgumentException($"Field '{fieldName}' not found.");
    }

    public Constant GetValue(string fieldName)
    {
        if (_left.TryGetString(fieldName, out var svalue))
            return new Constant(svalue);
        if (_left.TryGetInt32(fieldName, out var ivalue))
            return new Constant(ivalue);
        if (_right.TryGetString(fieldName, out svalue))
            return new Constant(svalue);
        if (_right.TryGetInt32(fieldName, out ivalue))
            return new Constant(ivalue);
        throw new ArgumentException($"Field '{fieldName}' not found.");
    }

    public bool TryGetInt32(string fieldName, out int value)
    {
        if (_left.TryGetInt32(fieldName, out value) || _right.TryGetInt32(fieldName, out value))
            return true;
        value = default;
        return false;
    }

    public bool TryGetString(string fieldName, out string? value)
    {
        if (_left.TryGetString(fieldName, out value) || _right.TryGetString(fieldName, out value))
            return true;
        value = default;
        return false;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _left.Dispose();
            _right.Dispose();
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }
}