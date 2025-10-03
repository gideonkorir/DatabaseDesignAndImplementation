using SimpleDb.Record;

namespace SimpleDb.Query;

public class UnionScan : IScan
{
    private bool _disposed = false;
    private readonly IScan _left, _right;

    public Schema Schema => _left.Schema;

    public UnionScan(IScan left, IScan right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        if (!left.Schema.Equals(right.Schema))
            throw new InvalidOperationException($"Cannot create a union of 2 scans with differing schemas");

        _left = left;
        _right = right;
    }

    public void BeforeFirst()
    {
        _left.BeforeFirst();
        _right.BeforeFirst();
    }
    public bool Next()
    {
        if (_left.Next())
            return true;
        return _right.Next();
    }

    public int GetInt32(string fieldName)
    {
        if (_left.TryGetInt32(fieldName, out var value))
            return value;
        return _right.GetInt32(fieldName);
    }

    public string GetString(string fieldName)
    {
        if (_left.TryGetString(fieldName, out var value))
            return value!;
        return _right.GetString(fieldName);
    }

    public Constant GetValue(string fieldName)
    {
        if (_left.TryGetString(fieldName, out var svalue))
            return new Constant(svalue!);
        if (_left.TryGetInt32(fieldName, out var ivalue))
            return new Constant(ivalue);
        return _right.GetValue(fieldName);
    }

    public bool TryGetInt32(string fieldName, out int value)
    {
        if (_left.TryGetInt32(fieldName, out value))
            return true;
        return _right.TryGetInt32(fieldName, out value);
    }

    public bool TryGetString(string fieldName, out string? value)
    {
        if (_left.TryGetString(fieldName, out value))
            return true;
        return _right.TryGetString(fieldName, out value);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                _left.Dispose();
                _right.Dispose();
            }
            _disposed = true;
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
}