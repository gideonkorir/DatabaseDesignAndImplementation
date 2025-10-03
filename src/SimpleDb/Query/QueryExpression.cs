using SimpleDb.Record;

namespace SimpleDb.Query;

public class QueryExpression
{
    public Constant? Constant { get; }
    public string? FieldName { get; }

    public bool IsConstant => Constant is not null;

    public QueryExpression(Constant constant)
    {
        Constant = constant;
    }

    public QueryExpression(string fieldName)
    {
        FieldName = fieldName;
    }

    public bool IsFieldName(string fieldName)
    {
        if (IsConstant)
            return false;
        return FieldName!.Equals(fieldName, StringComparison.OrdinalIgnoreCase);
    }

    public Constant Evaluate(ScanRecord record)
    {
        if (IsConstant)
            return Constant!.Value;
        return record.GetValue(FieldName!);
    }

    public bool AppliesTo(Schema schema)
    {
        if (IsConstant)
            return true;
        return schema.TryGetField(FieldName!, out _);
    }

    public override string ToString() => IsConstant ? Constant!.Value.ToString() : FieldName!;
}
