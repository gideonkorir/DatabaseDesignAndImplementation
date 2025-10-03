using SimpleDb.Record;

namespace SimpleDb.Query;

public class Term
{
    public QueryExpression Left { get; }
    public QueryExpression Right { get; }

    public Term(QueryExpression left, QueryExpression right)
    {
        Left = left;
        Right = right;
    }

    public bool IsSatisfied(ScanRecord record)
    {
        Constant leftVal = Left.Evaluate(record);
        Constant rightVal = Right.Evaluate(record);
        return leftVal.Equals(rightVal);
    }

    public bool AppliesTo(Schema schema) => Left.AppliesTo(schema) && Right.AppliesTo(schema);

    public Constant? EquatesWithConstant(string fieldName)
    {
        if (Left.IsFieldName(fieldName) && Right.IsConstant)
            return Right.Constant!.Value;
        if (Right.IsFieldName(fieldName) && Left.IsConstant)
            return Left.Constant!.Value;
        return null;
    }

    public string? EquatesWithField(string fieldName)
    {
        if (Left.IsFieldName(fieldName) && !Right.IsConstant)
            return Right.FieldName!;
        if (Right.IsFieldName(fieldName) && !Left.IsConstant)
            return Left.FieldName!;
        return null;
    }

    public override string ToString() => $"{Left} = {Right}";
}