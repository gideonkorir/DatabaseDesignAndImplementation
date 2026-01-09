using SimpleDb.Record;
using System.Runtime.CompilerServices;

namespace SimpleDb.Query;

public class Term
{
    public QueryExpression Left { get; }
    public QueryExpression Right { get; }

    public BinaryOperator Operator { get; }

    public Term(QueryExpression left, QueryExpression right, BinaryOperator @operator)
    {
        Left = left;
        Right = right;
        Operator = @operator;
    }

    public bool IsSatisfied(ScanRecord record)
    {
        Constant leftVal = Left.Evaluate(record);
        Constant rightVal = Right.Evaluate(record);
        if (leftVal.FieldType == SchemaFieldType.I32)
        {
            if (rightVal.FieldType == SchemaFieldType.I32)
            {
                return Operator switch
                {
                    BinaryOperator.Equal => leftVal.IntValue == rightVal.IntValue,
                    BinaryOperator.GreaterThan => leftVal.IntValue > rightVal.IntValue,
                    BinaryOperator.GreaterThanOrEqualTo => leftVal.IntValue >= rightVal.IntValue,
                    BinaryOperator.LessThan => leftVal.IntValue < rightVal.IntValue,
                    BinaryOperator.LessThanOrEqualTo => leftVal.IntValue <= rightVal.IntValue,
                    _ => throw new NotImplementedException("Unknown operator for int/int")
                };
            }
            else
                throw new NotImplementedException($"Unexpected int/string comparison '{Operator}'");
        }
        else if (leftVal.FieldType == SchemaFieldType.String)
        {
            if (rightVal.FieldType == SchemaFieldType.String)
            {
                return Operator switch
                {
                    BinaryOperator.Equal => string.Equals(leftVal.StringValue, rightVal.StringValue, StringComparison.OrdinalIgnoreCase),
                    BinaryOperator.GreaterThan => string.Compare(leftVal.StringValue, rightVal.StringValue, true) > 0,
                    BinaryOperator.GreaterThanOrEqualTo => string.Compare(leftVal.StringValue, leftVal.StringValue, true) >= 0,
                    BinaryOperator.LessThan => string.Compare(leftVal.StringValue, rightVal.StringValue, true) < 0,
                    BinaryOperator.LessThanOrEqualTo => string.Compare(leftVal.StringValue, rightVal.StringValue, true) <= 0,
                    _ => throw new NotImplementedException($"Unexpected string/string comparison: '{Operator}'")
                };
            }
        }
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
