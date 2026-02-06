namespace SimpleDb.Parsing
{
    public enum SyntaxKind
    {
        //select
        Literal,
        Binary,
        Where,
        From,

        //statements
        SelectStatement,
        UpdateStatement,
        DeleteStatement,
        TableAccess,
        SubQuery,
        JoinExpression,

        //expressions
        Star,
        Unary,
        ParenthesizedExpr,
        Assignment,
        MemberAccess,
        Alias,
        MethodCall
    }
    public abstract record class SyntaxNode
    {
        public abstract SyntaxKind Kind { get; }
    }
    public abstract record class Statement : SyntaxNode
    {
    }

    public enum JoinType {  Inner, Outer }

    public record class JoinExpression(JoinType JoinType, Expression Left, Expression Right, Expression Condition)
        : Expression
    {
        public override SyntaxKind Kind => SyntaxKind.JoinExpression;
    }

}
