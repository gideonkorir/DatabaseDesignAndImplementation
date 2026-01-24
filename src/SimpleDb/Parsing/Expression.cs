namespace SimpleDb.Parsing
{
    public abstract record class Expression : SyntaxNode
    {

    }


    public record class StarExpression(SyntaxToken StarToken) : Expression
    {
        public override SyntaxKind Kind => SyntaxKind.Star;
    }

    public record class UnaryExpression(
        SyntaxToken OperatorToken,
        Expression Operand) : Expression
    {
        public override SyntaxKind Kind => SyntaxKind.Unary;
    }

    public record class LiteralExpression(
        SyntaxToken LiteralToken) : Expression
    {
        public override SyntaxKind Kind => SyntaxKind.Literal;
    }

    public record class AssignmentExpression(
        SyntaxToken Identifier,
        SyntaxToken EqualToken,
        Expression Value) : Expression
    {
        public override SyntaxKind Kind => SyntaxKind.Assignment;
    }

    public record class BinaryExpression(
        Expression Left,
        SyntaxToken OperatorToken,
        Expression Right) : Expression
    {
        public override SyntaxKind Kind => SyntaxKind.Binary;
    }

    public record class MemberAccessExpression(
        SyntaxToken Member) : Expression
    {
        public override SyntaxKind Kind => SyntaxKind.MemberAccess;
    }

    public record class AliasExpression(
        Expression Expression,
        SyntaxToken AsToken,
        SyntaxToken Alias) : Expression
    {
        public override SyntaxKind Kind => SyntaxKind.Alias;
    }

    public record class ParenthesizedExpression(
        SyntaxToken LeftParen,
        Expression InnerExpression,
        SyntaxToken RightParen) : Expression
    {
        public override SyntaxKind Kind => SyntaxKind.ParenthesizedExpr;
    }

    public record class MethodCallExpression(
        SyntaxToken MethodName,
        SyntaxToken LeftParen,
        List<Expression> Arguments,
        SyntaxToken RightParen) : Expression
    {
        public override SyntaxKind Kind => SyntaxKind.MethodCall;
    }
}
