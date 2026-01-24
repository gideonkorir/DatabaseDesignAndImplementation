namespace SimpleDb.Parsing
{
    public record class SelectStatement(
        SyntaxToken SelectKeyword,
        List<Expression> Values, 
        FromClause From,
        WhereClause? Where) : Statement
    {
        public override SyntaxKind Kind => SyntaxKind.SelectStatement;
    }

    public record class SubQuery(
        SyntaxToken LeftParen,
        SelectStatement Query,
        SyntaxToken RightParen) : Statement
    {
        public override SyntaxKind Kind => SyntaxKind.SubQuery;
    }
}
