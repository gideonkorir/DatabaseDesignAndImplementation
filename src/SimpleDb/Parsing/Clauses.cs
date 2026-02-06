namespace SimpleDb.Parsing
{
    public record class FromClause(SyntaxToken FromKeyword, SyntaxNode Source);

    public record class WhereClause(SyntaxToken WhereKeyword, Expression Condition);
}
