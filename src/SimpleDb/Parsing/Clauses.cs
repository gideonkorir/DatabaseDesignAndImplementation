namespace SimpleDb.Parsing
{
    public record class FromClause(SyntaxToken FromKeyword, QuerySource Source);

    public record class WhereClause(SyntaxToken WhereKeyword, Expression Condition);
}
