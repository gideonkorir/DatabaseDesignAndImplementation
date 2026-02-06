namespace SimpleDb.Parsing
{
    public record class UpdateStatement(
        SyntaxToken UpdateKeyword,
        SyntaxToken Table,
        SyntaxToken SetKeyword
        ) : Statement
    {
        public override SyntaxKind Kind => SyntaxKind.UpdateStatement;
    }
}
