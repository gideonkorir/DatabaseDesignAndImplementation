namespace SimpleDb.Parsing
{
    public record class DeleteStatement : Statement
    {
        public override SyntaxKind Kind => SyntaxKind.DeleteStatement;
    }
}
