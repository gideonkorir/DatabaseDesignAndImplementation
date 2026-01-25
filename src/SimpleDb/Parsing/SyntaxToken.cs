namespace SimpleDb.Parsing
{
    public record class SyntaxToken(TokenType Type, string Lexeme, int Start, object? Literal);

    public enum TokenType
    {
        EOF,
        Select,
        Update,
        Delete,
        Identifier,
        Comma,
        As,
        From,
        Where,

        //binary
        Plus,
        Minus,
        Star,
        Percent,
        ForwardSlash,
        Equal,
        Bang,
        BangEqual,
        LessThan,
        LessThanEqual,
        GreaterThan,
        GreaterThanEqual,
        And,
        Or,

        //nonwords
        LeftParen,
        RightParen,


        //values
        IntValue,
        StringValue
    }
}
