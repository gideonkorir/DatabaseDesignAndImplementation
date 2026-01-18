using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        AS,
        From,

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
