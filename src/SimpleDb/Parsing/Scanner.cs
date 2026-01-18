using System.Runtime.CompilerServices;
using System.Text;

namespace SimpleDb.Parsing
{
    public class Scanner(string queryText)
    {
        private static readonly Dictionary<string, TokenType> _keywords = new(StringComparer.OrdinalIgnoreCase)
        {
            ["select"] = TokenType.Select,
            ["update"] = TokenType.Update,
            ["delete"] = TokenType.Delete,
            ["from"] = TokenType.From,
            ["as"] = TokenType.AS,
            ["and"] = TokenType.And,
            ["or"] = TokenType.Or
        };

        private int _pos = 0, _start = 0;
        private List<SyntaxToken> _tokens = [];

        private bool IsAtEnd => _pos >= queryText.Length;

        private char Current => IsAtEnd ? '\0' : queryText[_pos];

        public List<SyntaxError> SyntaxErrors { get; } = [];

        public List<SyntaxToken> GetTokens()
        {
            //Our query can start with
            //select, update and delete
            while (!IsAtEnd)
            {
                _start = _pos;
                char c = Advance();
                switch(c)
                {
                    case '(':
                        AddToken(TokenType.LeftParen, null);
                        break;
                    case ')':
                        AddToken(TokenType.RightParen, null);
                        break;
                    case '+':
                        AddToken(TokenType.Plus, null);
                        break;
                    case '-': 
                        AddToken(TokenType.Minus, null);
                        break;
                    case '*':
                        AddToken(TokenType.Star, null);
                        break;
                    case '/':
                        AddToken(TokenType.ForwardSlash, null);
                        break;
                    case '%':
                        AddToken(TokenType.Percent, null);
                        break;
                    case '!':
                        AddToken(Match('=') ? TokenType.BangEqual : TokenType.Bang, null);
                        break;
                    case '=':
                        AddToken(TokenType.Equal, null);
                        break;
                    case '<':
                        AddToken(Match('=') ? TokenType.LessThanEqual : TokenType.LessThan, null);
                        break;
                    case '>':
                        AddToken(Match('=') ? TokenType.GreaterThanEqual : TokenType.GreaterThan, null);
                        break;
                    case '\'':
                        AddString();
                        break;
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                        AddNumber();
                        break;
                    case '\r':
                    case ' ':
                    case '\t':
                        break;
                    default:
                        {
                            if (char.IsLetter(c))
                            {
                                AddIdentifierOrKeyword();
                            }
                            else
                            {
                                Error($"Unexpected char: '{c}'.");
                            }
                            break;
                        }
                }
                
            }

            return _tokens;
        }

       
        private char Peek()
        {
            int p = _pos + 1;
            if(p < queryText.Length) 
                return queryText[p];
            return '\0';
        }

        private char Peek(int count)
        {
            int p = _pos + count;
            if(count < queryText.Length)
                return queryText[p];
            return '\0';
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private char Advance()
        {
            char c = queryText[_pos];
            _pos++;
            return c;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddToken(TokenType tokenType, object? literal)
        {
            var lexeme = queryText[_start.._pos];
            _tokens.Add(new SyntaxToken(tokenType, lexeme, _start, literal));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool Match(char c)
        {
            if (Current == c)
            {
                _pos ++;
                return true;
            }
            return false;
        }

        private void AddNumber()
        {
            while (char.IsDigit(Current))
                Advance();
            //_pos is now at the first non digit char
            //so the number is from _start to _pos-1
            if (int.TryParse(queryText.AsSpan(_start, _pos - _start), null, out int v))
                AddToken(TokenType.IntValue, v);
            Error($"Unable to parse number");
        }

        private void AddString()
        {
            StringBuilder sb = new StringBuilder();
            while (!IsAtEnd)
            {
                char c = Advance();
                if(c == '\'')
                {
                    if (Current == '\'') //Handle ''. The previous quote is escaped.
                    {
                        sb.Append('\''); //we escape quote ' by writing as double quote ''
                        Advance();
                    }
                    else
                        break;
                }
                else
                {
                    sb.Append(c);
                }
            }
            AddToken(TokenType.StringValue, sb.ToString());
        }

        private void AddIdentifierOrKeyword()
        {
            while(char.IsLetterOrDigit(Current))
            {
                Advance();
            }
            string text = queryText[_start.._pos];
            TokenType tokenType = _keywords.TryGetValue(text, out var type) ? type : TokenType.Identifier;
            AddToken(tokenType, text);
        }

        private void Error(string message)
            => SyntaxErrors.Add(new Parsing.SyntaxError(message, _start));
    }
}
