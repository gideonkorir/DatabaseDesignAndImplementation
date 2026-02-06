using SimpleDb.Metadata;

namespace SimpleDb.Parsing
{
    public partial class Parser(List<SyntaxToken> tokens)
    {
        private int _current = 0;
        private SyntaxToken Current => Peek(0);

        public bool IsAtEnd() => Current.Type == TokenType.EOF;

        public Statement Parse()
        {
            return Current.Type switch
            {
                TokenType.Select => ParseSelectStatement(),
                TokenType.Update => ParseUpdateStatement(),
                TokenType.Delete => ParseDeleteStatement(),
                TokenType.EOF => throw new Exception("Unexpected end of input."),
                _ => throw new Exception($"Unexpected token: [{Current.Type}] '{Current.Lexeme}' as start of sql statement")
            };
        }

        private SelectStatement ParseSelectStatement()
        {
            SyntaxToken select = Match(TokenType.Select); // Consume 'SELECT'
            List<Expression> columns = [];
            while (!IsAtEnd())
            {
                columns.Add(Expression());
                if (Current.Type == TokenType.Comma)
                {
                    Advance(); // Consume ','
                }
                else
                {
                    // No more columns to parse
                    break;
                }
            }
            if(columns.Count == 0)
            {
                throw new Exception("Expected at least one column in SELECT statement.");
            }

            if (Current.Type == TokenType.EOF)
            {
                return new SelectStatement(select, columns, null, null);
            }

            FromClause fromExpr = FromClause();
            WhereClause? whereExpr = null;
            if(Current.Type == TokenType.Where)
            {
                whereExpr = WhereClause();
            }

            return new SelectStatement(select, columns, fromExpr, whereExpr);
        }

        private UpdateStatement ParseUpdateStatement()
        {
            throw new NotImplementedException();
        }

        private DeleteStatement ParseDeleteStatement()
        {
            throw new NotImplementedException();
        }

        private FromClause FromClause()
        {
            SyntaxToken fromToken = Match(TokenType.From); // Consume 'FROM'
            Expression src = Join();

            return new FromClause(fromToken, src);
        }

        private Expression Join()
        {
            Expression src = QuerySource();
            do
            {
                JoinType? type = null;
                if(Current.Type == TokenType.Comma)
                {
                    //select a, b from tb1, tb2 on tb1.a = tb2.k
                    Advance();
                    type = JoinType.Inner;
                }
                else if(Current.Type == TokenType.Join)
                {
                    //tb1 join tb2
                    Advance(); //consume join
                    type = JoinType.Inner;
                }
                else if (Current.Type == TokenType.Inner)
                {
                    //tb1 inner join tbl2
                    Advance(); //consume inner
                    Match(TokenType.Join); //consume join
                    type = JoinType.Inner;
                }
                else if (Current.Type == TokenType.Outer)
                {
                    //tbl outer join tbl2
                    Advance();
                    Match(TokenType.Join);
                    type = JoinType.Outer;
                }

                if(!type.HasValue)
                {
                    break;
                }
                Expression right = QuerySource();
                Match(TokenType.On);
                Expression condition = Expression();
                src = new JoinExpression(type.Value, src, right, condition);
            }
            while (true);
            return src;
        }

        private Expression QuerySource()
        {
            Expression source;

            switch (Current.Type)
            {
                case TokenType.LeftParen:
                    source = SubQuery();
                    break;
                case TokenType.Identifier:
                    if (Peek(1).Type == TokenType.LeftParen)
                    {
                        source = MethodCallExpression();
                    }
                    else
                    {
                        source = MemberAccess();
                    }
                    break;
                default:
                    throw new Exception("Unexpected token in FROM clause. " + Current.Type);

            }

            SyntaxToken? alias = null;
            SyntaxToken? asToken = null;

            if (Current.Type == TokenType.As)
            {
                asToken = Advance(); // Consume 'AS'
                alias = Match(TokenType.Identifier);
            }
            else if (Current.Type == TokenType.Identifier)
            {
                alias = Advance(); // Consume alias
            }


            if (alias is not null)
                return new AliasExpression(source, asToken, alias);

            return source;
                
        }

        private WhereClause WhereClause()
        {
            SyntaxToken whereToken = Advance(); // Consume 'WHERE'
            Expression condition = Or();
            return new WhereClause(whereToken, condition);
        }
        private Expression Expression()
        {
            return Alias();
        }

        Expression Alias()
        {
            Expression expr = Or();
            if(Current.Type == TokenType.As)
            {
                SyntaxToken asToken = Advance(); // Consume 'AS'
                SyntaxToken aliasToken = Match(TokenType.Identifier); // Consume identifier
                expr = new AliasExpression(expr, asToken, aliasToken);
            }
            return expr;
        }

        Expression Or()
        {
            Expression left = And();
            while (Current.Type == TokenType.Or)
            {
                SyntaxToken operatorToken = Advance(); // Consume 'OR'
                Expression right = And();
                left = new BinaryExpression(left, operatorToken, right);
            }
            return left;
        }

        Expression And()
        {
            Expression left = Equal();
            while (Current.Type == TokenType.And)
            {
                SyntaxToken operatorToken = Advance(); // Consume 'AND'
                Expression right = Equal();
                left = new BinaryExpression(left, operatorToken, right);
            }
            return left;
        }

        Expression Equal()
        {
            Expression left = Term();
            while (Current.Type == TokenType.Equal || Current.Type == TokenType.BangEqual
                || Current.Type == TokenType.LessThan || Current.Type == TokenType.LessThanEqual
                || Current.Type == TokenType.GreaterThan || Current.Type == TokenType.GreaterThanEqual)
            {
                SyntaxToken operatorToken = Advance();
                Expression right = Term();
                left = new BinaryExpression(left, operatorToken, right);
            }
            return left;
        }

        Expression Term()
        {
            Expression left = Factor();
            while (Current.Type == TokenType.Plus || Current.Type == TokenType.Minus)
            {
                SyntaxToken operatorToken = Advance(); // Consume '+' or '-'
                Expression right = Factor();
                left = new BinaryExpression(left, operatorToken, right);
            }
            return left;
        }

        Expression Factor()
        {
            Expression left = Unary();
            while (Current.Type == TokenType.Star || Current.Type == TokenType.ForwardSlash || Current.Type == TokenType.Percent)
            {
                SyntaxToken operatorToken = Advance(); // Consume '*' or '/'
                Expression right = Unary();
                left = new BinaryExpression(left, operatorToken, right);
            }
            return left;
        }

        Expression Unary()
        {
            if(Current.Type == TokenType.Minus || Current.Type == TokenType.Bang)
            {
                SyntaxToken operatorToken = Advance(); // Consume '-' or 'NOT'
                Expression right = Primary();
                return new UnaryExpression(operatorToken, right);
            }
            return Primary();
        }

        Expression Primary()
        {
            switch(Current.Type)
            {
                case TokenType.IntValue:
                    SyntaxToken numberToken = Advance(); // Consume number
                    return new LiteralExpression(numberToken);
                case TokenType.StringValue:
                    SyntaxToken stringToken = Advance(); // Consume string
                    return new LiteralExpression(stringToken);
                case TokenType.Identifier:
                    if(Peek(1).Type == TokenType.LeftParen)
                    {
                        // It's a method call
                        return MethodCallExpression();
                    }
                    else
                    {
                        return MemberAccess();
                    }
                case TokenType.LeftParen:
                    return ParenthesizedExpression();
                case TokenType.Star:
                    SyntaxToken starToken = Advance(); // Consume '*' in select * from
                    return new StarExpression(starToken);
                default:
                    throw new Exception($"Unexpected token: {Current.Type}");
            }
        }

        private SubQuery SubQuery()
        {
            SyntaxToken leftParen = Match(TokenType.LeftParen); // Consume '('
            SelectStatement expr = ParseSelectStatement();
            SyntaxToken rightParen = Match(TokenType.RightParen); // Consume ')'
            return new SubQuery(leftParen,expr, rightParen);
        }

        private ParenthesizedExpression ParenthesizedExpression()
        {
            SyntaxToken leftParen = Match(TokenType.LeftParen); // Consume '('
            Expression expr = Expression();
            SyntaxToken rightParen = Match(TokenType.RightParen); // Consume ')'
            return new ParenthesizedExpression(leftParen, expr, rightParen);
        }

        private MethodCallExpression MethodCallExpression()
        {
            SyntaxToken methodName = Match(TokenType.Identifier); // Consume method name
            SyntaxToken leftParen = Match(TokenType.LeftParen); // Consume '('
            List<Expression> arguments = [];
            if (Current.Type != TokenType.RightParen)
            {
                do
                {
                    arguments.Add(Expression());
                    if (Current.Type == TokenType.Comma)
                    {
                        Advance(); // Consume ','
                    }
                    else
                    {
                        break;
                    }
                } while (true);
            }
            SyntaxToken rightParen = Match(TokenType.RightParen); // Consume ')'
            return new MethodCallExpression(methodName, leftParen, arguments, rightParen);
        }

        private MemberAccessExpression MemberAccess()
        {
            SyntaxToken identifierToken = Advance(); // Consume identifier
            if (Current.Type == TokenType.Dot)
            {
                SyntaxToken dot = Advance();
                // Consume identifier
                SyntaxToken qualifiedIdentifier = Match(TokenType.Identifier);
                return new MemberAccessExpression(identifierToken, dot, qualifiedIdentifier);
            }
            else
            {
                return new MemberAccessExpression(identifierToken);
            }
        }

        public SyntaxToken Advance()
        {
            SyntaxToken token = Current;
            if (!IsAtEnd())
                _current++;
            return token;
        }

        private SyntaxToken Peek(int offset)
        {
            int index = _current + offset;
            if (index >= tokens.Count)
                return tokens[^1];
            return tokens[index];
        }

        private SyntaxToken Match(TokenType expectedType)
        {
            if (Current.Type != expectedType)
                throw new Exception($"Expected token of type {expectedType}, but found {Current.Type}.");
            return Advance();
        }


    }
}
