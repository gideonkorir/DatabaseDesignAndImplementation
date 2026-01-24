using SimpleDb.Parsing;

namespace SimpleDb.UnitTests.Parsing
{
    public partial class ParserTests
    {
        [Fact]
        public void Parser_Simple_Arithmetic()
        {
            List<SyntaxToken> tokens = [
                new(TokenType.Select, "select", 0, null),
                new(TokenType.Identifier, "column1", 7, null),
                new(TokenType.Star, "*", 8, null),
                new(TokenType.IntValue, "100", 19, 100),
                new(TokenType.Comma, ",", 14, null),
                new(TokenType.Identifier, "column2", 16, null),
                new(TokenType.From, "from", 24, null),
                new(TokenType.Identifier, "table1", 29, null),
                new(TokenType.EOF, "",40, null),
                ];
            var parser = new Parser(tokens);
            var stmt = parser.Parse();
            Assert.NotNull(stmt);
            AssertIs<SelectStatement>(stmt, c =>
            {
                Assert.Equal(2, c.Values.Count);
                Assert.IsType<BinaryExpression>(c.Values[0]);
                BinaryExpression binaryExpression = (BinaryExpression)c.Values[0];
                AssertIs<MemberAccessExpression>(binaryExpression.Left, me =>
                {
                    Assert.Equal("column1", me.Member.Lexeme);
                });
                Assert.Equal(TokenType.Star, binaryExpression.OperatorToken.Type);
                AssertIs<LiteralExpression>(binaryExpression.Right, me =>
                {
                    Assert.Equal(100, me.LiteralToken.Literal);
                });
            });
        }

        [Fact]
        public void Parser_Operator_Precedence()
        {
            List<SyntaxToken> tokens = [
                new(TokenType.Select, "select", 0, null),
                new(TokenType.Identifier, "column1", 7, null),
                new(TokenType.Plus, "+", 15, null),
                new(TokenType.Identifier, "column2", 16, null),
                new(TokenType.Star, "*", 23, null),
                new(TokenType.IntValue, "10", 24, 10),
                new(TokenType.From, "from", 27, null),
                new(TokenType.Identifier, "table1", 32, null),
                new(TokenType.EOF, "",40, null),
                ];
            var parser = new Parser(tokens);
            var stmt = parser.Parse();
            Assert.NotNull(stmt);
            AssertIs<SelectStatement>(stmt, c =>
            {
                Assert.Single(c.Values);
                AssertIs<BinaryExpression>(c.Values[0], be =>
                {
                    AssertIs<MemberAccessExpression>(be.Left, me =>
                    {
                        Assert.Equal("column1", me.Member.Lexeme);
                    });
                    Assert.Equal(TokenType.Plus, be.OperatorToken.Type);
                    AssertIs<BinaryExpression>(be.Right, rightBe =>
                    {
                        AssertIs<MemberAccessExpression>(rightBe.Left, me2 =>
                        {
                            Assert.Equal("column2", me2.Member.Lexeme);
                        });
                        Assert.Equal(TokenType.Star, rightBe.OperatorToken.Type);
                        AssertIs<LiteralExpression>(rightBe.Right, le =>
                        {
                            Assert.Equal(10, le.LiteralToken.Literal);
                        });
                    });
                });
            });
        }

        [Fact]
        public void Parser_Parenthesized_Expression()
        {
            List<SyntaxToken> tokens = [
                new(TokenType.Select, "select", 0, null),
                new(TokenType.LeftParen, "(", 7, null),
                new(TokenType.Identifier, "column1", 8, null),
                new(TokenType.Plus, "+", 15, null),
                new(TokenType.IntValue, "50", 16, 50),
                new(TokenType.RightParen, ")", 18, null),
                new(TokenType.From, "from", 20, null),
                new(TokenType.Identifier, "table1", 25, null),
                new(TokenType.EOF, "",30, null),
                ];
            var parser = new Parser(tokens);
            var stmt = parser.Parse();
            Assert.NotNull(stmt);
            AssertIs<SelectStatement>(stmt, c =>
            {
                Assert.Single(c.Values);
                AssertIs<ParenthesizedExpression>(c.Values[0], pe =>
                {
                    AssertIs<BinaryExpression>(pe.InnerExpression, be =>
                    {
                        AssertIs<MemberAccessExpression>(be.Left, me =>
                        {
                            Assert.Equal("column1", me.Member.Lexeme);
                        });
                        Assert.Equal(TokenType.Plus, be.OperatorToken.Type);
                        AssertIs<LiteralExpression>(be.Right, le =>
                        {
                            Assert.Equal(50, le.LiteralToken.Literal);
                        });
                    });
                });
            });
        }

        [Fact]
        public void Parser_Nested_Expressions()
        {
            List<SyntaxToken> tokens = [
                new(TokenType.Select, "select", 0, null),
                new(TokenType.LeftParen, "(", 7, null),
                new(TokenType.Identifier, "column1", 8, null),
                new(TokenType.Plus, "+", 15, null),
                new(TokenType.LeftParen, "(", 16, null),
                new(TokenType.IntValue, "20", 17, 20),
                new(TokenType.Star, "*", 19, null),
                new(TokenType.Identifier, "column2", 20, null),
                new(TokenType.RightParen, ")", 27, null),
                new(TokenType.RightParen, ")", 28, null),
                new(TokenType.From, "from", 30, null),
                new(TokenType.Identifier, "table1", 35, null),
                new(TokenType.EOF, "",40, null),
                ];
            var parser = new Parser(tokens);
            var stmt = parser.Parse();
            Assert.NotNull(stmt);
            AssertIs<SelectStatement>(stmt, c =>
            {
                Assert.Single(c.Values);
                AssertIs<ParenthesizedExpression>(c.Values[0], pe =>
                {
                    AssertIs<BinaryExpression>(pe.InnerExpression, be =>
                    {
                        AssertIs<MemberAccessExpression>(be.Left, me =>
                        {
                            Assert.Equal("column1", me.Member.Lexeme);
                        });
                        Assert.Equal(TokenType.Plus, be.OperatorToken.Type);
                        AssertIs<ParenthesizedExpression>(be.Right, innerPe =>
                        {
                            AssertIs<BinaryExpression>(innerPe.InnerExpression, innerBe =>
                            {
                                AssertIs<LiteralExpression>(innerBe.Left, le =>
                                {
                                    Assert.Equal(20, le.LiteralToken.Literal);
                                });
                                Assert.Equal(TokenType.Star, innerBe.OperatorToken.Type);
                                AssertIs<MemberAccessExpression>(innerBe.Right, me2 =>
                                {
                                    Assert.Equal("column2", me2.Member.Lexeme);
                                });
                            });
                        });
                    });
                });
            });
        }

        [Fact]
        public void Parser_Aliasing_Columns()
        {
            List<SyntaxToken> tokens = [
                new(TokenType.Select, "select", 0, null),
                new(TokenType.Identifier, "column1", 7, null),
                new(TokenType.As, "as", 15, null),
                new(TokenType.Identifier, "col1_alias", 18, null),
                new(TokenType.From, "from", 30, null),
                new(TokenType.Identifier, "table1", 35, null),
                new(TokenType.EOF, "",40, null),
                ];
            var parser = new Parser(tokens);
            var stmt = parser.Parse();
            Assert.NotNull(stmt);
            AssertIs<SelectStatement>(stmt, c =>
            {
                Assert.Single(c.Values);
                AssertIs<AliasExpression>(c.Values[0], ae =>
                {
                    AssertIs<MemberAccessExpression>(ae.Expression, me =>
                    {
                        Assert.Equal("column1", me.Member.Lexeme);
                    });
                    Assert.Equal("col1_alias", ae.Alias.Lexeme);
                });
            });
        }

        private static void AssertIs<T>(object value, Action<T> next)
        {
            Assert.NotNull(value);
            Assert.IsType<T>(value);
            T vt = (T)value;
            next(vt);
        }
    }
}
