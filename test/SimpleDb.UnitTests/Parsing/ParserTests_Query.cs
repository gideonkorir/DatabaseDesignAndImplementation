using SimpleDb.Parsing;

namespace SimpleDb.UnitTests.Parsing
{
    public partial class ParserTests
    {

        [Theory]
        [MemberData(nameof(GetSimpleQueries))]
        public void Parser_ParseSelectExpression_ReturnsQueryExpression(List<SyntaxToken> tokens, string column, string table)
        {
            var parser = new Parser(tokens);
            // Act
            var expression = parser.Parse() as SelectStatement;
            // Assert
            Assert.NotNull(expression);
            Assert.Equal(SyntaxKind.SelectStatement, expression.Kind);

            string[] columns = column.Split(',', StringSplitOptions.RemoveEmptyEntries);

            Assert.Equal(columns.Length, expression.Values.Count);
            for (int i = 0; i < columns.Length; i++)
            {
                var valueExpr = expression.Values[i] as MemberAccessExpression;
                Assert.NotNull(valueExpr);
                Assert.Equal(columns[i], valueExpr.Member.Lexeme);
            }

            string[] tables = table.Split(',', StringSplitOptions.RemoveEmptyEntries);
            Assert.NotNull(expression.From.Source.TableOrViewName);
            Assert.Equal(tables[0], expression.From.Source.TableOrViewName.Lexeme);

        }


        [Theory, MemberData(nameof(GetFilteredQueries))]
        public void Parser_ParseFilteredSelectExpression_ReturnsQueryExpression(List<SyntaxToken> tokens, string column, string table)
        {
            var parser = new Parser(tokens);
            // Act
            var expression = parser.Parse() as SelectStatement;
            // Assert
            Assert.NotNull(expression);
            Assert.Equal(SyntaxKind.SelectStatement, expression.Kind);
            string[] columns = column.Split(',', StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(columns.Length, expression.Values.Count);
            for (int i = 0; i < columns.Length; i++)
            {
                var valueExpr = expression.Values[i] as MemberAccessExpression;
                Assert.NotNull(valueExpr);
                Assert.Equal(columns[i], valueExpr.Member.Lexeme);
            }
            string[] tables = table.Split(',', StringSplitOptions.RemoveEmptyEntries);
            Assert.NotNull(expression.From.Source.TableOrViewName);
            Assert.Equal(tables[0], expression.From.Source.TableOrViewName.Lexeme);
            Assert.NotNull(expression.Where);
            // Further assertions on the where clause can be added here
        }

        [Fact]
        public void Parse_Table_Alias()
        {
            List<SyntaxToken> tokens = [
                new(TokenType.Select, "select", 0, null),
                new(TokenType.Identifier, "column1", 7, null),
                new(TokenType.From, "from", 15, null),
                new(TokenType.Identifier, "table1", 20, null),
                new(TokenType.Identifier, "t1", 27, null),
                new(TokenType.EOF, "", 29, null)
                ];
            var parser = new Parser(tokens);
            var stmt = parser.Parse();
            Assert.NotNull(stmt);
            AssertIs<SelectStatement>(stmt, c =>
            {
                Assert.Single(c.Values);
                AssertIs<MemberAccessExpression>(c.Values[0], me =>
                {
                    Assert.Equal("column1", me.Member.Lexeme);
                });
                Assert.NotNull(c.From.Source.Alias);
                Assert.Equal("t1", c.From.Source.Alias!.Lexeme);
            });
        }

        [Fact]
        public void Parse_Column_Alias()
        {
            List<SyntaxToken> tokens = [
                new(TokenType.Select, "select", 0, null),
                new(TokenType.Identifier, "column1", 7, null),
                new(TokenType.As, "as", 14, null),
                new(TokenType.Identifier, "col1", 17, null),
                new(TokenType.From, "from", 22, null),
                new(TokenType.Identifier, "table1", 27, null),
                new(TokenType.EOF, "", 33, null)
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
                    Assert.Equal("col1", ae.Alias.Lexeme);
                });
            });
        }

        [Fact]
        public void Parse_NestedQuery()
        {
            List<SyntaxToken> tokens = [
                new(TokenType.Select, "select", 0, null),
                new(TokenType.Identifier, "column1", 7, null),
                new(TokenType.From, "from", 15, null),
                new(TokenType.LeftParen, "(", 20, null),
                new(TokenType.Select, "select", 21, null),
                new(TokenType.Identifier, "column2", 28, null),
                new(TokenType.From, "from", 36, null),
                new(TokenType.Identifier, "table2", 41, null),
                new(TokenType.RightParen, ")", 47, null),
                new(TokenType.Identifier, "t1", 49, null),
                new(TokenType.EOF, "", 51, null)
                ];
            var parser = new Parser(tokens);
            var stmt = parser.Parse();
            Assert.NotNull(stmt);
            AssertIs<SelectStatement>(stmt, c =>
            {
                Assert.Single(c.Values);
                AssertIs<MemberAccessExpression>(c.Values[0], me =>
                {
                    Assert.Equal("column1", me.Member.Lexeme);
                });
                Assert.NotNull(c.From.Source.NestedQuery);
                Assert.NotNull(c.From.Source.Alias);
                Assert.Equal("t1", c.From.Source.Alias!.Lexeme);
            });
        }

        [Fact]
        public void Parse_NestedQuery_With_Alias()
        {
            List<SyntaxToken> tokens = [
                new(TokenType.Select, "select", 0, null),
                new(TokenType.Identifier, "column1", 7, null),
                new(TokenType.From, "from", 15, null),
                new(TokenType.LeftParen, "(", 20, null),
                new(TokenType.Select, "select", 21, null),
                new(TokenType.Identifier, "column2", 28, null),
                new(TokenType.As, "as", 35, null),
                new(TokenType.Identifier, "col2", 38, null),
                new(TokenType.From, "from", 43, null),
                new(TokenType.Identifier, "table2", 48, null),
                new(TokenType.RightParen, ")", 54, null),
                new(TokenType.Identifier, "t1", 56, null),
                new(TokenType.EOF, "", 58, null)
                ];
            var parser = new Parser(tokens);
            var stmt = parser.Parse();
            Assert.NotNull(stmt);
            AssertIs<SelectStatement>(stmt, c =>
            {
                Assert.Single(c.Values);
                AssertIs<MemberAccessExpression>(c.Values[0], me =>
                {
                    Assert.Equal("column1", me.Member.Lexeme);
                });
                Assert.NotNull(c.From.Source.NestedQuery);
                Assert.NotNull(c.From.Source.Alias);
                Assert.Equal("t1", c.From.Source.Alias!.Lexeme);
            });
        }

        [Fact]
        public void Parse_SelectAllColumns()
        {
            List<SyntaxToken> tokens = [
                new(TokenType.Select, "select", 0, null),
                new(TokenType.Star, "*", 7, null),
                new(TokenType.From, "from", 9, null),
                new(TokenType.Identifier, "table1", 14, null),
                new(TokenType.EOF, "", 20, null)
                ];
            var parser = new Parser(tokens);
            var stmt = parser.Parse();
            Assert.NotNull(stmt);
            AssertIs<SelectStatement>(stmt, c =>
            {
                Assert.Single(c.Values);
                AssertIs<StarExpression>(c.Values[0], se =>
                {
                });
            });
        }

        [Fact]
        public void Parse_Method_Call_In_Select()
        {
            List<SyntaxToken> tokens = [
                new(TokenType.Select, "select", 0, null),
                new(TokenType.Identifier, "MAX", 7, null),
                new(TokenType.LeftParen, "(", 10, null),
                new(TokenType.Identifier, "column1", 11, null),
                new(TokenType.RightParen, ")", 18, null),
                new(TokenType.From, "from", 20, null),
                new(TokenType.Identifier, "table1", 25, null),
                new(TokenType.EOF, "", 31, null)
                ];
            var parser = new Parser(tokens);
            var stmt = parser.Parse();
            Assert.NotNull(stmt);
            AssertIs<SelectStatement>(stmt, c =>
            {
                Assert.Single(c.Values);
                AssertIs<MethodCallExpression>(c.Values[0], mce =>
                {
                    Assert.Equal("MAX", mce.MethodName.Lexeme);
                    Assert.Single(mce.Arguments);
                    AssertIs<MemberAccessExpression>(mce.Arguments[0], me =>
                    {
                        Assert.Equal("column1", me.Member.Lexeme);
                    });
                });
            });
        }

        [Fact]
        public void Parse_TableValuedFunction_Select()
        {
            List<SyntaxToken> tokens = [
                new(TokenType.Select, "select", 0, null),
                new(TokenType.Identifier, "column1", 7, null),
                new(TokenType.From, "from", 15, null),
                new(TokenType.Identifier, "GetItems", 20, null),
                new(TokenType.LeftParen, "(", 28, null),
                new(TokenType.RightParen, ")", 29, null),
                new(TokenType.EOF, "", 31, null)
                ];
            var parser = new Parser(tokens);
            var stmt = parser.Parse();
            Assert.NotNull(stmt);
            AssertIs<SelectStatement>(stmt, c =>
            {
                Assert.Single(c.Values);
                AssertIs<MemberAccessExpression>(c.Values[0], me =>
                {
                    Assert.Equal("column1", me.Member.Lexeme);
                });
                AssertIs<MethodCallExpression>(c.From.Source.TableValuedFunction!, mce =>
                {
                    Assert.Equal("GetItems", mce.MethodName.Lexeme);
                    Assert.Empty(mce.Arguments);
                });
            });
        }

        public static IEnumerable<object[]> GetSimpleQueries()
        {
            yield return [
                new List<SyntaxToken>
                {
                    new(TokenType.Select, "select", 0, null),
                    new(TokenType.Identifier, "column1", 7, null),
                    new(TokenType.From, "from", 15, null),
                    new(TokenType.Identifier, "table1", 20, null),
                    new(TokenType.EOF, "", 26, null)
                },
                "column1",
                "table1"
            ];

            yield return [
                new List<SyntaxToken>
                {
                    new(TokenType.Select, "select", 0, null),
                    new(TokenType.Identifier, "column1", 7, null),
                    new(TokenType.Comma, ",", 14, null),
                    new(TokenType.Identifier, "column2", 16, null),
                    new(TokenType.From, "from", 24, null),
                    new(TokenType.Identifier, "table1", 29, null),
                    new(TokenType.EOF, "", 35, null)
                },
                "column1,column2",
                "table1"
            ];
        }

        public static IEnumerable<object[]> GetFilteredQueries()
        {
            yield return [
                new List<SyntaxToken>
                {
                    new(TokenType.Select, "select", 0, null),
                    new(TokenType.Identifier, "column1", 7, null),
                    new(TokenType.From, "from", 15, null),
                    new(TokenType.Identifier, "table1", 20, null),
                    new(TokenType.Where, "where", 27, null),
                    new(TokenType.Identifier, "column1", 33, null),
                    new(TokenType.Equal, "=", 41, null),
                    new(TokenType.IntValue, "100", 43, 100),
                    new(TokenType.EOF, "", 26, null)
                },
                "column1",
                "table1"
            ];

            yield return [
                new List<SyntaxToken>
                {
                    new(TokenType.Select, "select", 0, null),
                    new(TokenType.Identifier, "column1", 7, null),
                    new(TokenType.Comma, ",", 14, null),
                    new(TokenType.Identifier, "column2", 16, null),
                    new(TokenType.From, "from", 24, null),
                    new(TokenType.Identifier, "table1", 29, null),
                    new(TokenType.Where, "where", 36, null),
                    new(TokenType.Identifier, "column2", 42, null),
                    new(TokenType.LessThan, "<", 50, null),
                    new(TokenType.IntValue, "200", 52, 200),
                    new(TokenType.EOF, "", 35, null)
                },
                "column1,column2",
                "table1"
            ];

            yield return [
                new List<SyntaxToken>
                {
                    new(TokenType.Select, "select", 0, null),
                    new(TokenType.Identifier, "column1", 7, null),
                    new(TokenType.From, "from", 15, null),
                    new(TokenType.Identifier, "table1", 20, null),
                    new(TokenType.Where, "where", 27, null),
                    new(TokenType.Identifier, "column2", 33, null),
                    new(TokenType.GreaterThan, ">", 41, null),
                    new(TokenType.IntValue, "50", 43, 50),
                    new(TokenType.Or, "or", 46, null),
                    new(TokenType.Identifier, "column1", 49, null),
                    new(TokenType.LessThanEqual, "<=", 57, null),
                    new(TokenType.IntValue, "10", 60, 10),
                    new(TokenType.EOF, "", 45, null)
                },
                "column1",
                "table1"
            ];
        }
    }
}
