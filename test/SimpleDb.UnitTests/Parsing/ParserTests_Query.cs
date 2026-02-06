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
            AssertIs<MemberAccessExpression>(expression.From!.Source, ts =>
            {
                Assert.NotNull(ts.Member);
                Assert.Equal(table, ts.Member.Lexeme);
            });

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
            AssertIs<MemberAccessExpression>(expression.From!.Source, ts =>
            {
                Assert.NotNull(ts.Member);
                Assert.Equal(table, ts.Member.Lexeme);
            });
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
                AssertIs<AliasExpression>(c.From!.Source, alias =>
                {
                    Assert.Equal("t1", alias.Alias.Lexeme);
                    AssertIs<MemberAccessExpression>(alias.Expression, ts =>
                    {
                        Assert.NotNull(ts.Member);
                        Assert.Equal("table1", ts.Member.Lexeme);
                    });
                });
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
                AssertIs<AliasExpression>(c.From!.Source, alias =>
                {
                    Assert.Equal("t1", alias.Alias!.Lexeme);
                    AssertIs<SubQuery>(alias.Expression, sq =>
                    {
                        Assert.NotNull(sq.Query);
                        AssertIs<SelectStatement>(sq.Query, inner =>
                        {
                            Assert.Single(inner.Values);
                            AssertIs<MemberAccessExpression>(inner.Values[0], me =>
                            {
                                Assert.Equal("column2", me.Member.Lexeme);
                            });
                            AssertIs<MemberAccessExpression>(inner.From!.Source, ts =>
                            {
                                Assert.Equal("table2", ts.Member.Lexeme);
                            });
                        });
                    });
                });
                
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
                AssertIs<AliasExpression>(c.From!.Source, alias =>
                {
                    Assert.Equal("t1", alias.Alias!.Lexeme);
                    AssertIs<SubQuery>(alias.Expression, sq =>
                    {
                        AssertIs<SelectStatement>(sq.Query, inner =>
                        {
                            Assert.Single(inner.Values);
                            AssertIs<AliasExpression>(inner.Values[0], alias =>
                            {
                                AssertIs<MemberAccessExpression>(alias.Expression, me =>
                                {
                                    Assert.Equal("column2", me.Member.Lexeme);
                                });
                            });
                            AssertIs<MemberAccessExpression>(inner.From!.Source, ts =>
                            {
                                Assert.NotNull(ts.Member);
                                Assert.Equal("table2", ts.Member.Lexeme);
                            });
                        });
                    });
                    
                });
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
                AssertIs<MethodCallExpression>(c.From!.Source, mce =>
                {
                    Assert.Equal("GetItems", mce.MethodName.Lexeme);
                    Assert.Empty(mce.Arguments);
                });
            });
        }

        [Fact]
        public void Parse_TableValuedFunction_With_Arguments_Select()
        {
            List<SyntaxToken> tokens = [
                new(TokenType.Select, "select", 0, null),
                new(TokenType.Identifier, "column1", 7, null),
                new(TokenType.From, "from", 15, null),
                new(TokenType.Identifier, "GetItems", 20, null),
                new(TokenType.LeftParen, "(", 28, null),
                new(TokenType.IntValue, "100", 29, 100),
                new(TokenType.RightParen, ")", 32, null),
                new(TokenType.EOF, "", 34, null)
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
                AssertIs<MethodCallExpression>(c.From!.Source, mce =>
                {
                    Assert.Equal("GetItems", mce.MethodName.Lexeme);
                    Assert.Single(mce.Arguments);
                    AssertIs<LiteralExpression>(mce.Arguments[0], le =>
                    {
                        Assert.Equal(100, le.LiteralToken.Literal);
                    });
                });
            });
        }

        //joins

        [Fact]
        public void Parse_SelectFromMultipleTablesWithJoin()
        {
            var query = "select u.id, u.name, count(o.id) as order_ct from users u join orders o on u.id = o.user_id";
            var tokens = new Scanner(query).GetTokens();
            var parser = new Parser(tokens);
            var stmt = parser.Parse();
            Assert.NotNull(stmt);
            AssertIs<SelectStatement>(stmt, c =>
            {
                Assert.Equal(3,c.Values.Count);
                AssertIs<MemberAccessExpression>(c.Values[0], me =>
                {
                    Assert.Equal("u", me.Object?.Lexeme);
                    Assert.Equal("id", me.Member.Lexeme);
                });
                AssertIs<MemberAccessExpression>(c.Values[1], me =>
                {
                    Assert.Equal("u", me.Object?.Lexeme);
                    Assert.Equal("name", me.Member.Lexeme);
                });
                AssertAliasOf<MethodCallExpression>(c.Values[2], "order_ct", me =>
                {
                    Assert.Equal("count", me.MethodName.Lexeme);
                    Assert.Single(me.Arguments);
                    AssertIs<MemberAccessExpression>(me.Arguments[0], arg =>
                    {
                        Assert.Equal("o", arg.Object?.Lexeme);
                        Assert.Equal("id", arg.Member.Lexeme);
                    });
                });

                AssertIs<JoinExpression>(c.From!.Source, je =>
                {
                    Assert.Equal(JoinType.Inner, je.JoinType);
                    //left
                    AssertIs<AliasExpression>(je.Left, left =>
                    {
                        Assert.Equal("u", left.Alias?.Lexeme);
                        AssertIs<MemberAccessExpression>(left.Expression, t =>
                        {
                            Assert.Equal("users", t.Member.Lexeme);
                        });
                    });
                    //right
                    AssertIs<AliasExpression>(je.Right, right =>
                    {
                        Assert.Equal("o", right.Alias.Lexeme);
                        AssertIs<MemberAccessExpression>(right.Expression, m =>
                        {
                            Assert.Equal("orders", m.Member.Lexeme);
                        });
                    });
                    //condition
                    AssertIs<BinaryExpression>(je.Condition, binary =>
                    {
                        Assert.Equal(TokenType.Equal, binary.OperatorToken.Type);
                        AssertIs<MemberAccessExpression>(binary.Left, left =>
                        {
                            Assert.Equal("u", left.Object?.Lexeme);
                            Assert.Equal("id", left.Member.Lexeme);
                        });
                        AssertIs<MemberAccessExpression>(binary.Right, right =>
                        {
                            Assert.Equal("o", right.Object?.Lexeme);
                            Assert.Equal("user_id", right.Member.Lexeme);
                        });
                    });
                });
            });
        }

        [Fact]
        public void Parse_SelectFromMultipleTablesWithCommaJoin()
        {
            var query = "select u.id, u.name, count(o.id) as order_ct from users u, orders o on u.id = o.user_id";
            var tokens = new Scanner(query).GetTokens();
            var parser = new Parser(tokens);
            var stmt = parser.Parse();
            Assert.NotNull(stmt);
            AssertIs<SelectStatement>(stmt, c =>
            {
                Assert.Equal(3, c.Values.Count);
                AssertIs<MemberAccessExpression>(c.Values[0], me =>
                {
                    Assert.Equal("u", me.Object?.Lexeme);
                    Assert.Equal("id", me.Member.Lexeme);
                });
                AssertIs<MemberAccessExpression>(c.Values[1], me =>
                {
                    Assert.Equal("u", me.Object?.Lexeme);
                    Assert.Equal("name", me.Member.Lexeme);
                });
                AssertAliasOf<MethodCallExpression>(c.Values[2], "order_ct", me =>
                {
                    Assert.Equal("count", me.MethodName.Lexeme);
                    Assert.Single(me.Arguments);
                    AssertIs<MemberAccessExpression>(me.Arguments[0], arg =>
                    {
                        Assert.Equal("o", arg.Object?.Lexeme);
                        Assert.Equal("id", arg.Member.Lexeme);
                    });
                });

                AssertIs<JoinExpression>(c.From!.Source, je =>
                {
                    Assert.Equal(JoinType.Inner, je.JoinType);
                    //left
                    AssertIs<AliasExpression>(je.Left, left =>
                    {
                        Assert.Equal("u", left.Alias?.Lexeme);
                        AssertIs<MemberAccessExpression>(left.Expression, t =>
                        {
                            Assert.Equal("users", t.Member.Lexeme);
                        });
                    });
                    //right
                    AssertIs<AliasExpression>(je.Right, right =>
                    {
                        Assert.Equal("o", right.Alias.Lexeme);
                        AssertIs<MemberAccessExpression>(right.Expression, m =>
                        {
                            Assert.Equal("orders", m.Member.Lexeme);
                        });
                    });
                    //condition
                    AssertIs<BinaryExpression>(je.Condition, binary =>
                    {
                        Assert.Equal(TokenType.Equal, binary.OperatorToken.Type);
                        AssertIs<MemberAccessExpression>(binary.Left, left =>
                        {
                            Assert.Equal("u", left.Object?.Lexeme);
                            Assert.Equal("id", left.Member.Lexeme);
                        });
                        AssertIs<MemberAccessExpression>(binary.Right, right =>
                        {
                            Assert.Equal("o", right.Object?.Lexeme);
                            Assert.Equal("user_id", right.Member.Lexeme);
                        });
                    });
                });
            });
        }

        [Fact]
        public void Parse_SelectFromMultipleTablesWithInnerJoin()
        {
            var query = "select u.id, u.name, count(o.id) as order_ct from users u inner join orders o on u.id = o.user_id";
            var tokens = new Scanner(query).GetTokens();
            var parser = new Parser(tokens);
            var stmt = parser.Parse();
            Assert.NotNull(stmt);
            AssertIs<SelectStatement>(stmt, c =>
            {
                Assert.Equal(3, c.Values.Count);
                AssertIs<MemberAccessExpression>(c.Values[0], me =>
                {
                    Assert.Equal("u", me.Object?.Lexeme);
                    Assert.Equal("id", me.Member.Lexeme);
                });
                AssertIs<MemberAccessExpression>(c.Values[1], me =>
                {
                    Assert.Equal("u", me.Object?.Lexeme);
                    Assert.Equal("name", me.Member.Lexeme);
                });
                AssertAliasOf<MethodCallExpression>(c.Values[2], "order_ct", me =>
                {
                    Assert.Equal("count", me.MethodName.Lexeme);
                    Assert.Single(me.Arguments);
                    AssertIs<MemberAccessExpression>(me.Arguments[0], arg =>
                    {
                        Assert.Equal("o", arg.Object?.Lexeme);
                        Assert.Equal("id", arg.Member.Lexeme);
                    });
                });

                AssertIs<JoinExpression>(c.From!.Source, je =>
                {
                    Assert.Equal(JoinType.Inner, je.JoinType);
                    //left
                    AssertIs<AliasExpression>(je.Left, left =>
                    {
                        Assert.Equal("u", left.Alias?.Lexeme);
                        AssertIs<MemberAccessExpression>(left.Expression, t =>
                        {
                            Assert.Equal("users", t.Member.Lexeme);
                        });
                    });
                    //right
                    AssertIs<AliasExpression>(je.Right, right =>
                    {
                        Assert.Equal("o", right.Alias.Lexeme);
                        AssertIs<MemberAccessExpression>(right.Expression, m =>
                        {
                            Assert.Equal("orders", m.Member.Lexeme);
                        });
                    });
                    //condition
                    AssertIs<BinaryExpression>(je.Condition, binary =>
                    {
                        Assert.Equal(TokenType.Equal, binary.OperatorToken.Type);
                        AssertIs<MemberAccessExpression>(binary.Left, left =>
                        {
                            Assert.Equal("u", left.Object?.Lexeme);
                            Assert.Equal("id", left.Member.Lexeme);
                        });
                        AssertIs<MemberAccessExpression>(binary.Right, right =>
                        {
                            Assert.Equal("o", right.Object?.Lexeme);
                            Assert.Equal("user_id", right.Member.Lexeme);
                        });
                    });
                });
            });
        }

        [Fact]
        public void Parse_SelectFromMultipleTablesWithOuterJoin()
        {
            var query = "select u.id, u.name, count(o.id) as order_ct from users u outer join orders o on u.id = o.user_id";
            var tokens = new Scanner(query).GetTokens();
            var parser = new Parser(tokens);
            var stmt = parser.Parse();
            Assert.NotNull(stmt);
            AssertIs<SelectStatement>(stmt, c =>
            {
                Assert.Equal(3, c.Values.Count);
                AssertIs<MemberAccessExpression>(c.Values[0], me =>
                {
                    Assert.Equal("u", me.Object?.Lexeme);
                    Assert.Equal("id", me.Member.Lexeme);
                });
                AssertIs<MemberAccessExpression>(c.Values[1], me =>
                {
                    Assert.Equal("u", me.Object?.Lexeme);
                    Assert.Equal("name", me.Member.Lexeme);
                });
                AssertAliasOf<MethodCallExpression>(c.Values[2], "order_ct", me =>
                {
                    Assert.Equal("count", me.MethodName.Lexeme);
                    Assert.Single(me.Arguments);
                    AssertIs<MemberAccessExpression>(me.Arguments[0], arg =>
                    {
                        Assert.Equal("o", arg.Object?.Lexeme);
                        Assert.Equal("id", arg.Member.Lexeme);
                    });
                });

                AssertIs<JoinExpression>(c.From!.Source, je =>
                {
                    Assert.Equal(JoinType.Outer, je.JoinType);
                    //left
                    AssertIs<AliasExpression>(je.Left, left =>
                    {
                        Assert.Equal("u", left.Alias?.Lexeme);
                        AssertIs<MemberAccessExpression>(left.Expression, t =>
                        {
                            Assert.Equal("users", t.Member.Lexeme);
                        });
                    });
                    //right
                    AssertIs<AliasExpression>(je.Right, right =>
                    {
                        Assert.Equal("o", right.Alias.Lexeme);
                        AssertIs<MemberAccessExpression>(right.Expression, m =>
                        {
                            Assert.Equal("orders", m.Member.Lexeme);
                        });
                    });
                    //condition
                    AssertIs<BinaryExpression>(je.Condition, binary =>
                    {
                        Assert.Equal(TokenType.Equal, binary.OperatorToken.Type);
                        AssertIs<MemberAccessExpression>(binary.Left, left =>
                        {
                            Assert.Equal("u", left.Object?.Lexeme);
                            Assert.Equal("id", left.Member.Lexeme);
                        });
                        AssertIs<MemberAccessExpression>(binary.Right, right =>
                        {
                            Assert.Equal("o", right.Object?.Lexeme);
                            Assert.Equal("user_id", right.Member.Lexeme);
                        });
                    });
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
