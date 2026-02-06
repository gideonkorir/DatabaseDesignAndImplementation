using SimpleDb.Parsing;

namespace SimpleDb.UnitTests.Parsing;

public class ScannerTests
{
    [Theory]
    [InlineData("select", TokenType.Select)]
    [InlineData("SELECT", TokenType.Select)]
    [InlineData("update", TokenType.Update)]
    [InlineData("UPDATE", TokenType.Update)]
    [InlineData("delete", TokenType.Delete)]
    [InlineData("DELETE", TokenType.Delete)]
    [InlineData("from", TokenType.From)]
    [InlineData("FROM", TokenType.From)]
    [InlineData("as", TokenType.As)]
    [InlineData("AS", TokenType.As)]
    [InlineData("and", TokenType.And)]
    [InlineData("AND", TokenType.And)]
    [InlineData("or", TokenType.Or)]
    [InlineData("OR", TokenType.Or)]
    public void Scanner_Recognizes_Keywords_CaseInsensitive(string keyword, TokenType expectedType)
    {
        // Arrange
        var scanner = new Scanner(keyword);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Equal(2, tokens.Count);
        Assert.Equal(expectedType, tokens[0].Type);
        Assert.Equal(keyword, tokens[0].Lexeme);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
        Assert.Empty(scanner.SyntaxErrors);
    }

    [Theory]
    [InlineData("myTable", "myTable")]
    [InlineData("column1", "column1")]
    [InlineData("Table123", "Table123")]
    [InlineData("_invalidStart", "_invalidStart")] // Should trigger error
    public void Scanner_Recognizes_Identifiers(string identifier, string expectedLexeme)
    {
        // Arrange
        var scanner = new Scanner(identifier);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        if (identifier.StartsWith('_'))
        {
            Assert.NotEmpty(scanner.SyntaxErrors);
        }
        else
        {
            Assert.Equal(2, tokens.Count);
            Assert.Equal(TokenType.Identifier, tokens[0].Type);
            Assert.Equal(expectedLexeme, tokens[0].Lexeme);
            Assert.Equal(expectedLexeme, tokens[0].Literal);
            Assert.Equal(TokenType.EOF, tokens[^1].Type);
        }
    }

    [Theory]
    [InlineData("0", 0)]
    [InlineData("123", 123)]
    [InlineData("456789", 456789)]
    [InlineData("2147483647", int.MaxValue)]
    public void Scanner_Recognizes_IntegerValues(string number, int expectedValue)
    {
        // Arrange
        var scanner = new Scanner(number);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Equal(2, tokens.Count);
        Assert.Equal(TokenType.IntValue, tokens[0].Type);
        Assert.Equal(expectedValue, tokens[0].Literal);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
    }

    [Theory]
    [InlineData("'hello'", "hello")]
    [InlineData("'world'", "world")]
    [InlineData("''", "")]
    [InlineData("'it''s escaped'", "it's escaped")]
    [InlineData("'double''quote''test'", "double'quote'test")]
    public void Scanner_Recognizes_StringValues(string input, string expectedValue)
    {
        // Arrange
        var scanner = new Scanner(input);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Equal(2, tokens.Count);
        Assert.Equal(TokenType.StringValue, tokens[0].Type);
        Assert.Equal(expectedValue, tokens[0].Literal);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
    }

    [Theory]
    [InlineData("(", TokenType.LeftParen)]
    [InlineData(")", TokenType.RightParen)]
    [InlineData("+", TokenType.Plus)]
    [InlineData("-", TokenType.Minus)]
    [InlineData("*", TokenType.Star)]
    [InlineData("/", TokenType.ForwardSlash)]
    [InlineData("%", TokenType.Percent)]
    [InlineData("!", TokenType.Bang)]
    [InlineData("=", TokenType.Equal)]
    [InlineData("<", TokenType.LessThan)]
    [InlineData(">", TokenType.GreaterThan)]
    public void Scanner_Recognizes_SingleCharacterOperators(string op, TokenType expectedType)
    {
        // Arrange
        var scanner = new Scanner(op);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Equal(2, tokens.Count);
        Assert.Equal(expectedType, tokens[0].Type);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
        Assert.Equal(op, tokens[0].Lexeme);
    }

    [Theory]
    [InlineData("!=", TokenType.BangEqual)]
    [InlineData("<=", TokenType.LessThanEqual)]
    [InlineData(">=", TokenType.GreaterThanEqual)]
    public void Scanner_Recognizes_TwoCharacterOperators(string op, TokenType expectedType)
    {
        // Arrange
        var scanner = new Scanner(op);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Equal(2, tokens.Count);
        Assert.Equal(expectedType, tokens[0].Type);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
        Assert.Equal(op, tokens[0].Lexeme);
    }

    [Fact]
    public void Scanner_Tokenizes_SimpleSelectQuery()
    {
        // Arrange
        var query = "select id from users";
        var scanner = new Scanner(query);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Equal(5, tokens.Count);
        Assert.Equal(TokenType.Select, tokens[0].Type);
        Assert.Equal(TokenType.Identifier, tokens[1].Type);
        Assert.Equal("id", tokens[1].Literal);
        Assert.Equal(TokenType.From, tokens[2].Type);
        Assert.Equal(TokenType.Identifier, tokens[3].Type);
        Assert.Equal("users", tokens[3].Literal);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
        Assert.Empty(scanner.SyntaxErrors);
    }

    [Fact]
    public void Scanner_Tokenizes_ComplexExpression()
    {
        // Arrange
        var query = "select id + 10 * 2 from users";
        var scanner = new Scanner(query);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Equal(9, tokens.Count);
        Assert.Equal(TokenType.Select, tokens[0].Type);
        Assert.Equal(TokenType.Identifier, tokens[1].Type);
        Assert.Equal(TokenType.Plus, tokens[2].Type);
        Assert.Equal(TokenType.IntValue, tokens[3].Type);
        Assert.Equal(10, tokens[3].Literal);
        Assert.Equal(TokenType.Star, tokens[4].Type);
        Assert.Equal(TokenType.IntValue, tokens[5].Type);
        Assert.Equal(2, tokens[5].Literal);
        Assert.Equal(TokenType.From, tokens[6].Type);
        Assert.Equal(TokenType.Identifier, tokens[7].Type);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
    }

    [Fact]
    public void Scanner_Tokenizes_QueryWithParentheses()
    {
        // Arrange
        var query = "select (id + 10) from users";
        var scanner = new Scanner(query);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Equal(9, tokens.Count);
        Assert.Equal(TokenType.Select, tokens[0].Type);
        Assert.Equal(TokenType.LeftParen, tokens[1].Type);
        Assert.Equal(TokenType.Identifier, tokens[2].Type);
        Assert.Equal(TokenType.Plus, tokens[3].Type);
        Assert.Equal(TokenType.IntValue, tokens[4].Type);
        Assert.Equal(TokenType.RightParen, tokens[5].Type);
        Assert.Equal(TokenType.From, tokens[6].Type);
        Assert.Equal(TokenType.Identifier, tokens[7].Type);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
    }

    [Fact]
    public void Scanner_Tokenizes_QueryWithStringLiterals()
    {
        // Arrange
        var query = "select name from users";
        var scanner = new Scanner(query);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Equal(5, tokens.Count);
        Assert.Equal(TokenType.Select, tokens[0].Type);
        Assert.Equal(TokenType.Identifier, tokens[1].Type);
        Assert.Equal(TokenType.From, tokens[2].Type);
        Assert.Equal(TokenType.Identifier, tokens[3].Type);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
    }

    [Fact]
    public void Scanner_Tokenizes_QueryWithComparisonOperators()
    {
        // Arrange
        var query = "id = 10 and age >= 18 or status != 'active'";
        var scanner = new Scanner(query);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Equal(12, tokens.Count);
        Assert.Equal(TokenType.Identifier, tokens[0].Type);
        Assert.Equal(TokenType.Equal, tokens[1].Type);
        Assert.Equal(TokenType.IntValue, tokens[2].Type);
        Assert.Equal(TokenType.And, tokens[3].Type);
        Assert.Equal(TokenType.Identifier, tokens[4].Type);
        Assert.Equal(TokenType.GreaterThanEqual, tokens[5].Type);
        Assert.Equal(TokenType.IntValue, tokens[6].Type);
        Assert.Equal(TokenType.Or, tokens[7].Type);
        Assert.Equal(TokenType.Identifier, tokens[8].Type);
        Assert.Equal(TokenType.BangEqual, tokens[9].Type);
        Assert.Equal(TokenType.StringValue, tokens[10].Type);
        Assert.Equal("active", tokens[10].Literal);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
    }

    [Fact]
    public void Scanner_IgnoresWhitespace()
    {
        // Arrange
        var query = "  select   id  \t from  \r users  ";
        var scanner = new Scanner(query);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Equal(5, tokens.Count);
        Assert.Equal(TokenType.Select, tokens[0].Type);
        Assert.Equal(TokenType.Identifier, tokens[1].Type);
        Assert.Equal(TokenType.From, tokens[2].Type);
        Assert.Equal(TokenType.Identifier, tokens[3].Type);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
    }

    [Fact]
    public void Scanner_ReportsErrorForUnexpectedCharacter()
    {
        // Arrange
        var query = "select @ from users";
        var scanner = new Scanner(query);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Single(scanner.SyntaxErrors);
        Assert.Contains("Unexpected char: '@'", scanner.SyntaxErrors[0].Message);
    }

    [Fact]
    public void Scanner_HandlesEmptyInput()
    {
        // Arrange
        var scanner = new Scanner("");

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Single(tokens);
        Assert.Empty(scanner.SyntaxErrors);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
    }

    [Fact]
    public void Scanner_HandlesOnlyWhitespace()
    {
        // Arrange
        var scanner = new Scanner("   \t  \r  ");

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Single(tokens);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
        Assert.Empty(scanner.SyntaxErrors);
    }

    [Fact]
    public void Scanner_TokenizesUpdateQuery()
    {
        // Arrange
        var query = "update users";
        var scanner = new Scanner(query);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Equal(3, tokens.Count);
        Assert.Equal(TokenType.Update, tokens[0].Type);
        Assert.Equal(TokenType.Identifier, tokens[1].Type);
        Assert.Equal("users", tokens[1].Literal);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
    }

    [Fact]
    public void Scanner_TokenizesDeleteQuery()
    {
        // Arrange
        var query = "delete from users";
        var scanner = new Scanner(query);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Equal(4, tokens.Count);
        Assert.Equal(TokenType.Delete, tokens[0].Type);
        Assert.Equal(TokenType.From, tokens[1].Type);
        Assert.Equal(TokenType.Identifier, tokens[2].Type);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
    }

    [Fact]
    public void Scanner_TokenizesAllArithmeticOperators()
    {
        // Arrange
        var query = "5 + 3 - 2 * 10 / 2 % 3";
        var scanner = new Scanner(query);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Equal(12, tokens.Count);
        Assert.Equal(TokenType.IntValue, tokens[0].Type);
        Assert.Equal(TokenType.Plus, tokens[1].Type);
        Assert.Equal(TokenType.IntValue, tokens[2].Type);
        Assert.Equal(TokenType.Minus, tokens[3].Type);
        Assert.Equal(TokenType.IntValue, tokens[4].Type);
        Assert.Equal(TokenType.Star, tokens[5].Type);
        Assert.Equal(TokenType.IntValue, tokens[6].Type);
        Assert.Equal(TokenType.ForwardSlash, tokens[7].Type);
        Assert.Equal(TokenType.IntValue, tokens[8].Type);
        Assert.Equal(TokenType.Percent, tokens[9].Type);
        Assert.Equal(TokenType.IntValue, tokens[10].Type);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
    }

    [Fact]
    public void Scanner_SetsCorrectStartPositionForTokens()
    {
        // Arrange
        var query = "select id";
        var scanner = new Scanner(query);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Equal(3, tokens.Count);
        Assert.Equal(0, tokens[0].Start);
        Assert.Equal(7, tokens[1].Start);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
    }

    [Fact]
    public void Scanner_HandlesUnterminatedString()
    {
        // Arrange
        var query = "'unterminated";
        var scanner = new Scanner(query);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Equal(2, tokens.Count);
        Assert.Equal(TokenType.StringValue, tokens[0].Type);
        Assert.Equal("unterminated", tokens[0].Literal);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
    }

    [Fact]
    public void Scanner_HandlesConsecutiveOperators()
    {
        // Arrange
        var query = "!= <= >=";
        var scanner = new Scanner(query);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Equal(4, tokens.Count);
        Assert.Equal(TokenType.BangEqual, tokens[0].Type);
        Assert.Equal(TokenType.LessThanEqual, tokens[1].Type);
        Assert.Equal(TokenType.GreaterThanEqual, tokens[2].Type);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
    }

    [Fact]
    public void Scanner_HandlesAliasWithAS()
    {
        // Arrange
        var query = "select id as userId from users as u";
        var scanner = new Scanner(query);

        // Act
        var tokens = scanner.GetTokens();

        // Assert
        Assert.Equal(9, tokens.Count);
        Assert.Equal(TokenType.Select, tokens[0].Type);
        Assert.Equal(TokenType.Identifier, tokens[1].Type);
        Assert.Equal(TokenType.As, tokens[2].Type);
        Assert.Equal(TokenType.Identifier, tokens[3].Type);
        Assert.Equal(TokenType.From, tokens[4].Type);
        Assert.Equal(TokenType.Identifier, tokens[5].Type);
        Assert.Equal(TokenType.As, tokens[6].Type);
        Assert.Equal(TokenType.Identifier, tokens[7].Type);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
    }

    [Theory]
    [InlineData("select id, name, age from users", 9)]
    public void Scanner_QueryWithMultipleColumns(string query, int token_count)
    {
        // Arrange
        var scanner = new Scanner(query);
        // Act
        var tokens = scanner.GetTokens();
        // Assert
        Assert.Equal(token_count, tokens.Count);
        Assert.Equal(TokenType.Select, tokens[0].Type);
        Assert.Equal(TokenType.Identifier, tokens[1].Type);
        Assert.Equal(TokenType.Comma, tokens[2].Type);
        Assert.Equal(TokenType.Identifier, tokens[3].Type);
        Assert.Equal(TokenType.Comma, tokens[4].Type);
        Assert.Equal(TokenType.Identifier, tokens[5].Type);
        Assert.Equal(TokenType.From, tokens[6].Type);
        Assert.Equal(TokenType.Identifier, tokens[7].Type);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
    }

    [Fact]
    public void Scanner_QueryWithMultipleColumnsAndAlias()
    {
        // Arrange
        var query = "select id, name as full_name, age from users as u";
        var scanner = new Scanner(query);
        // Act
        var tokens = scanner.GetTokens();
        // Assert
        Assert.Equal(13, tokens.Count);
        Assert.Equal(TokenType.Select, tokens[0].Type);
        Assert.Equal(TokenType.Identifier, tokens[1].Type);
        Assert.Equal(TokenType.Comma, tokens[2].Type);
        Assert.Equal(TokenType.Identifier, tokens[3].Type);
        Assert.Equal(TokenType.As, tokens[4].Type);
        Assert.Equal(TokenType.Identifier, tokens[5].Type);
        Assert.Equal(TokenType.Comma, tokens[6].Type);
        Assert.Equal(TokenType.Identifier, tokens[7].Type);
        Assert.Equal(TokenType.From, tokens[8].Type);
        Assert.Equal(TokenType.Identifier, tokens[9].Type);
        Assert.Equal(TokenType.As, tokens[10].Type);
        Assert.Equal(TokenType.Identifier, tokens[11].Type);
        Assert.Equal(TokenType.EOF, tokens[^1].Type);
    }
}