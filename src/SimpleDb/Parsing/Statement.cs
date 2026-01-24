namespace SimpleDb.Parsing
{
    public enum SyntaxKind
    {
        //select
        Select,
        Update,
        Delete,
        Literal,
        Binary,
        Where,
        From,

        //statements
        SelectStatement,
        UpdateStatement,
        DeleteStatement,
        SubQuery,

        //expresions
        Star,
        Unary,
        ParenthesizedExpr,
        Assignment,
        MemberAccess,
        Alias,
        MethodCall,

        //table source
        QuerySource
    }
    public abstract record class SyntaxNode
    {
        public abstract SyntaxKind Kind { get; }
    }
    public abstract record class Statement : SyntaxNode
    {
    }

    public record class QuerySource : Statement
    {
        public const int TableOrView = 0, SubQuery = 1, TVF = 2;

        override public SyntaxKind Kind => SyntaxKind.QuerySource;
        public QuerySource(SyntaxToken tableOrViewName, SyntaxToken? alias)
        {
            TableOrViewName = tableOrViewName;
            Alias = alias;
        }

        public QuerySource(MethodCallExpression tvf, SyntaxToken? alias)
        {
            TableValuedFunction = tvf;
            Alias = alias;
            SubType = TVF;
        }

        public QuerySource(SubQuery nestedQuery, SyntaxToken? alias)
        {
            NestedQuery = nestedQuery;
            Alias = alias;
            SubType = SubQuery;
        }

        public SyntaxToken? TableOrViewName { get; }

        public MethodCallExpression? TableValuedFunction { get; }

        public SubQuery? NestedQuery { get; }

        public SyntaxToken? Alias { get; }

        public int SubType { get; } // 0=TableOrView, 1=SubQuery, 2=TVF

        public static QuerySource FromOne(SyntaxToken? tableOrViewName, SubQuery? subQuery, MethodCallExpression? tvf, SyntaxToken? alias)
        {
            if (tableOrViewName is not null)
                return new QuerySource(tableOrViewName, alias);
            else if (subQuery is not null)
                return new QuerySource(subQuery, alias);
            else if (tvf is not null)
                return new QuerySource(tvf,  alias);
            else
                throw new ArgumentException("At least one of tableOrViewName, subQuery or tvf must be non-null");
        }
    }

}
