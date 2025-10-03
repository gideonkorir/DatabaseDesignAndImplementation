using SimpleDb.Record;

namespace SimpleDb.Query;

public class QueryPredicate
{
    private List<Term> _terms = new();

    private QueryPredicate()
    {
    }
    public QueryPredicate(Term terms)
    {
        _terms.Add(terms);
    }

    public void Include(QueryPredicate predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        _terms.AddRange(predicate._terms);
    }

    public bool IsSatisfied(ScanRecord scan)
    {
        foreach (var term in _terms)
        {
            if (!term.IsSatisfied(scan))
                return false;
        }
        return true;
    }

    public QueryPredicate? SelectSubPredicate(Schema schema)
    {
        var result = new QueryPredicate();
        foreach (var term in _terms)
        {
            if (term.AppliesTo(schema))
                result._terms.Add(term);
        }
        if (result._terms.Count == 0)
            return null!;
        return result;
    }

    public QueryPredicate? JoinSubPredicate(Schema leftSchema, Schema rightSchema)
    {
        var result = new QueryPredicate();
        Schema schema = new();
        schema.CopyFrom(leftSchema);
        schema.CopyFrom(rightSchema);

        foreach (var term in _terms)
        {
            if (term.AppliesTo(leftSchema) && term.AppliesTo(rightSchema))
                result._terms.Add(term);
        }
        if (result._terms.Count == 0)
            return null!;
        return result;
    }

    public Constant? GetEquatesWithConstantOrNull(string fieldName)
    {
        foreach (var term in _terms)
        {
            var constant = term.GetEquatesWithConstantOrNull(fieldName);
            if (constant is not null)
                return constant;
        }
        return null;
    }

    public string? GetEquatesWithFieldOrNull(string fieldName)
    {
        foreach (var term in _terms)
        {
            var field = term.GetEquatesWithFieldOrNull(fieldName);
            if (field is not null)
                return field;
        }
        return null;
    }

    public override string ToString() => _terms.Count == 1 ? _terms[0].ToString() : string.Join(" and ", _terms.Select(c => $"({c})"));
}