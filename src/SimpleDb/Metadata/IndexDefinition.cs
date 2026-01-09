namespace SimpleDb.Metadata;

public record IndexDefinition(string IndexName, string IndexType, string TableName, string FieldName)
{
    public string FullName { get; } = $"{TableName}.{IndexName}";
}