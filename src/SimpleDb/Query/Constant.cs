using SimpleDb.Record;

namespace SimpleDb.Query
{
    public readonly record struct Constant : IComparable<Constant>
    {
        public int? IntValue { get; }
        public string? StringValue { get; }

        public SchemaFieldType FieldType { get; }

        public Constant(int? value)
        {
            IntValue = value;
            FieldType = SchemaFieldType.I32;
        }

        public Constant(string? value)
        {
            StringValue = value;
            FieldType = SchemaFieldType.String;
        }

        public readonly int CompareTo(Constant other)
        {
            if (!IntValue.HasValue && StringValue is null)
                return 0; //empty is equatable
            if (IntValue.HasValue && other.IntValue.HasValue)
                return IntValue.Value.CompareTo(IntValue.Value);
            else if(StringValue is not null)
                return StringValue.CompareTo(other.StringValue);
            return 0;
        }
    }
}
