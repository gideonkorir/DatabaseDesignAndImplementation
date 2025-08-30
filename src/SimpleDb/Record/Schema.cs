namespace SimpleDb.Record
{
    public class Schema : IEnumerable<KeyValuePair<string, Schema.FieldInfo>>
    {
        private readonly Dictionary<string, FieldInfo> _fields = new(StringComparer.OrdinalIgnoreCase);

        public FieldInfo this[string field]
        {
            get
            {
                if(TryGetField(field, out var fieldInfo)) 
                    return fieldInfo;
                throw new KeyNotFoundException($"The field '{field}' was not found in the schema");
            }
        }
        public Schema AddField(string name, SchemaFieldType columnType, int length)
        {
            _fields.Add(name, new FieldInfo(columnType, length));
            return this;
        }

        public bool TryGetField(string field, out FieldInfo fieldInfo)
            => _fields.TryGetValue(field, out fieldInfo);

        public IEnumerator<KeyValuePair<string, FieldInfo>> GetEnumerator()
            => _fields.GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            => _fields.GetEnumerator();

        public record struct FieldInfo(SchemaFieldType FieldType, int Length);
    }
}
