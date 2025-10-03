namespace SimpleDb.Record
{
    public class Schema : IEnumerable<Schema.FieldInfo>, IEquatable<Schema>
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
            if (TryGetField(name, out _))
            {
                throw new SchemaException($"The field {name} already exists in the schema");
            }
            _fields.Add(name, new FieldInfo(name, _fields.Count, columnType, length));
            return this;
        }

        public Schema AddFieldAtOrdinal(string name, int ordinal, SchemaFieldType columnType, int length)
        {
            if (TryGetField(name, out _))
            {
                throw new SchemaException($"The field {name} already exists in the schema");
            }
            if(_fields.Any(c => c.Value.Ordinal == ordinal))
            {
                throw new SchemaException($"The ordinal {ordinal} is already in use");
            }
            _fields.Add(name, new FieldInfo(name, ordinal, columnType, length));
            return this;
        }

        public bool TryGetField(string field, out FieldInfo fieldInfo)
            => _fields.TryGetValue(field, out fieldInfo);

        public bool Equals(Schema? other)
        {
            if (other is null)
                return false;
            if (_fields.Count != other._fields.Count)
                return false;
            foreach (var field in _fields)
            {
                if (!other._fields.TryGetValue(field.Key, out var otherField))
                    return false;
                if (field.Value.Ordinal != otherField.Ordinal || field.Value.FieldType != otherField.FieldType || field.Value.Length != otherField.Length)
                    return false;
            }
            return true;
        }

        public override bool Equals(object? obj) => obj is Schema schema && Equals(schema);

        public override int GetHashCode()
        {
            int hash = 17;
            foreach (var field in _fields.OrderBy(c => c.Value.Ordinal))
            {
                hash = hash * 23 + field.Key.GetHashCode(StringComparison.OrdinalIgnoreCase);
                hash = hash * 23 + field.Value.Ordinal.GetHashCode();
                hash = hash * 23 + field.Value.FieldType.GetHashCode();
                hash = hash * 23 + field.Value.Length.GetHashCode();
            }
            return hash;
        }

        public IEnumerator<FieldInfo> GetEnumerator()
            => _fields.OrderBy(c => c.Value.Ordinal).Select(c => c.Value).GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            => _fields.GetEnumerator();

        public record struct FieldInfo(string Name, int Ordinal, SchemaFieldType FieldType, int Length);
    }
}
