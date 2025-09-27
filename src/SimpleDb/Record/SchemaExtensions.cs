namespace SimpleDb.Record
{
    public static class SchemaExtensions
    {
        public static Schema AddIntField(this Schema schema, string name) => schema.AddField(name, SchemaFieldType.I32, 4);

        public static Schema AddStringField(this Schema schema, string name, int length) => schema.AddField(name, SchemaFieldType.String, length);

        public static Schema CopyFrom(this Schema schema, Schema source)
        {
            ArgumentNullException.ThrowIfNull(schema);

            ArgumentNullException.ThrowIfNull(source);

            foreach(Schema.FieldInfo field in source)
            {
                schema.AddFieldAtOrdinal(field.Name, field.Ordinal, field.FieldType, field.Length);
            }
            return schema;
        }
    }
}
