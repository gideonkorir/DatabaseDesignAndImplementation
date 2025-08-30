using SimpleDb.Files;

namespace SimpleDb.Record
{
    public class Layout
    {
        private readonly Dictionary<string, int> _offsets = [];
        public Schema Schema { get; }
        public int SlotSize { get; }

        public Layout(Schema schema)
        {
            Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            SlotSize = 4;
            int pos = SlotSize; //fields start after the slot size
            foreach(var (name, info) in schema)
            {
                _offsets.Add(name, pos);
                pos += LengthInBytes(info);
            }
            SlotSize = pos;
        }

        public Layout(Schema schema, Dictionary<string, int> offsets, int slotSize)
        {
            Schema = schema ?? throw new ArgumentNullException( nameof(schema));
            _offsets = offsets ?? throw new ArgumentNullException(nameof(offsets));
            SlotSize = slotSize;
        }

        public int GetOffset(string fieldName)
            => _offsets[fieldName];



        public static int LengthInBytes(Schema.FieldInfo fieldInfo)
                => fieldInfo.FieldType switch
                {
                    SchemaFieldType.I32 => 4,
                    SchemaFieldType.String => Page.MaxLength(fieldInfo.Length),
                    _ => throw new InvalidOperationException($"Unexpected FieldType: '{fieldInfo.FieldType}'")
                };
    }
}
