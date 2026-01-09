using SimpleDb.Record;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace SimpleDb.Query
{
    public class RenameScan(IScan scan, Dictionary<string, string> renames) : IScan
    {
        private readonly Dictionary<string, string> _reverseNames = renames.ToDictionary(c => c.Value, c => c.Key);

        private Schema? _schema;
        public Schema Schema
        {
            get
            {
                if(_schema is null)
                {
                    var s = new Schema();
                    foreach(var f in scan.Schema)
                    {
                        string renamed = renames.TryGetValue(f.Name, out var  renamedValue) ? renamedValue : f.Name;
                        s.AddFieldAtOrdinal(renamed, f.Ordinal, f.FieldType, f.Length);
                    }
                    _schema = s;
                }
                return _schema;
            }
        }

        public void BeforeFirst()
            => scan.BeforeFirst();

        public void Dispose()
            => scan.Dispose();

        public int GetInt32(string fieldName)
            => scan.GetInt32(GetUnderlyingName(fieldName));

        public string GetString(string fieldName)
            => scan.GetString(GetUnderlyingName(fieldName));

        public Constant GetValue(string fieldName)
            => scan.GetValue(GetUnderlyingName(fieldName));

        public bool Next()
            => scan.Next();

        public bool TryGetInt32(string fieldName, [NotNullWhen(true)] out int value)
            => scan.TryGetInt32(GetUnderlyingName(fieldName), out value);

        public bool TryGetString(string fieldName, [NotNullWhen(true)] out string? value)
            => scan.TryGetString(GetUnderlyingName(fieldName), out value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private string GetUnderlyingName(string fieldName)
            => _reverseNames.TryGetValue(fieldName, out var name) ? name : fieldName;
    }
}
