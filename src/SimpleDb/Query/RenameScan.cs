using SimpleDb.Record;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace SimpleDb.Query
{
    public record struct Rename(string From, string To);
    public class RenameScan(IScan scan, Rename[] renames) : IScan
    {
        private bool _disposed = false;

        private Schema? _schema;
        public Schema Schema
        {
            get
            {
                if (_schema is null)
                {
                    var s = new Schema();
                    foreach (var f in scan.Schema)
                    {
                        string renamed = GetNewName(f.Name);
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
        {
            if (!_disposed)
            {
                scan.Dispose();
                _disposed = true;
            }
            GC.SuppressFinalize(this);
        }

        public int GetInt32(string fieldName)
            => scan.GetInt32(GetOriginalName(fieldName));

        public string GetString(string fieldName)
            => scan.GetString(GetOriginalName(fieldName));

        public Constant GetValue(string fieldName)
            => scan.GetValue(GetOriginalName(fieldName));

        public bool Next()
            => scan.Next();

        public bool TryGetInt32(string fieldName, out int value)
            => scan.TryGetInt32(GetOriginalName(fieldName), out value);

        public bool TryGetString(string fieldName, [NotNullWhen(true)] out string? value)
            => scan.TryGetString(GetOriginalName(fieldName), out value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private string GetOriginalName(string fieldName)
        {
            foreach(var (from, to) in renames)
            {
                if(string.Equals(to, fieldName, StringComparison.OrdinalIgnoreCase))
                {
                    return from;
                }
            }
            return fieldName;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private string GetNewName(string fieldName)
        {
            foreach (var (from, to) in renames)
            {
                if (string.Equals(from, fieldName, StringComparison.OrdinalIgnoreCase))
                {
                    return to;
                }
            }
            return fieldName;
        }
    }
}