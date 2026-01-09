using SimpleDb.Record;
using System.Diagnostics.CodeAnalysis;

namespace SimpleDb.Query
{
    public class SemiJoinScan(IScan left, IScan right, Func<ScanRecord, ScanRecord, bool> predicate) : IScan
    {
        public Schema Schema => throw new NotImplementedException();

        private bool _leftHasValue;

        public void BeforeFirst()
        {
            left.BeforeFirst();
            right.BeforeFirst();
            _leftHasValue = left.Next();
        }

        public void Dispose()
        {
            left.Dispose(); right.Dispose();
        }

        public int GetInt32(string fieldName)
        {
            return left.TryGetInt32(fieldName, out var value) ? value : right.GetInt32(fieldName);
        }

        public string GetString(string fieldName)
        {
            return left.TryGetString(fieldName, out var value) ? value : right.GetString(fieldName);
        }

        public Constant GetValue(string fieldName)
        {
            if (left.Schema.TryGetField(fieldName, out _))
                return left.GetValue(fieldName);
            return right.GetValue(fieldName);
        }

        public bool Next()
        {
            if (_leftHasValue)
                return false;
            do
            {
                while (right.Next())
                {
                    if (predicate(new ScanRecord(left), new ScanRecord(right)))
                        return true;
                }
                //if we are here we need to prepare for the next scan
                right.BeforeFirst();
            }
            while (left.Next());
            return false;
        }

        public bool TryGetInt32(string fieldName, [NotNullWhen(true)] out int value)
        {
            return left.TryGetInt32(fieldName, out value) || right.TryGetInt32(fieldName, out value);
        }

        public bool TryGetString(string fieldName, [NotNullWhen(true)] out string? value)
        {
            return left.TryGetString(fieldName, out value) || right.TryGetString(fieldName, out value);
        }
    }
}