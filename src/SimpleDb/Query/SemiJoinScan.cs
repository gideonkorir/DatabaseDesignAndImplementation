using SimpleDb.Record;
using System.Diagnostics.CodeAnalysis;

namespace SimpleDb.Query
{
    public class SemiJoinScan(IScan left, IScan right, Func<ScanRecord, ScanRecord, bool> predicate) : IScan
    {
        public Schema Schema => left.Schema;

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
            return left.GetInt32(fieldName);
        }

        public string GetString(string fieldName)
        {
            return left.GetString(fieldName);
        }

        public Constant GetValue(string fieldName)
        {
            return left.GetValue(fieldName);
        }

        public bool Next()
        {
            if (!_leftHasValue)
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
            return left.TryGetInt32(fieldName, out value);
        }

        public bool TryGetString(string fieldName, [NotNullWhen(true)] out string? value)
        {
            return left.TryGetString(fieldName, out value);
        }
    }
}