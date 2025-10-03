using System.Runtime.CompilerServices;
using SimpleDb.Record;

namespace SimpleDb.Query
{
    public class SelectScan(IScan scan, Func<ScanRecord, bool> predicate) : IUpdateScan
    {
        private bool _disposed = false;
        private readonly IUpdateScan? _updateScan = scan as IUpdateScan;

        public RID RID
        {
            get
            {
                CheckCanUpdate();
                return _updateScan!.RID;
            }
        }

        public Schema Schema => scan.Schema;

        public void BeforeFirst() => scan.BeforeFirst();

        public int GetInt32(string fieldName) => scan.GetInt32(fieldName);

        public string GetString(string fieldName) => scan.GetString(fieldName);

        public Constant GetValue(string fieldName) => scan.GetValue(fieldName);

        public bool Next()
        {
            while (scan.Next())
            {
                if (predicate(new ScanRecord(scan)))
                    return true;
            }
            return false;
        }

        public bool TryGetInt32(string fieldName, out int value) => scan.TryGetInt32(fieldName, out value);

        public bool TryGetString(string fieldName, out string? value) => scan.TryGetString(fieldName, out value);

        public void Dispose()
        {
            if (!_disposed)
            {
                scan.Dispose();
                _disposed = true;
            }
            GC.SuppressFinalize(this);
        }

        public void SetValue(string fieldName, int value)
        {
            CheckCanUpdate();
            _updateScan!.SetValue(fieldName, value);
        }

        public void SetValue(string fieldName, string value)
        {
            CheckCanUpdate();
            _updateScan!.SetValue(fieldName, value);
        }

        public void SetValue(string fieldName, Constant value)
        {
            CheckCanUpdate();
            _updateScan!.SetValue(fieldName, value);        
        }

        public void Insert()
        {
            CheckCanUpdate();
            _updateScan!.Insert();
        }

        public void Delete()
        {
            CheckCanUpdate();
            _updateScan!.Delete();
        }

        public void MoveToRID(RID rid)
        {
            CheckCanUpdate();
            _updateScan!.MoveToRID(rid);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckCanUpdate()
        {
            if (_updateScan == null)
                throw new NotSupportedException("The underlying scan is not updatable");
        }
    }
}