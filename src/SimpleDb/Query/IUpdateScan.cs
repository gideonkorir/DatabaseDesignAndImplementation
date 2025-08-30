using SimpleDb.Record;

namespace SimpleDb.Query
{
    public interface IUpdateScan : IScan
    {
        RID RID { get; }
        void SetValue(string fieldName, int value);

        void SetValue(string fieldName, string value);

        void SetValue(string fieldName, Constant value);

        void Insert();

        void Delete();

        void MoveToRID(RID rid);
    }
}
