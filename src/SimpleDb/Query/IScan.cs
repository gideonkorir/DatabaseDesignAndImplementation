namespace SimpleDb.Query
{
    public interface IScan : IDisposable
    {
        void BeforeFirst();

        bool Next();

        int GetInt32(string fieldName);

        string GetString(string fieldName);

        Constant GetValue(string fieldName);

        bool TryGetInt32(string fieldName, out int value);

        bool TryGetString(string fieldName, out string? value);
    }
}
