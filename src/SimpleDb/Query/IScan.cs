using System.Diagnostics.CodeAnalysis;
using SimpleDb.Record;

namespace SimpleDb.Query
{
    public interface IScan : IDisposable
    {
        Schema Schema { get; }
        void BeforeFirst();

        bool Next();

        int GetInt32(string fieldName);

        string GetString(string fieldName);

        Constant GetValue(string fieldName);

        bool TryGetInt32(string fieldName, [NotNullWhen(true)] out int value);

        bool TryGetString(string fieldName, [NotNullWhen(true)] out string? value);
    }
}
