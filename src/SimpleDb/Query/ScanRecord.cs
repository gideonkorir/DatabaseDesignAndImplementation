namespace SimpleDb.Query;

public readonly struct ScanRecord(IScan scan)
{
    public int GetInt32(string fieldName) => scan.GetInt32(fieldName);

    public string GetString(string fieldName) => scan.GetString(fieldName);

    public Constant GetValue(string fieldName) => scan.GetValue(fieldName);

    public bool TryGetInt32(string fieldName, out int value) => scan.TryGetInt32(fieldName, out value);

    public bool TryGetString(string fieldName, out string? value) => scan.TryGetString(fieldName, out value);
}