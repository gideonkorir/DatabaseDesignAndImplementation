namespace SimpleDb.Tx.Concurrency
{
    public class AcquireLockFailedException(string message) : Exception(message)
    {
    }
}
