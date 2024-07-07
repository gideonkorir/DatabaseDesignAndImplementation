namespace SimpleDb.BufferPool
{
    public enum BufferReplacementStrategy
    {
        Naive,
        LRU,
        LRM,
        Clock
    }
}
