using SimpleDb.Record;
using System.Reflection.Metadata;

namespace SimpleDb.Indexing
{
#pragma warning disable IDE1006 // Naming Styles
    public interface Index : IDisposable
#pragma warning restore IDE1006 // Naming Styles
    {
        /// <summary>
        /// Gets the RID of the record being pointed to at the current
        /// index position
        /// </summary>
        RID Current { get; }

        /// <summary>
        /// Positions the index before the 1st record having the specified key
        /// </summary>
        /// <param name="searchKey"></param>
        void BeforeFirst(Constant searchKey);

        /// <summary>
        /// Move the record to the next record having the specified search key
        /// </summary>
        /// <returns></returns>
        bool Next();

        /// <summary>
        /// Insert the index record having the specified data val and record id
        /// </summary>
        /// <param name="searchKey"></param>
        /// <param name="rid"></param>
        void Insert(Constant searchKey, RID rid);

        /// <summary>
        /// Deletes the index record having the specified dataval and RID values.
        /// </summary>
        /// <param name="searchKey"></param>
        /// <param name="rid"></param>
        void Delete(Constant searchKey, RID rid);
    }
}
