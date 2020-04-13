using System.Collections.Generic;
using System.Linq;

namespace System.Collections.Generic
{
    public static class DictionaryExtensions
    {
        public static void Put<TKey, TValue>(this Dictionary<TKey, TValue> dictionary, TKey key, TValue value)
            => dictionary.Add(key, value);
    }

    public static class EnumerableExtensions
    {
        public static bool IsEmpty<TSource>(this IEnumerable<TSource> collection)
            => !collection.Any();

        public static string ToJoinedString<T>(this IEnumerable<T> collection, char separator = ',')
           => string.Join(separator, collection ?? Enumerable.Empty<T>());
    }
}

namespace System
{
    public static class ArrayExtensions
    {
        public static List<T> AsList<T>(this Array _, params T[] items)
        {
            var list = new List<T>();

            list.AddRange(items);

            return list;
        }
    }
}
