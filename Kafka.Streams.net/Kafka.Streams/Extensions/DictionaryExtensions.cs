using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Kafka.Common.Extensions
{
    public static class DictionaryExtensions
    {
        public static IDictionary<K, V> PutAll<K, V>(this IDictionary<K, V> original, IDictionary<K, V> keyValuePairs)
        {
            foreach (var kvp in keyValuePairs)
            {
                original.TryAdd(kvp.Key, kvp.Value);
            }

            return original;
        }

        public static IDictionary<K, V> RemoveAll<K, V>(this IDictionary<K, V> original, IEnumerable<K> keys)
        {
            foreach (var toRemoveKey in keys)
            {
                original.Remove(toRemoveKey);
            }

            return original;
        }
    }
}
