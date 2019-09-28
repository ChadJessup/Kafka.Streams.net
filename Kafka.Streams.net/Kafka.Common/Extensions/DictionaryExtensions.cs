using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Kafka.Common.Extensions
{
    public static class DictionaryExtensions
    {
        public static Dictionary<K, V> PutAll<K, V>(this Dictionary<K, V> original, Dictionary<K, V> keyValuePairs)
        {
            foreach (var kvp in keyValuePairs)
            {
                original.TryAdd(kvp.Key, kvp.Value);
            }

            return original;
        }

        public static Dictionary<K, V> RemoveAll<K, V>(this Dictionary<K, V> original, IEnumerable<K> keys)
        {
            foreach (var toRemoveKey in keys)
            {
                original.Remove(toRemoveKey);
            }

            return original;
        }
    }
}
