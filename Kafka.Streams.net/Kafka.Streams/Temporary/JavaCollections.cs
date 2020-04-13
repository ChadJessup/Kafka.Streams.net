using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Confluent.Kafka;
using Kafka.Streams.Nodes;
using Kafka.Streams.State;

namespace Kafka.Streams.Temporary
{
    public static class Collections
    {
        public static List<T> singletonList<T>(T item)
            => new List<T> { item };

        public static HashSet<T> singleton<T>(T item)
            => new HashSet<T> { item };

        public static Dictionary<TKey, TValue> singletonMap<TKey, TValue>(TKey key, TValue value)
            => new Dictionary<TKey, TValue>(1)
            {
                { key, value },
            };

        public static void sort<T>(this List<T> list)
            => list.Sort();

        public static Dictionary<TKey, TValue> emptyMap<TKey, TValue>()
            => new Dictionary<TKey, TValue>();

        public static IEnumerable<T> emptyList<T>()
            => new List<T>();

        public static HashSet<T> emptySet<T>()
            => new HashSet<T>();
    }
}
