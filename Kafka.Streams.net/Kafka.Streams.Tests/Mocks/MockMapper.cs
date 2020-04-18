using Kafka.Streams.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Mocks
{
    public static class MockMapper
    {
        public static KeyValueMapper<K, V, K> GetSelectKeyKeyValueMapper<K, V>()
            => (k, v) => k;

        public static KeyValueMapper<K, V, IEnumerable<KeyValuePair<K, V>>> GetNoOpFlatKeyValueMapper<K, V>()
            => (k, v) => new[] { KeyValuePair.Create(k, v) };

        public static KeyValueMapper<K, V, KeyValuePair<K, V>> GetNoOpKeyValueMapper<K, V>()
            => (k, v) => KeyValuePair.Create(k, v);

        public static KeyValueMapper<K, V, KeyValuePair<V, V>> GetSelectValueKeyValueMapper<K, V>()
            => (k, v) => KeyValuePair.Create(v, v);

        public static KeyValueMapper<K, V, V> GetSelectValueMapper<K, V>()
            => (k, v) => v;

        public static ValueMapper<V, V> GetNoOpValueMapper<K, V>()
            => v => v;
    }
}
