using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Mappers;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Tests.Mocks
{
    public class MockMapper
    {
        private class NoOpKeyValueMapper<K, V> : IKeyValueMapper<K, V, KeyValuePair<K, V>>
        {
            public KeyValuePair<K, V> Apply(K key, V value)
            {
                return KeyValuePair.Create(key, value);
            }
        }

        private class NoOpFlatKeyValueMapper<K, V> : IKeyValueMapper<K, V, IEnumerable<KeyValuePair<K, V>>>
        {
            public IEnumerable<KeyValuePair<K, V>> Apply(K key, V value)
            {
                return new[] { KeyValuePair.Create(key, value) };
            }
        }

        internal class SelectValueKeyValueMapper<K, V> : IKeyValueMapper<K, V, KeyValuePair<V, V>>
        {
            public KeyValuePair<V, V> Apply(K key, V value)
            {
                return KeyValuePair.Create(value, value);
            }
        }

        private class SelectValueMapper<K, V> : IKeyValueMapper<K, V, V>
        {
            public V Apply(K key, V value)
            {
                return value;
            }
        }

        private class SelectKeyMapper<K, V> : IKeyValueMapper<K, V, K>
        {
            public K Apply(K key, V value)
            {
                return key;
            }
        }

        private class NoOpValueMapper<V> : IValueMapper<V, V>
        {
            public V Apply(V value)
            {
                return value;
            }
        }

        public static IKeyValueMapper<K, V, K> SelectKeyKeyValueMapper<K, V>()
        {
            return new SelectKeyMapper<K, V>();
        }

        public static IKeyValueMapper<K, V, IEnumerable<KeyValuePair<K, V>>> noOpFlatKeyValueMapper<K, V>()
        {
            return new NoOpFlatKeyValueMapper<K, V>();
        }

        public static IKeyValueMapper<K, V, KeyValuePair<K, V>> noOpKeyValueMapper<K, V>()
        {
            return new NoOpKeyValueMapper<K, V>();
        }

        public static IKeyValueMapper<K, V, KeyValuePair<V, V>> selectValueKeyValueMapper<K, V>()
        {
            return new SelectValueKeyValueMapper<K, V>();
        }

        public static IKeyValueMapper<K, V, V> selectValueMapper<K, V>()
        {
            return new SelectValueMapper<K, V>();
        }

        public static IValueMapper<V, V> noOpValueMapper<K, V>()
        {
            return new NoOpValueMapper<V>();
        }
    }
}
