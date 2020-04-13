using System;
using System.Collections.Generic;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.Windowed;

namespace Kafka.Streams.State.KeyValues
{
    public class KeyValueIterators<K, V> : EmptyKeyValueIterator<K, V>, IWindowStoreIterator<V>
    {
        public static IKeyValueIterator<DateTime, V> EMPTY_ITERATOR { get; } = new EmptyKeyValueIterator<DateTime, V>();
        public static IWindowStoreIterator<V> EMPTY_WINDOW_STORE_ITERATOR { get; } = new KeyValueIterators<K, V>();

        KeyValuePair<DateTime, V> IEnumerator<KeyValuePair<DateTime, V>>.Current => KeyValueIterators<K, V>.EMPTY_WINDOW_STORE_ITERATOR.Current;

        DateTime IKeyValueIterator<DateTime, V>.PeekNextKey()
        {
            return KeyValueIterators<K, V>.EMPTY_WINDOW_STORE_ITERATOR.PeekNextKey();
        }
    }
}
