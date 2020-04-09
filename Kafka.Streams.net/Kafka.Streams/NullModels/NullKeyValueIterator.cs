using System;
using System.Collections;
using System.Collections.Generic;
using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Windowed;

namespace Kafka.Streams.NullModels
{
    internal class NullKeyValueIterator<K, V> :
        IKeyValueIterator<K, V>,
        IKeyValueIterator<Windowed<K>, V>
    {
        public KeyValuePair<K, V> Current { get; }
        object IEnumerator.Current { get; }
        KeyValuePair<Windowed<K>, V> IEnumerator<KeyValuePair<Windowed<K>, V>>.Current { get; }

        public void Close()
        {
        }

        public void Dispose()
        {
        }

        public bool MoveNext() => false;

        public K PeekNextKey() => default;

        public void Reset()
        {
        }

        Windowed<K> IKeyValueIterator<Windowed<K>, V>.PeekNextKey()
            => new NullWindowedKeyValueIterator<Windowed<K>, V>();

        private class NullWindowedKeyValueIterator<T, V> : Windowed<K>
        {
            public NullWindowedKeyValueIterator()
                : base(default, new NullWindow())
            {
            }
        }
    }
}
