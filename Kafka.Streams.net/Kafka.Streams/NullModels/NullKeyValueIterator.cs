﻿using System;
using System.Collections;
using System.Collections.Generic;
using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Windowed;

namespace Kafka.Streams.NullModels
{
    internal class NullKeyValueIterator<K, V> :
        IKeyValueIterator<K, V>,
        IKeyValueIterator<IWindowed<K>, V>
    {
        public KeyValuePair<K, V> Current { get; }
        object IEnumerator.Current { get; }
        KeyValuePair<IWindowed<K>, V> IEnumerator<KeyValuePair<IWindowed<K>, V>>.Current { get; }

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

        IWindowed<K> IKeyValueIterator<IWindowed<K>, V>.PeekNextKey()
            => new NullWindowedKeyValueIterator<IWindowed<K>, V>();

        private class NullWindowedKeyValueIterator<T, V> : Windowed2<K>
        {
            public NullWindowedKeyValueIterator()
                : base(default, new NullWindow())
            {
            }
        }
    }
}
