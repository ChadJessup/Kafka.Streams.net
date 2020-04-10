using System;
using System.Collections;
using System.Collections.Generic;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.State.Windowed
{
    public class WindowStoreIteratorFacade<V> : IWindowStoreIterator<V>
    {
        private readonly IKeyValueIterator<long, IValueAndTimestamp<V>> innerIterator;

        public KeyValuePair<long, V> Current { get; }
        object IEnumerator.Current { get; }

        public WindowStoreIteratorFacade(IKeyValueIterator<long, IValueAndTimestamp<V>> iterator)
        {
            this.innerIterator = iterator;
        }

        public void Close()
        {
            this.innerIterator.Close();
        }
        public long PeekNextKey()
        {
            return this.innerIterator.PeekNextKey();
        }

        public bool HasNext()
        {
            return this.innerIterator.MoveNext();
        }

        public KeyValuePair<long, V> Next()
        {
            KeyValuePair<long, IValueAndTimestamp<V>> innerKeyValue = this.innerIterator.Current;

            return KeyValuePair.Create(innerKeyValue.Key, ValueAndTimestamp.GetValueOrNull(innerKeyValue.Value));
        }

        public bool MoveNext()
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
        }

        public void Dispose()
        {
        }
    }
}
