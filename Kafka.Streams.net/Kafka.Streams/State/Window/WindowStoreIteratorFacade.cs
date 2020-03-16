using System.Collections;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.State.Window
{
    public class WindowStoreIteratorFacade<V> : IWindowStoreIterator<V>
    {
        readonly IKeyValueIterator<long, ValueAndTimestamp<V>> innerIterator;

        public KeyValue<long, V> Current { get; }
        object IEnumerator.Current { get; }

        public WindowStoreIteratorFacade(IKeyValueIterator<long, ValueAndTimestamp<V>> iterator)
        {
            innerIterator = iterator;
        }

        public void close()
        {
            innerIterator.close();
        }
        public long peekNextKey()
        {
            return innerIterator.peekNextKey();
        }

        public bool hasNext()
        {
            return innerIterator.MoveNext();
        }

        public KeyValue<long, V> next()
        {
            KeyValue<long, ValueAndTimestamp<V>> innerKeyValue = innerIterator.Current;

            return KeyValue.Pair(innerKeyValue.Key, ValueAndTimestamp<V>.GetValueOrNull(innerKeyValue.Value));
        }

        public bool MoveNext()
        {
            throw new System.NotImplementedException();
        }

        public void Reset()
        {
            throw new System.NotImplementedException();
        }

        public void Dispose()
        {
            throw new System.NotImplementedException();
        }
    }
}