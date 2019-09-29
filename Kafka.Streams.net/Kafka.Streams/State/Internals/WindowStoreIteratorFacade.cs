using System.Collections;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;

namespace Kafka.Streams.State.Internals
{
    public class WindowStoreIteratorFacade<V> : IWindowStoreIterator<V>
    {
        IKeyValueIterator<long, ValueAndTimestamp<V>> innerIterator;

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

            return KeyValue<long, V>.Pair(innerKeyValue.Key, ValueAndTimestamp<V>.getValueOrNull(innerKeyValue.Value));
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