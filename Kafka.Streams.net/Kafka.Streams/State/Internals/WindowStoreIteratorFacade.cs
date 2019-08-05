using Kafka.Streams.State;

namespace Kafka.Streams.State.Internals
{
    public class WindowStoreIteratorFacade<V> : WindowStoreIterator<V>
    {
        KeyValueIterator<long, ValueAndTimestamp<V>> innerIterator;

        WindowStoreIteratorFacade(KeyValueIterator<long, ValueAndTimestamp<V>> iterator)
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
            return innerIterator.hasNext();
        }


        public KeyValue<long, V> next()
        {
            KeyValue<long, ValueAndTimestamp<V>> innerKeyValue = innerIterator.next();
            return KeyValue.pair(innerKeyValue.key, getValueOrNull(innerKeyValue.value));
        }
    }
}