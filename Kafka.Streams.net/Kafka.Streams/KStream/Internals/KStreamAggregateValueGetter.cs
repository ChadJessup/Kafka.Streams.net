using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamAggregateValueGetter<K, V> : IKTableValueGetter<K, V>
    {
        private ITimestampedKeyValueStore<K, V> store;

        public void init(IProcessorContext<K, V> context, string storeName)
        {
            store = (ITimestampedKeyValueStore<K, V>)context.getStateStore(storeName);
        }

        public ValueAndTimestamp<V> get(K key)
        {
            return store.get(key);
        }

        public void close() { }
    }
}