using Kafka.Streams.IProcessor.Interfaces;
using Kafka.Streams.State;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamAggregateValueGetter<K, T> : IKTableValueGetter<K, T>
    {
        private ITimestampedKeyValueStore<K, T> store;

        public void init(IProcessorContext<K, T> context)
        {
            store = (ITimestampedKeyValueStore<K, T>)context.getStateStore(storeName);
        }


        public ValueAndTimestamp<T> get(K key)
        {
            return store.get(key);
        }


        public void close() { }
    }
}