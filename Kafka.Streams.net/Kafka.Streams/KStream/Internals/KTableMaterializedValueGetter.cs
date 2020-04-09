using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableMaterializedValueGetter<K, V> : IKTableValueGetter<K, V>
    {
        private ITimestampedKeyValueStore<K, V> store;
        private readonly KafkaStreamsContext context;

        public KTableMaterializedValueGetter(KafkaStreamsContext context)
        {
            this.context = context;
        }

        public void Init(IProcessorContext context, string storeName)
        {
            store = (ITimestampedKeyValueStore<K, V>)context.GetStateStore(this.context, storeName);
        }

        public ValueAndTimestamp<V> Get(K key)
        {
            return store.Get(key);
        }

        public void Close() { }
    }
}
