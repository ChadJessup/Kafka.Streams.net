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
            this.store = (ITimestampedKeyValueStore<K, V>)context.GetStateStore(storeName);
        }

        public IValueAndTimestamp<V> Get(K key)
        {
            return this.store.Get(key);
        }

        public void Close() { }
    }
}
