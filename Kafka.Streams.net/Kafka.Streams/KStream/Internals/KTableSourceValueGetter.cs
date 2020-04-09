
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableSourceValueGetter<K, V> : IKTableValueGetter<K, V>
    {
        private ITimestampedKeyValueStore<K, V> store = null;
        private readonly KafkaStreamsContext context;

        public KTableSourceValueGetter(KafkaStreamsContext context)
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

        ValueAndTimestamp<V> IKTableValueGetter<K, V>.Get(K key)
        {
            throw new NotImplementedException();
        }
    }
}
