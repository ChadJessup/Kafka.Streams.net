
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableSourceValueGetter<K, V> : IKTableValueGetter<K, V>
    {
        private ITimestampedKeyValueStore<K, V> store = null;

        public void Init(IProcessorContext context, string storeName)
        {
            store = (ITimestampedKeyValueStore<K, V>)context.GetStateStore(storeName);
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
