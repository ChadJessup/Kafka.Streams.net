﻿using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableMaterializedValueGetter<K, V> : IKTableValueGetter<K, V>
    {
        private ITimestampedKeyValueStore<K, V> store;

        public void init(IProcessorContext context, string storeName)
        {
            store = (ITimestampedKeyValueStore<K, V>)context.getStateStore(storeName);
        }

        public ValueAndTimestamp<V> get(K key)
        {
            return store.Get(key);
        }

        public void close() { }
    }
}
