using Kafka.Common;
using Kafka.Streams.Interfaces;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Metered;
using System;

namespace Kafka.Streams.State.TimeStamped
{
    public class TimestampedKeyValueStoreBuilder<K, V>
        : AbstractStoreBuilder<K, IValueAndTimestamp<V>, ITimestampedKeyValueStore<K, V>>
    {
        private readonly IKeyValueBytesStoreSupplier storeSupplier;

        public TimestampedKeyValueStoreBuilder(
            KafkaStreamsContext context,
            IKeyValueBytesStoreSupplier storeSupplier,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
            : base(
                context,
                storeSupplier.Name,
                keySerde,
                valueSerde == null ? null : new ValueAndTimestampSerde<V>(valueSerde))
        {
            storeSupplier = storeSupplier ?? throw new ArgumentNullException(nameof(storeSupplier));

            this.storeSupplier = storeSupplier;
        }

        public override ITimestampedKeyValueStore<K, V> Build()
        {
            IKeyValueStore<Bytes, byte[]> store = this.storeSupplier.Get();

            if (!(store is ITimestampedBytesStore))
            {
                if (store.Persistent())
                {
                    store = new KeyValueToTimestampedKeyValueByteStoreAdapter(store);
                }
                else
                {
                    store = new InMemoryTimestampedKeyValueStoreMarker(store);
                }
            }

            return new MeteredTimestampedKeyValueStore<K, V>(
                this.context,
                store,
                this.keySerde,
                this.valueSerde);
        }
    }
}
