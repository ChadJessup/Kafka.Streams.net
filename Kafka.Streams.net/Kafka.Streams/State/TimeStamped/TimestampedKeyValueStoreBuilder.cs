using Kafka.Streams.Interfaces;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;
using NodaTime;
using System;

namespace Kafka.Streams.State.TimeStamped
{
    public class TimestampedKeyValueStoreBuilder<K, V>
        : AbstractStoreBuilder<K, ValueAndTimestamp<V>, ITimestampedKeyValueStore<K, V>>
    {
        private readonly IKeyValueBytesStoreSupplier storeSupplier;

        public TimestampedKeyValueStoreBuilder(
            IKeyValueBytesStoreSupplier storeSupplier,
            ISerde<K> keySerde,
            ISerde<V> valueSerde,
            IClock clock)
            : base(
                storeSupplier.name,
                keySerde,
                valueSerde == null ? null : new ValueAndTimestampSerde<V>(valueSerde),
                clock)
        {
            storeSupplier = storeSupplier ?? throw new ArgumentNullException(nameof(storeSupplier));

            this.storeSupplier = storeSupplier;
        }

        public override ITimestampedKeyValueStore<K, V> Build()
        {
            IKeyValueStore<Bytes, byte[]> store = storeSupplier.get();

            if (!(store is ITimestampedBytesStore))
            {
                if (store.persistent())
                {
                    store = null; // new KeyValueToTimestampedKeyValueByteStoreAdapter(store);
                }
                else
                {
                    store = null; // new InMemoryTimestampedKeyValueStoreMarker(store);
                }
            }

            return null;
            //new MeteredTimestampedKeyValueStore<>(
            //    maybeWrapCaching(maybeWrapLogging(store)),
            //    storeSupplier.metricsScope(),
            //    time,
            //    keySerde,
            //    valueSerde);
        }
    }
}