using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.Metered;
using Kafka.Streams.State.Windowed;
using System;

namespace Kafka.Streams.State.TimeStamped
{
    public class TimestampedWindowStoreBuilder<K, V>
        : AbstractStoreBuilder<K, IValueAndTimestamp<V>, ITimestampedWindowStore<K, V>>
    {
        private readonly IWindowBytesStoreSupplier storeSupplier;

        public TimestampedWindowStoreBuilder(
            KafkaStreamsContext context,
            IWindowBytesStoreSupplier storeSupplier,
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

        public override ITimestampedWindowStore<K, V> Build()
        {
            IWindowStore<Bytes, byte[]> store = this.storeSupplier.Get();
            if (!(store is ITimestampedBytesStore))
            {
                if (store.Persistent())
                {
                    store = new WindowToTimestampedWindowByteStoreAdapter(store);
                }
                else
                {
                    store = new InMemoryTimestampedWindowStoreMarker(store);
                }
            }

            return (ITimestampedWindowStore<K, V>)new MeteredTimestampedWindowStore<K, V>(
                this.context,
                store, //MaybeWrapCaching(MaybeWrapLogging(store)),
                this.storeSupplier.WindowSize,
                this.keySerde,
                this.valueSerde);
        }

        public long RetentionPeriod()
        {
            return (long)this.storeSupplier.RetentionPeriod.TotalMilliseconds;
        }
    }
}
