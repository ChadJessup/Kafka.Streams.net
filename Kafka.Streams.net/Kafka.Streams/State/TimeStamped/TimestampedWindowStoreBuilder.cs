using Kafka.Common;
using Kafka.Streams.Interfaces;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.Windowed;

using System;

namespace Kafka.Streams.State.TimeStamped
{
    public class TimestampedWindowStoreBuilder<K, V>
        : AbstractStoreBuilder<K, ValueAndTimestamp<V>, ITimestampedWindowStore<K, V>>
    {
        private readonly IWindowBytesStoreSupplier storeSupplier;

        public TimestampedWindowStoreBuilder(
            IWindowBytesStoreSupplier storeSupplier,
            ISerde<K> keySerde,
            ISerde<V> valueSerde,
            IClock clock)
            : base(
                  storeSupplier.Name,
                  keySerde,
                  valueSerde == null ? null : new ValueAndTimestampSerde<V>(valueSerde),
                  clock)
        {
            storeSupplier = storeSupplier ?? throw new ArgumentNullException(nameof(storeSupplier));

            this.storeSupplier = storeSupplier;
        }

        public override ITimestampedWindowStore<K, V> Build()
        {
            IWindowStore<Bytes, byte[]> store = storeSupplier.Get();
            if (!(store is ITimestampedBytesStore))
            {
                if (store.Persistent())
                {
                    store = null;// new WindowToTimestampedWindowByteStoreAdapter(store);
                }
                else
                {
                    store = null; // new InMemoryTimestampedWindowStoreMarker(store);
                }
            }

            return null;
            //new MeteredTimestampedWindowStore<>(
            //    maybeWrapCaching(maybeWrapLogging(store)),
            //    storeSupplier.windowSize(),
            //    storeSupplier.metricsScope(),
            //    time,
            //    keySerde,
            //    valueSerde);
        }

        public long RetentionPeriod()
        {
            return (long)storeSupplier.RetentionPeriod.TotalMilliseconds;
        }
    }
}