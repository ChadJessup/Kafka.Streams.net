using Kafka.Common.Utils;
using Kafka.Streams.Interfaces;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.State.Window;
using NodaTime;
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
                  storeSupplier.name,
                  keySerde,
                  valueSerde == null ? null : new ValueAndTimestampSerde<V>(valueSerde),
                  clock)
        {
            storeSupplier = storeSupplier ?? throw new ArgumentNullException(nameof(storeSupplier));

            this.storeSupplier = storeSupplier;
        }

        public override ITimestampedWindowStore<K, V> Build()
        {
            IWindowStore<Bytes, byte[]> store = storeSupplier.get();
            if (!(store is ITimestampedBytesStore))
            {
                if (store.persistent())
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

        private IWindowStore<Bytes, byte[]> maybeWrapCaching(IWindowStore<Bytes, byte[]> inner)
        {
            if (!enableCaching)
            {
                return inner;
            }

            return null;
            //new CachingWindowStore(
            //    inner,
            //    storeSupplier.windowSize(),
            //    storeSupplier.segmentIntervalMs());
        }

        private IWindowStore<Bytes, byte[]> maybeWrapLogging(IWindowStore<Bytes, byte[]> inner)
        {
            if (!enableLogging)
            {
                return inner;
            }

            return null; 
                //new ChangeLoggingTimestampedWindowBytesStore(inner, storeSupplier.retainDuplicates());
        }

        public long retentionPeriod()
        {
            return storeSupplier.retentionPeriod();
        }
    }
}