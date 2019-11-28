using Kafka.Common.Utils;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Interfaces;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.Window;
using NodaTime;

namespace Kafka.Streams.State.Window
{
    public class WindowStoreBuilder<K, V> : AbstractStoreBuilder<K, V, IWindowStore<K, V>>
    {
        private readonly IWindowBytesStoreSupplier storeSupplier;

        public WindowStoreBuilder(
            IWindowBytesStoreSupplier storeSupplier,
            ISerde<K> keySerde,
            ISerde<V> valueSerde,
            IClock clock)
            : base(storeSupplier.name, keySerde, valueSerde, clock)
        {
            this.storeSupplier = storeSupplier;
        }

        public override IWindowStore<K, V> Build()
        {
            return null;
            //new MeteredWindowStore<K, V>(
            //    maybeWrapCaching(maybeWrapLogging(storeSupplier)),
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
            //new ChangeLoggingWindowBytesStore(inner, storeSupplier.retainDuplicates());
        }

        public long retentionPeriod()
        {
            return storeSupplier.retentionPeriod();
        }
    }
}