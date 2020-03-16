using Kafka.Streams.Interfaces;
using Kafka.Streams.State.Internals;
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

        public long retentionPeriod()
        {
            return storeSupplier.retentionPeriod();
        }
    }
}