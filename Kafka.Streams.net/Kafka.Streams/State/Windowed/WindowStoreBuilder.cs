using Kafka.Streams.Interfaces;
using Kafka.Streams.State.Internals;
using NodaTime;
using System;

namespace Kafka.Streams.State.Windowed
{
    public class WindowStoreBuilder<K, V> : AbstractStoreBuilder<K, V, IWindowStore<K, V>>
    {
        private readonly IWindowBytesStoreSupplier storeSupplier;

        public WindowStoreBuilder(
            IWindowBytesStoreSupplier storeSupplier,
            ISerde<K> keySerde,
            ISerde<V> valueSerde,
            IClock clock)
            : base(storeSupplier.Name, keySerde, valueSerde, clock)
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

        public TimeSpan RetentionPeriod
            => storeSupplier.RetentionPeriod;
    }
}
