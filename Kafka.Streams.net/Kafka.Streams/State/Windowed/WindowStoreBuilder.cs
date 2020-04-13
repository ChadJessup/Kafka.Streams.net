using System;
using Kafka.Streams.Interfaces;
using Kafka.Streams.State.ChangeLogging;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.Metered;

namespace Kafka.Streams.State.Windowed
{
    public class WindowStoreBuilder<K, V> : AbstractStoreBuilder<K, V, IWindowStore<K, V>>
    {
        private readonly IWindowBytesStoreSupplier storeSupplier;

        public WindowStoreBuilder(
            KafkaStreamsContext context,
            IWindowBytesStoreSupplier storeSupplier,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
            : base(
                  context,
                  storeSupplier.Name,
                  keySerde,
                  valueSerde)
        {
            this.storeSupplier = storeSupplier;
        }

        public TimeSpan RetentionPeriod => this.storeSupplier.RetentionPeriod;

        public override IWindowStore<K, V> Build()
        {
            return new MeteredWindowStore<K, V>(
                this.context,
                this.MaybeWrapCaching(this.MaybeWrapLogging(this.storeSupplier.Get())),
                this.storeSupplier.WindowSize,
                this.keySerde,
                this.valueSerde);
        }

        private IWindowStore<Bytes, byte[]> MaybeWrapCaching(IWindowStore<Bytes, byte[]> inner)
        {
            if (!this.enableCaching)
            {
                return inner;
            }

            return new CachingWindowStore(
                this.context,
                inner,
                this.storeSupplier.WindowSize,
                this.storeSupplier.SegmentInterval);
        }

        private IWindowStore<Bytes, byte[]> MaybeWrapLogging(IWindowStore<Bytes, byte[]> inner)
        {
            if (!this.enableLogging)
            {
                return inner;
            }
         
            return new ChangeLoggingWindowBytesStore(
                this.context,
                inner, 
                this.storeSupplier.RetainDuplicates);
        }
    }
}
