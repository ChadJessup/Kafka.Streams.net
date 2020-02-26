using Kafka.Common.Utils;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Interfaces;
using Kafka.Streams.State.Internals;
using NodaTime;
using System;

namespace Kafka.Streams.State.KeyValue
{
    public class KeyValueStoreBuilder<K, V> : AbstractStoreBuilder<K, V, IKeyValueStore<K, V>>
    {
        private readonly IKeyValueBytesStoreSupplier storeSupplier;

        public KeyValueStoreBuilder(
            IKeyValueBytesStoreSupplier storeSupplier,
            ISerde<K> keySerde,
            ISerde<V> valueSerde,
            IClock clock)
            : base(storeSupplier.name, keySerde, valueSerde, clock)
        {
            storeSupplier = storeSupplier ?? throw new ArgumentNullException(nameof(storeSupplier));
            this.storeSupplier = storeSupplier;
        }

        public override IKeyValueStore<K, V> Build()
        {
            return null;
                // new MeteredKeyValueStore<K, V>(
                // maybeWrapCaching(maybeWrapLogging(storeSupplier)),
                // storeSupplier.metricsScope(),
                // this.clock,
                // keySerde,
                // valueSerde);
        }

        private IKeyValueStore<Bytes, byte[]> maybeWrapCaching(IKeyValueStore<Bytes, byte[]> inner)
        {
            if (!enableCaching)
            {
                return inner;
            }

            return null;// new CachingKeyValueStore(inner);
        }

        private IKeyValueStore<Bytes, byte[]> maybeWrapLogging(IKeyValueStore<Bytes, byte[]> inner)
        {
            if (!enableLogging)
            {
                return inner;
            }

            return new ChangeLoggingKeyValueBytesStore(inner);
        }
    }
}