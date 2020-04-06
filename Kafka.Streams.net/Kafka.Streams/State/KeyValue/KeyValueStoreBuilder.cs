using Kafka.Common;
using Kafka.Streams.Interfaces;
using Kafka.Streams.State.Internals;

using System;

namespace Kafka.Streams.State.KeyValues
{
    public class KeyValueStoreBuilder<K, V> : AbstractStoreBuilder<K, V, IKeyValueStore<K, V>>
    {
        private readonly IKeyValueBytesStoreSupplier storeSupplier;

        public KeyValueStoreBuilder(
            IKeyValueBytesStoreSupplier storeSupplier,
            ISerde<K> keySerde,
            ISerde<V> valueSerde,
            IClock clock)
            : base(storeSupplier.Name, keySerde, valueSerde, clock)
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
    }
}