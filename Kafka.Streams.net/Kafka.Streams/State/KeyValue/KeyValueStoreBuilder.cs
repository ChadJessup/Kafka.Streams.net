using Kafka.Common;
using Kafka.Streams.Interfaces;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.Metered;
using System;

namespace Kafka.Streams.State.KeyValues
{
    public class KeyValueStoreBuilder<K, V> : AbstractStoreBuilder<K, V, IKeyValueStore<K, V>>
    {
        private readonly IKeyValueBytesStoreSupplier storeSupplier;

        public KeyValueStoreBuilder(
            KafkaStreamsContext context,
            IKeyValueBytesStoreSupplier storeSupplier,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
            : base(context, storeSupplier.Name, keySerde, valueSerde)
        {
            storeSupplier = storeSupplier ?? throw new ArgumentNullException(nameof(storeSupplier));
            this.storeSupplier = storeSupplier;
        }

        public override IKeyValueStore<K, V> Build()
        {
            return new MeteredKeyValueStore<K, V>(
                this.context,
                storeSupplier.Get(),
                keySerde,
                valueSerde);
        }
    }
}
