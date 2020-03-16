using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.TimeStamped;
using NodaTime;

namespace Kafka.Streams.KStream.Internals
{
    public class TimestampedKeyValueStoreMaterializer<K, V>
    {
        private readonly IClock clock;
        private readonly MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>? materialized;

        public TimestampedKeyValueStoreMaterializer(
            IClock clock,
            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>? materialized)
        {
            this.clock = clock;
            this.materialized = materialized;
        }

        /**
         * @return  StoreBuilder
         */
        public IStoreBuilder<ITimestampedKeyValueStore<K, V>> materialize()
        {
            var supplier = (IKeyValueBytesStoreSupplier)materialized?.StoreSupplier ?? null;

            if (supplier == null)
            {
                var name = materialized?.StoreName;
                supplier = Stores.persistentTimestampedKeyValueStore(name);
            }

            IStoreBuilder<ITimestampedKeyValueStore<K, V>> builder =
                Stores.timestampedKeyValueStoreBuilder(
                    this.clock,
                   supplier,
                   materialized?.KeySerde,
                   materialized?.ValueSerde);

            if (materialized?.LoggingEnabled == true)
            {
                builder.WithLoggingEnabled(materialized.logConfig());
            }
            else
            {
                builder.WithLoggingDisabled();
            }

            if (materialized?.cachingEnabled == true)
            {
                builder.WithCachingEnabled();
            }

            return builder;
        }
    }
}
