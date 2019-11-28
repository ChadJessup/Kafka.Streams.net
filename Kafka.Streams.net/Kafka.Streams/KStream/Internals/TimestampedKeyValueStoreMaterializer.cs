using Kafka.Common.Utils;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValue;
using Kafka.Streams.State.TimeStamped;
using NodaTime;

namespace Kafka.Streams.KStream.Internals
{
    public class TimestampedKeyValueStoreMaterializer<K, V>
    {
        private readonly IClock clock;
        private readonly MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materialized;

        public TimestampedKeyValueStoreMaterializer(
            IClock clock,
            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            this.clock = clock;
            this.materialized = materialized;
        }

        /**
         * @return  StoreBuilder
         */
        public IStoreBuilder<ITimestampedKeyValueStore<K, V>> materialize()
        {
            IKeyValueBytesStoreSupplier supplier = (IKeyValueBytesStoreSupplier)materialized.StoreSupplier;

            if (supplier == null)
            {
                string name = materialized.StoreName;
                supplier = Stores.persistentTimestampedKeyValueStore(name);
            }

            IStoreBuilder<ITimestampedKeyValueStore<K, V>> builder =
                Stores.timestampedKeyValueStoreBuilder(
                    this.clock,
                   supplier,
                   materialized.KeySerde,
                   materialized.ValueSerde);

            if (materialized.LoggingEnabled)
            {
                builder.WithLoggingEnabled(materialized.logConfig());
            }
            else
            {
                builder.WithLoggingDisabled();
            }

            if (materialized.cachingEnabled)
            {
                builder.WithCachingEnabled();
            }

            return builder;
        }
    }
}