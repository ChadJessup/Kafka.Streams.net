using Kafka.Common;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.KStream.Internals
{
    public class TimestampedKeyValueStoreMaterializer<K, V>
    {
        private readonly MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>? materialized;
        private readonly KafkaStreamsContext context;

        public TimestampedKeyValueStoreMaterializer(
            KafkaStreamsContext context,
            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>? materialized)
        {
            this.context = context; ;
            this.materialized = materialized;
        }

        /**
         * @return  StoreBuilder
         */
        public IStoreBuilder<ITimestampedKeyValueStore<K, V>> Materialize()
        {
            var supplier = (IKeyValueBytesStoreSupplier)this.materialized?.StoreSupplier ?? null;

            if (supplier == null)
            {
                var Name = this.materialized?.StoreName;
                supplier = this.context.StoresFactory.PersistentTimestampedKeyValueStore(Name);
            }

            IStoreBuilder<ITimestampedKeyValueStore<K, V>> builder =
                this.context.StoresFactory.TimestampedKeyValueStoreBuilder(
                   this.context,
                   supplier,
                   this.materialized?.KeySerde,
                   this.materialized?.ValueSerde);

            if (this.materialized?.LoggingEnabled == true)
            {
                builder.WithLoggingEnabled(this.materialized.LogConfig());
            }
            else
            {
                builder.WithLoggingDisabled();
            }

            if (this.materialized?.CachingEnabled == true)
            {
                builder.WithCachingEnabled();
            }

            return builder;
        }
    }
}
