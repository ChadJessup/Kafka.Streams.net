using Kafka.Common;
using Kafka.Common.Utils;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Interfaces;
using Kafka.Streams.State.ChangeLogging;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.Metered;
using System;

namespace Kafka.Streams.State.Sessions
{
    public class SessionStoreBuilder<K, V> : AbstractStoreBuilder<K, V, ISessionStore<K, V>>
    {
        private ISessionBytesStoreSupplier storeSupplier;

        public SessionStoreBuilder(
            KafkaStreamsContext context,
            ISessionBytesStoreSupplier storeSupplier,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
            : base(context, "", keySerde, valueSerde)
        {
            this.storeSupplier = storeSupplier;
        }

        public override ISessionStore<K, V> Build()
        {
            return new MeteredSessionStore<K, V>(
                this.context,
                MaybeWrapCaching(MaybeWrapLogging(storeSupplier.Get())),
                keySerde,
                valueSerde);
        }

        private ISessionStore<Bytes, byte[]> MaybeWrapCaching(ISessionStore<Bytes, byte[]> inner)
        {
            if (!enableCaching)
            {
                return inner;
            }

            return new CachingSessionStore(this.context, inner, storeSupplier.SegmentIntervalMs());
        }

        private ISessionStore<Bytes, byte[]> MaybeWrapLogging(ISessionStore<Bytes, byte[]> inner)
        {
            if (!enableLogging)
            {
                return inner;
            }

            return new ChangeLoggingSessionBytesStore(context, inner);
        }

        public TimeSpan RetentionPeriod() => storeSupplier.RetentionPeriod;
    }
}
