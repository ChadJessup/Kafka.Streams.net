//using Kafka.Common;
//using Kafka.Common.Utils;
//using Kafka.Common.Utils.Interfaces;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.State.Interfaces;
//using System;

//namespace Kafka.Streams.State.Internals
//{
//    public class SessionStoreBuilder<K, V> : AbstractStoreBuilder<K, V, ISessionStore<K, V>>
//    {
//        private ISessionBytesStoreSupplier storeSupplier;

//        public SessionStoreBuilder(
//            ISessionBytesStoreSupplier storeSupplier,
//            ISerde<K> keySerde,
//            ISerde<V> valueSerde,
//            ITime time)
//            : base("", keySerde, valueSerde, time)
//        {
//            this.storeSupplier = storeSupplier;
//        }

//        public override ISessionStore<K, V> build()
//        {
//            return new MeteredSessionStore<K, V>(
//                maybeWrapCaching(maybeWrapLogging(storeSupplier)),
//                storeSupplier.metricsScope,
//                keySerde,
//                valueSerde,
//                time);
//        }

//        private ISessionStore<Bytes, byte[]> maybeWrapCaching(ISessionStore<Bytes, byte[]> inner)
//        {
//            if (!enableCaching)
//            {
//                return inner;
//            }
//            return new CachingSessionStore(inner, storeSupplier.segmentIntervalMs());
//        }

//        private ISessionStore<Bytes, byte[]> maybeWrapLogging(ISessionStore<Bytes, byte[]> inner)
//        {
//            if (!enableLogging)
//            {
//                return inner;
//            }
//            return new ChangeLoggingSessionBytesStore(inner);
//        }

//        public long retentionPeriod()
//        {
//            return storeSupplier.retentionPeriod();
//        }
//    }
