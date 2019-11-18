
//using Kafka.Common.Utils;
//using Kafka.Common.Utils.Interfaces;
//using Kafka.Streams.Interfaces;

//namespace Kafka.Streams.State.Internals
//{
//    public class KeyValueStoreBuilder<K, V> : AbstractStoreBuilder<K, V, IKeyValueStore<K, V>>
//    {
//        private IKeyValueBytesStoreSupplier storeSupplier;

//        public KeyValueStoreBuilder(IKeyValueBytesStoreSupplier storeSupplier,
//                                    ISerde<K> keySerde,
//                                    ISerde<V> valueSerde,
//                                    ITime time)
//            : base(storeSupplier.name, keySerde, valueSerde, time)
//        {
//            storeSupplier = storeSupplier ?? throw new ArgumentNullException(nameof(storeSupplier));
//            this.storeSupplier = storeSupplier;
//        }

//        public override IKeyValueStore<K, V> build()
//        {
//            return new MeteredKeyValueStore<>(
//                maybeWrapCaching(maybeWrapLogging(storeSupplier)),
//                storeSupplier.metricsScope(),
//                time,
//                keySerde,
//                valueSerde);
//        }

//        private IKeyValueStore<Bytes, byte[]> maybeWrapCaching(IKeyValueStore<Bytes, byte[]> inner)
//        {
//            if (!enableCaching)
//            {
//                return inner;
//            }
//            return new CachingKeyValueStore(inner);
//        }

//        private IKeyValueStore<Bytes, byte[]> maybeWrapLogging(IKeyValueStore<Bytes, byte[]> inner)
//        {
//            if (!enableLogging)
//            {
//                return inner;
//            }
//            return new ChangeLoggingKeyValueBytesStore(inner);
//        }
//    }
//}