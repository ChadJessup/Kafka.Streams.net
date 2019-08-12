





















//namespace Kafka.Streams.State
//{

//    public static class TimestampedKeyValueStoreType<K, V>
//            : QueryableStoreTypeMatcher<ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>>
//    {

//        TimestampedKeyValueStoreType()
//        {
//            base(new HashSet<>(Arrays.asList(
//                ITimestampedKeyValueStore,
//                IReadOnlyKeyValueStore)));
//        }


//        public IReadOnlyKeyValueStore<K, ValueAndTimestamp<V>> create(StateStoreProvider storeProvider,
//                                                                     string storeName)
//        {
//            return new CompositeReadOnlyKeyValueStore<>(storeProvider, this, storeName);
//        }
//    }
//}
