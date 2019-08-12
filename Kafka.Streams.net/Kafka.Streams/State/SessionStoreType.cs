





















//namespace Kafka.Streams.State
//{

//    public static class SessionStoreType<K, V> : QueryableStoreTypeMatcher<ReadOnlySessionStore<K, V>>
//    {
//        static SessionStoreType()
//            : base(Collections.singleton(ReadOnlySessionStore))
//        {
//        }


//        public ReadOnlySessionStore<K, V> create(StateStoreProvider storeProvider,
//                                                 string storeName)
//        {
//            return new CompositeReadOnlySessionStore<>(storeProvider, this, storeName);
//        }
//    }
//}
