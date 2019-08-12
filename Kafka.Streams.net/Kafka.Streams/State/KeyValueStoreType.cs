





















//namespace Kafka.Streams.State
//{

//    public class KeyValueStoreType<K, V> : QueryableStoreTypeMatcher<ReadOnlyKeyValueStore<K, V>>
//    {

//        KeyValueStoreType()
//        {
//            base(Collections.singleton(ReadOnlyKeyValueStore));
//        }


//        public IReadOnlyKeyValueStore<K, V> create(StateStoreProvider storeProvider,
//                                                  string storeName)
//        {
//            return new CompositeReadOnlyKeyValueStore<>(storeProvider, this, storeName);
//        }

//    }
//}
