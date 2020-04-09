
//    public class CompositeReadOnlySessionStore<K, V> : ReadOnlySessionStore<K, V>
//    {
//        private IStateStoreProvider storeProvider;
//        private IQueryableStoreType<ReadOnlySessionStore<K, V>> queryableStoreType;
//        private string storeName;

//        public CompositeReadOnlySessionStore(IStateStoreProvider storeProvider,
//                                             IQueryableStoreType<ReadOnlySessionStore<K, V>> queryableStoreType,
//                                             string storeName)
//        {
//            this.storeProvider = storeProvider;
//            this.queryableStoreType = queryableStoreType;
//            this.storeName = storeName;
//        }

//        public override IKeyValueIterator<Windowed<K>, V> fetch(K key)
//        {
//            key = key ?? throw new ArgumentNullException(nameof(key));
//            List<ReadOnlySessionStore<K, V>> stores = storeProvider.stores(storeName, queryableStoreType);
//            foreach (ReadOnlySessionStore<K, V> store in stores)
//            {
//                try
//                {
//                    IKeyValueIterator<Windowed<K>, V> result = store.Fetch(key);
//                    if (!result.HasNext())
//                    {
//                        result.close();
//                    }
//                    else
//                    {
//                        return result;
//                    }
//                }
//                catch (InvalidStateStoreException ise)
//                {
//                    throw new InvalidStateStoreException("State store  [" + storeName + "] is not available anymore" +
//                                                                 " and may have been migrated to another instance; " +
//                                                                 "please re-discover its location from the state metadata. " +
//                                                                 "Original error message: " + ise.ToString());
//                }
//            }
//            return KeyValueIterators.emptyIterator();
//        }

//        public override IKeyValueIterator<Windowed<K>, V> fetch(K from, K to)
//        {
//            from = from ?? throw new ArgumentNullException(nameof(from));
//            to = to ?? throw new ArgumentNullException(nameof(to));
//            INextIteratorFunction<Windowed<K>, V, ReadOnlySessionStore<K, V>> nextIteratorFunction = store => store.Fetch(from, to);
//            return new DelegatingPeekingKeyValueIterator<>(storeName,
//                                                           new CompositeKeyValueIterator<>(
//                                                                   storeProvider.stores(storeName, queryableStoreType).iterator(),
//                                                                   nextIteratorFunction));
//        }
//    }
//}