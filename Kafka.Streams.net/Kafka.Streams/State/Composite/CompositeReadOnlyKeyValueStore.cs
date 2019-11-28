//using Kafka.Streams.Errors;
//using Kafka.Streams.State.Interfaces;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Internals
//{
//    /**
//     * A wrapper over the underlying {@link ReadOnlyKeyValueStore}s found in a {@link
//     * org.apache.kafka.streams.processor.Internals.ProcessorTopology}
//     *
//     * @param key type
//     * @param value type
//     */
//    public class CompositeReadOnlyKeyValueStore<K, V> : IReadOnlyKeyValueStore<K, V>
//    {
//        private IStateStoreProvider storeProvider;
//        private IQueryableStoreType<IReadOnlyKeyValueStore<K, V>> storeType;
//        private string storeName;

//        public CompositeReadOnlyKeyValueStore(
//            IStateStoreProvider storeProvider,
//            IQueryableStoreType<IReadOnlyKeyValueStore<K, V>> storeType,
//            string storeName)
//        {
//            this.storeProvider = storeProvider;
//            this.storeType = storeType;
//            this.storeName = storeName;
//        }

//        public V get(K key)
//        {
//            var stores = this.storeProvider.stores<IReadOnlyKeyValueStore<K, V>>(this.storeName, this.storeType);
//            foreach (IReadOnlyKeyValueStore<K, V> store in stores)
//            {
//                try
//                {
//                    V result = store.get(key);

//                    if (result != null)
//                    {
//                        return result;
//                    }
//                }
//                catch (InvalidStateStoreException e)
//                {
//                    throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
//                }

//            }
//            return default;
//        }

//        public IKeyValueIterator<K, V> range(K from, K to)
//        {
//            INextIteratorFunction<K, V, IReadOnlyKeyValueStore<K, V>> nextIteratorFunction = null; // new INextIteratorFunction<K, V, IReadOnlyKeyValueStore<K, V>>()
//            {

//                public KeyValueIterator<K, V> apply(IReadOnlyKeyValueStore<K, V> store)
//                {
//                    try
//                    {
//                        return store.range(from, to);
//                    }
//                    catch (InvalidStateStoreException e)
//                    {
//                        throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
//                    }
//                }
//            }

//            List<IReadOnlyKeyValueStore<K, V>> stores = this.storeProvider.stores<IReadOnlyKeyValueStore<K, V>>(this.storeName, this.storeType);

//            return new DelegatingPeekingKeyValueIterator<K, V>(
//                storeName,
//                new CompositeKeyValueIterator<IReadOnlyKeyValueStore<K, V>, K, V>(
//                    stores.GetEnumerator(),
//                    nextIteratorFunction));
//        }

//        public IKeyValueIterator<K, V> all()
//        {
//            NextIteratorFunction<K, V, IReadOnlyKeyValueStore<K, V>> nextIteratorFunction = new NextIteratorFunction<K, V, IReadOnlyKeyValueStore<K, V>>()
//            {

//                public KeyValueIterator<K, V> apply(IReadOnlyKeyValueStore<K, V> store)
//            {
//                try
//                {
//                    return store.all();
//                }
//                catch (InvalidStateStoreException e)
//                {
//                    throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
//                }
//            }

//            List<IReadOnlyKeyValueStore<K, V>> stores = this.storeProvider.stores<IReadOnlyKeyValueStore<K, V>>(this.storeName, this.storeType);
//            return new DelegatingPeekingKeyValueIterator<K, V>(storeName, new CompositeKeyValueIterator<IReadOnlyKeyValueStore<K, V>, K, V>(stores.GetEnumerator(), nextIteratorFunction));
//        }

//        public long approximateNumEntries
//        {
//            get
//            {
//                List<IReadOnlyKeyValueStore<K, V>> stores = this.storeProvider.stores<IReadOnlyKeyValueStore<K, V>>(this.storeName, this.storeType);
//                long total = 0;

//                foreach (IReadOnlyKeyValueStore<K, V> store in stores)
//                {
//                    total += store.approximateNumEntries;
//                    if (total < 0)
//                    {
//                        return long.MaxValue;
//                    }
//                }

//                return total;
//            }
//        }
//    }
//}