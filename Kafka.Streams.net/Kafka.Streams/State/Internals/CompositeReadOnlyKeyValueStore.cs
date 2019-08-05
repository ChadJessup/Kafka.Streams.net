using Kafka.Streams.State;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    /**
     * A wrapper over the underlying {@link ReadOnlyKeyValueStore}s found in a {@link
     * org.apache.kafka.streams.processor.Internals.ProcessorTopology}
     *
     * @param key type
     * @param value type
     */
    public class CompositeReadOnlyKeyValueStore<K, V> : IReadOnlyKeyValueStore<K, V>
    {

        private StateStoreProvider storeProvider;
        private QueryableStoreType<IReadOnlyKeyValueStore<K, V>> storeType;
        private string storeName;

        public CompositeReadOnlyKeyValueStore(
            StateStoreProvider storeProvider,
            QueryableStoreType<IReadOnlyKeyValueStore<K, V>> storeType,
            string storeName)
        {
            this.storeProvider = storeProvider;
            this.storeType = storeType;
            this.storeName = storeName;
        }


        public override V get(K key)
        {
            List<IReadOnlyKeyValueStore<K, V>> stores = storeProvider.stores(storeName, storeType);
            foreach (IReadOnlyKeyValueStore<K, V> store in stores)
            {
                try
                {
                    V result = store[key];
                    if (result != null)
                    {
                        return result;
                    }
                }
                catch (InvalidStateStoreException e)
                {
                    throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
                }

            }
            return default;
        }

        public override KeyValueIterator<K, V> range(K from, K to)
        {
            //NextIteratorFunction<K, V, IReadOnlyKeyValueStore<K, V>> nextIteratorFunction = new NextIteratorFunction<K, V, IReadOnlyKeyValueStore<K, V>>()
            //{

            //public KeyValueIterator<K, V> apply(IReadOnlyKeyValueStore<K, V> store)
            //{
            //    try
            //    {
            //        return store.range(from, to);
            //    }
            //    catch (InvalidStateStoreException e)
            //    {
            //        throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
            //    }
            //}
            //}

            List<IReadOnlyKeyValueStore<K, V>> stores = storeProvider.stores(storeName, storeType);
            return new DelegatingPeekingKeyValueIterator<>(storeName, new CompositeKeyValueIterator<>(stores.iterator(), nextIteratorFunction));
        }

        public override KeyValueIterator<K, V> all()
        {
            //NextIteratorFunction<K, V, IReadOnlyKeyValueStore<K, V>> nextIteratorFunction = new NextIteratorFunction<K, V, IReadOnlyKeyValueStore<K, V>>()
            //{

            //    public KeyValueIterator<K, V> apply(IReadOnlyKeyValueStore<K, V> store)
            //{
            //    try
            //    {
            //        return store.all();
            //    }
            //    catch (InvalidStateStoreException e)
            //    {
            //        throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
            //    }
            //}

            //List<IReadOnlyKeyValueStore<K, V>> stores = storeProvider.stores(storeName, storeType);

            return new DelegatingPeekingKeyValueIterator<>(storeName, new CompositeKeyValueIterator<>(stores.iterator(), nextIteratorFunction));
        }

        public override long approximateNumEntries()
        {
            List<IReadOnlyKeyValueStore<K, V>> stores = storeProvider.stores(storeName, storeType);
            long total = 0;

            foreach (IReadOnlyKeyValueStore<K, V> store in stores)
            {
                total += store.approximateNumEntries();
                if (total < 0)
                {
                    return long.MaxValue;
                }
            }
            return total;
        }
    }