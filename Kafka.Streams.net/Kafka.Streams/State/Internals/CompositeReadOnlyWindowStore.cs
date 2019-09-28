//using Kafka.Streams.Errors;
//using Kafka.Streams.Internals;
//using Kafka.Streams.KStream;
//using Kafka.Streams.State.Interfaces;
//using System;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Internals
//{
//    /**
//     * Wrapper over the underlying {@link ReadOnlyWindowStore}s found in a {@link
//     * org.apache.kafka.streams.processor.Internals.ProcessorTopology}
//     */
//    public class CompositeReadOnlyWindowStore<K, V> : IReadOnlyWindowStore<K, V>
//    {
//        private IQueryableStoreType<IReadOnlyWindowStore<K, V>> windowStoreType;

//        private string storeName;
//        private IStateStoreProvider provider;

//        public CompositeReadOnlyWindowStore(
//            IStateStoreProvider provider,
//            IQueryableStoreType<IReadOnlyWindowStore<K, V>> windowStoreType,
//            string storeName)
//        {
//            this.provider = provider;
//            this.windowStoreType = windowStoreType;
//            this.storeName = storeName;
//        }

//        public V fetch(K key, long time)
//        {
//            key = key ?? throw new ArgumentNullException(nameof(key));
//            List<IReadOnlyWindowStore<K, V>> stores = provider.stores<IReadOnlyWindowStore<K, V>>(storeName, windowStoreType);

//            foreach (IReadOnlyWindowStore<K, V> windowStore in stores)
//            {
//                try
//                {
//                    V result = windowStore.fetch(key, time);
//                    if (result != null)
//                    {
//                        return result;
//                    }
//                }
//                catch (InvalidStateStoreException e)
//                {
//                    throw new InvalidStateStoreException(
//                            "State store is not available anymore and may have been migrated to another instance; " +
//                                    "please re-discover its location from the state metadata.");
//                }
//            }

//            return default;
//        }

//        [Obsolete]
//        public IWindowStoreIterator<V> fetch(K key, long timeFrom, long timeTo)
//        {
//            key = key ?? throw new ArgumentNullException("key can't be null", nameof(key));
//            List<IReadOnlyWindowStore<K, V>> stores = provider.stores<IReadOnlyWindowStore<K, V>>(storeName, windowStoreType);
//            foreach (IReadOnlyWindowStore<K, V> windowStore in stores)
//            {
//                try
//                {
//                    IWindowStoreIterator<V> result = windowStore.fetch(key, timeFrom, timeTo);
//                    if (!result.MoveNext())
//                    {
//                        result.close();
//                    }
//                    else
//                    {
//                        return result;
//                    }
//                }
//                catch (InvalidStateStoreException e)
//                {
//                    throw new InvalidStateStoreException(
//                            "State store is not available anymore and may have been migrated to another instance; " +
//                                    "please re-discover its location from the state metadata.");
//                }
//            }

//            return KeyValueIterators<V>.EMPTY_WINDOW_STORE_ITERATOR;
//        }


//        public IWindowStoreIterator<V> fetch(K key, DateTime from, DateTime to)
//        {
//            return fetch(
//                key,
//                ApiUtils.validateMillisecondInstant(from, ApiUtils.prepareMillisCheckFailMsgPrefix(from, "from")),
//                ApiUtils.validateMillisecondInstant(to, ApiUtils.prepareMillisCheckFailMsgPrefix(to, "to")));
//        }


//        public IKeyValueIterator<K, V> fetch(
//            K from,
//            K to,
//            long timeFrom,
//            long timeTo)
//        {
//            from = from ?? throw new ArgumentNullException(nameof(from));
//            to = to ?? throw new ArgumentNullException(nameof(to));

//            INextIteratorFunction<K, V, IReadOnlyWindowStore<K, V>> nextIteratorFunction = null;
//            //                store => store.fetch(from, to, timeFrom, timeTo);

//            var enumerator = provider.stores(storeName, windowStoreType).GetEnumerator();
//            var compositeKVIterator = new CompositeKeyValueIterator<K, V, IReadOnlyWindowStore<K, V>>(
//                    enumerator,
//                    nextIteratorFunction);

//            var dpKVIterator = new DelegatingPeekingKeyValueIterator<K, V>(storeName,
//                compositeKVIterator);

//            return dpKVIterator;
//        }

//        public IKeyValueIterator<K, V> fetch(
//            K from,
//            K to,
//            DateTime fromTime,
//            DateTime toTime)
//        {
//            return fetch(
//                from,
//                to,
//                ApiUtils.validateMillisecondInstant(fromTime, ApiUtils.prepareMillisCheckFailMsgPrefix(fromTime, "fromTime")),
//                ApiUtils.validateMillisecondInstant(toTime, ApiUtils.prepareMillisCheckFailMsgPrefix(toTime, "toTime")));
//        }

//        public IKeyValueIterator<K, V> all()
//        {
//            INextIteratorFunction<K, V, IReadOnlyWindowStore<K, V>> nextIteratorFunction = null;// IReadOnlyWindowStore;

//            return new DelegatingPeekingKeyValueIterator<K, V>(
//                storeName,
//                new CompositeKeyValueIterator<K, V, IReadOnlyWindowStore<K, V>>(
//                    provider.stores(storeName, windowStoreType).GetEnumerator(),
//                    nextIteratorFunction));
//        }


//        [System.Obsolete]
//        public IKeyValueIterator<K, V> fetchAll(long timeFrom, long timeTo)
//        {
//            INextIteratorFunction<K, V, IReadOnlyWindowStore<K, V>> nextIteratorFunction = null;
//                //store => store.fetchAll(timeFrom, timeTo);

//            return new DelegatingPeekingKeyValueIterator<K, V>(
//                storeName,
//                new CompositeKeyValueIterator<K, V, IReadOnlyWindowStore<K, V>>(
//                    provider.stores(storeName, windowStoreType).GetEnumerator(),
//                    nextIteratorFunction));
//        }


//        public IKeyValueIterator<K, V> fetchAll(DateTime from, DateTime to)
//        {
//            return fetchAll(
//                ApiUtils.validateMillisecondInstant(from, ApiUtils.prepareMillisCheckFailMsgPrefix(from, "from")),
//                ApiUtils.validateMillisecondInstant(to, ApiUtils.prepareMillisCheckFailMsgPrefix(to, "to")));
//        }
//    }
//}