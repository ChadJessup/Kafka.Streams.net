//using Kafka.Streams.Errors;
//using Kafka.Streams.Internals;
//using Kafka.Streams.KStream;
//using Kafka.Streams.State.Interfaces;
//using System;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Internals
//{
//    /**
//     * Wrapper over the underlying {@link IReadOnlyWindowStore}s found in a {@link
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

//        public V Fetch(K key, long time)
//        {
//            key = key ?? throw new ArgumentNullException(nameof(key));
//            List<IReadOnlyWindowStore<K, V>> stores = provider.stores<IReadOnlyWindowStore<K, V>>(storeName, windowStoreType);

//            foreach (IReadOnlyWindowStore<K, V> windowStore in stores)
//            {
//                try
//                {
//                    V result = windowStore.Fetch(key, time);
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
//        public IWindowStoreIterator<V> Fetch(K key, long timeFrom, long timeTo)
//        {
//            key = key ?? throw new ArgumentNullException(nameof(key));
//            List<IReadOnlyWindowStore<K, V>> stores = provider.stores<IReadOnlyWindowStore<K, V>>(storeName, windowStoreType);
//            foreach (IReadOnlyWindowStore<K, V> windowStore in stores)
//            {
//                try
//                {
//                    IWindowStoreIterator<V> result = windowStore.Fetch(key, timeFrom, timeTo);
//                    if (!result.MoveNext())
//                    {
//                        result.Close();
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


//        public IWindowStoreIterator<V> Fetch(K key, DateTime from, DateTime to)
//        {
//            return Fetch(
//                key,
//                ApiUtils.validateMillisecondInstant(from, ApiUtils.ApiUtils.PrepareMillisCheckFailMsgPrefix(from, "from")),
//                ApiUtils.validateMillisecondInstant(to, ApiUtils.ApiUtils.PrepareMillisCheckFailMsgPrefix(to, "to")));
//        }


//        public IKeyValueIterator<K, V> Fetch(
//            K from,
//            K to,
//            long timeFrom,
//            long timeTo)
//        {
//            from = from ?? throw new ArgumentNullException(nameof(from));
//            to = to ?? throw new ArgumentNullException(nameof(to));

//            INextIteratorFunction<K, V, IReadOnlyWindowStore<K, V>> nextIteratorFunction = null;
//            //                store => store.Fetch(from, to, timeFrom, timeTo);

//            var enumerator = provider.stores(storeName, windowStoreType).GetEnumerator();
//            var compositeKVIterator = new CompositeKeyValueIterator<K, V, IReadOnlyWindowStore<K, V>>(
//                    enumerator,
//                    nextIteratorFunction);

//            var dpKVIterator = new DelegatingPeekingKeyValueIterator<K, V>(storeName,
//                compositeKVIterator);

//            return dpKVIterator;
//        }

//        public IKeyValueIterator<K, V> Fetch(
//            K from,
//            K to,
//            DateTime fromTime,
//            DateTime toTime)
//        {
//            return Fetch(
//                from,
//                to,
//                ApiUtils.validateMillisecondInstant(fromTime, ApiUtils.ApiUtils.PrepareMillisCheckFailMsgPrefix(fromTime, "fromTime")),
//                ApiUtils.validateMillisecondInstant(toTime, ApiUtils.ApiUtils.PrepareMillisCheckFailMsgPrefix(toTime, "toTime")));
//        }

//        public IKeyValueIterator<K, V> All()
//        {
//            INextIteratorFunction<K, V, IReadOnlyWindowStore<K, V>> nextIteratorFunction = null;// IReadOnlyWindowStore;

//            return new DelegatingPeekingKeyValueIterator<K, V>(
//                storeName,
//                new CompositeKeyValueIterator<K, V, IReadOnlyWindowStore<K, V>>(
//                    provider.stores(storeName, windowStoreType).GetEnumerator(),
//                    nextIteratorFunction));
//        }


//        [System.Obsolete]
//        public IKeyValueIterator<K, V> FetchAll(long timeFrom, long timeTo)
//        {
//            INextIteratorFunction<K, V, IReadOnlyWindowStore<K, V>> nextIteratorFunction = null;
//                //store => store.FetchAll(timeFrom, timeTo);

//            return new DelegatingPeekingKeyValueIterator<K, V>(
//                storeName,
//                new CompositeKeyValueIterator<K, V, IReadOnlyWindowStore<K, V>>(
//                    provider.stores(storeName, windowStoreType).GetEnumerator(),
//                    nextIteratorFunction));
//        }


//        public IKeyValueIterator<K, V> FetchAll(DateTime from, DateTime to)
//        {
//            return FetchAll(
//                ApiUtils.validateMillisecondInstant(from, ApiUtils.ApiUtils.PrepareMillisCheckFailMsgPrefix(from, "from")),
//                ApiUtils.validateMillisecondInstant(to, ApiUtils.ApiUtils.PrepareMillisCheckFailMsgPrefix(to, "to")));
//        }
//    }
//}