//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.State;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.State.ReadOnly;
//using Kafka.Streams.State.Windowed;
//
//using System.Collections;
//using System.Collections.Generic;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    /**
//     * A very simple window store stub for testing purposes.
//     */
//    public class ReadOnlyWindowStoreStub<K, V> : IReadOnlyWindowStore<K, V>, IStateStore
//    {
//        private readonly long windowSize;
//        private Dictionary<long, NavigableMap<K, V>> data = new HashMap<>();
//        private readonly bool open = true;

//        ReadOnlyWindowStoreStub(long windowSize)
//        {
//            this.windowSize = windowSize;
//        }

//        public V Fetch(K key, long time)
//        {
//            Dictionary<K, V> kvMap = data[time];
//            if (kvMap != null)
//            {
//                return kvMap[key];
//            }
//            else
//            {
//                return null;
//            }
//        }

//        public IWindowStoreIterator<V> Fetch(K key, long timeFrom, long timeTo)
//        {
//            if (!open)
//            {
//                throw new InvalidStateStoreException("Store is not open");
//            }

//            var results = new List<KeyValuePair<long, V>>();
//            for (long now = timeFrom; now <= timeTo; now++)
//            {
//                Dictionary<K, V> kvMap = data[now];
//                if (kvMap != null && kvMap.ContainsKey(key))
//                {
//                    results.Add(new KeyValuePair<long, V>(now, kvMap[key]));
//                }
//            }
//            return new TheWindowStoreIterator<>(results.iterator());
//        }


//        public IWindowStoreIterator<V> Fetch(K key, Instant from, Instant to)
//        {// throws ArgumentException
//            return Fetch(
//                key,
//                ApiUtils.validateMillisecondInstant(from, prepareMillisCheckFailMsgPrefix(from, "from")),
//                ApiUtils.validateMillisecondInstant(to, prepareMillisCheckFailMsgPrefix(to, "to")));
//        }

//        public IKeyValueIterator<IWindowed<K>, V> All()
//        {
//            if (!open)
//            {
//                throw new InvalidStateStoreException("Store is not open");
//            }
//            List<KeyValuePair<IWindowed<K>, V>> results = new List<KeyValuePair<IWindowed<K>, V>>();
//            foreach (long now in data.Keys)
//            {
//                var kvMap = data[now];
//                if (kvMap != null)
//                {
//                    foreach (var entry in kvMap)
//                    {
//                        results.Add(new KeyValuePair<IWindowed<K>, V>(
//                            new Windowed<K>(
//                                entry.Key,
//                                new TimeWindow(now, now + windowSize)), entry.Value));
//                    }
//                }
//            }
//            Iterator<KeyValuePair<IWindowed<K>, V>> iterator = results.iterator();

//            return new IKeyValueIterator<IWindowed<K>, V>()
//            {


//            public void Close() { }


//            public IWindowed<K> PeekNextKey()
//            {
//                throw new UnsupportedOperationException("PeekNextKey() not supported in " + getClass().getName());
//            }


//            public bool HasNext()
//            {
//                return iterator.HasNext();
//            }


//            public KeyValuePair<IWindowed<K>, V> next()
//            {
//                return iterator.MoveNext();
//            }



//            public void remove()
//            {
//                throw new UnsupportedOperationException("remove() not supported in " + getClass().getName());
//            }
//        };
//    }



//    public IKeyValueIterator<IWindowed<K>, V> FetchAll(long timeFrom, long timeTo)
//    {
//        if (!open)
//        {
//            throw new InvalidStateStoreException("Store is not open");
//        }
//        List<KeyValuePair<IWindowed<K>, V>> results = new List<KeyValuePair<IWindowed<K>, V>>();
//        foreach (long now in data.keySet())
//        {
//            if (!(now >= timeFrom && now <= timeTo))
//            {
//                continue;
//            }
//            NavigableDictionary<K, V> kvMap = data.Get(now);
//            if (kvMap != null)
//            {
//                foreach (Entry<K, V> entry in kvMap)
//                {
//                    results.Add(KeyValuePair.Create(new Windowed<>(entry.Key, new TimeWindow(now, now + windowSize)), entry.Value));
//                }
//            }
//        }
//        Iterator<KeyValuePair<IWindowed<K>, V>> iterator = results.iterator();

//        return new IKeyValueIterator<IWindowed<K>, V>()
//        {


//            public void Close() { }


//        public IWindowed<K> PeekNextKey()
//        {
//            throw new UnsupportedOperationException("PeekNextKey() not supported in " + getClass().getName());
//        }


//        public bool HasNext()
//        {
//            return iterator.HasNext();
//        }


//        public KeyValuePair<IWindowed<K>, V> next()
//        {
//            return iterator.MoveNext();
//        }



//        public void remove()
//        {
//            throw new UnsupportedOperationException("remove() not supported in " + getClass().getName());
//        }
//    };
//}


//public IKeyValueIterator<IWindowed<K>, V> FetchAll(Instant from, Instant to)
//{// throws ArgumentException
//    return FetchAll(
//        ApiUtils.validateMillisecondInstant(from, prepareMillisCheckFailMsgPrefix(from, "from")),
//        ApiUtils.validateMillisecondInstant(to, prepareMillisCheckFailMsgPrefix(to, "to")));
//}



//public IKeyValueIterator<IWindowed<K>, V> Fetch(K from, K to, long timeFrom, long timeTo)
//{
//    if (!open)
//    {
//        throw new InvalidStateStoreException("Store is not open");
//    }
//    List<KeyValuePair<IWindowed<K>, V>> results = new List<KeyValuePair<IWindowed<K>, V>>();
//    for (long now = timeFrom; now <= timeTo; now++)
//    {
//        NavigableDictionary<K, V> kvMap = data.Get(now);
//        if (kvMap != null)
//        {
//            foreach (Entry<K, V> entry in kvMap.subMap(from, true, to, true))
//            {
//                results.Add(KeyValuePair.Create(new Windowed<>(entry.Key, new TimeWindow(now, now + windowSize)), entry.Value));
//            }
//        }
//    }
//    Iterator<KeyValuePair<IWindowed<K>, V>> iterator = results.iterator();

//    return new IKeyValueIterator<IWindowed<K>, V>()
//    {


//            public void Close() { }


//    public IWindowed<K> PeekNextKey()
//    {
//        throw new UnsupportedOperationException("PeekNextKey() not supported in " + getClass().getName());
//    }


//    public bool HasNext()
//    {
//        return iterator.HasNext();
//    }


//    public KeyValuePair<IWindowed<K>, V> next()
//    {
//        return iterator.MoveNext();
//    }



//    public void remove()
//    {
//        throw new UnsupportedOperationException("remove() not supported in " + getClass().getName());
//    }
//};
//    }

//     public IKeyValueIterator<IWindowed<K>, V> Fetch(K from,
//                                                            K to,
//                                                            Instant fromTime,
//                                                            Instant toTime)
//{// throws ArgumentException
//    return Fetch(
//        from,
//        to,
//        ApiUtils.validateMillisecondInstant(fromTime, prepareMillisCheckFailMsgPrefix(fromTime, "fromTime")),
//        ApiUtils.validateMillisecondInstant(toTime, prepareMillisCheckFailMsgPrefix(toTime, "toTime")));
//}

//public void Put(K key, V value, long timestamp)
//{
//    if (!data.ContainsKey(timestamp))
//    {
//        data.Put(timestamp, new TreeMap<>());
//    }
//    data.Get(timestamp).Put(key, value);
//}


//public string Name()
//{
//    return null;
//}


//public void Init(IProcessorContext context, IStateStore root) { }


//public void Flush() { }


//public void Close() { }


//public bool Persistent()
//{
//    return false;
//}


//public bool IsOpen()
//{
//    return open;
//}

//void SetOpen(bool open)
//{
//    this.open = open;
//}

//private class TheWindowStoreIterator<E> : IWindowStoreIterator<E>
//{

//    private Iterator<KeyValuePair<long, E>> underlying;

//    public KeyValuePair<long, E> Current { get; }
//    object? IEnumerator.Current { get; }

//    TheWindowStoreIterator(Iterator<KeyValuePair<long, E>> underlying)
//    {
//        this.underlying = underlying;
//    }


//    public void Close() { }


//    public long PeekNextKey()
//    {
//        throw new UnsupportedOperationException("PeekNextKey() not supported in " + getClass().getName());
//    }


//    public bool HasNext()
//    {
//        return underlying.HasNext();
//    }


//    public KeyValuePair<long, E> Next()
//    {
//        return underlying.MoveNext();
//    }


//    public void Remove()
//    {
//        throw new UnsupportedOperationException("remove() not supported in " + getClass().getName());
//    }

//    public bool MoveNext()
//    {
//        throw new System.NotImplementedException();
//    }

//    public void Reset()
//    {
//        throw new System.NotImplementedException();
//    }

//    public void Dispose()
//    {
//        throw new System.NotImplementedException();
//    }
//}
//}
//}
