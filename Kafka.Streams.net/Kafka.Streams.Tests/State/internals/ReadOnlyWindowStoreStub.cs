/*






 *

 *





 */

























/**
 * A very simple window store stub for testing purposes.
 */
public class ReadOnlyWindowStoreStub<K, V> : ReadOnlyWindowStore<K, V>, StateStore {

    private readonly long windowSize;
    private Dictionary<long, NavigableMap<K, V>> data = new HashMap<>();
    private readonly bool open  = true;

    ReadOnlyWindowStoreStub(long windowSize) {
        this.windowSize = windowSize;
    }

    
    public V Fetch(K key, long time) {
        Dictionary<K, V> kvMap = data.get(time);
        if (kvMap != null) {
            return kvMap.get(key);
        } else {
            return null;
        }
    }

    
    
    public WindowStoreIterator<V> Fetch(K key, long timeFrom, long timeTo) {
        if (!open) {
            throw new InvalidStateStoreException("Store is not open");
        }
        List<KeyValuePair<long, V>> results = new ArrayList<>();
        for (long now = timeFrom; now <= timeTo; now++) {
            Dictionary<K, V> kvMap = data.get(now);
            if (kvMap != null && kvMap.containsKey(key)) {
                results.add(new KeyValuePair<>(now, kvMap.get(key)));
            }
        }
        return new TheWindowStoreIterator<>(results.iterator());
    }

    
    public WindowStoreIterator<V> Fetch(K key, Instant from, Instant to) {// throws IllegalArgumentException
        return fetch(
            key, 
            ApiUtils.validateMillisecondInstant(from, prepareMillisCheckFailMsgPrefix(from, "from")),
            ApiUtils.validateMillisecondInstant(to, prepareMillisCheckFailMsgPrefix(to, "to")));
    }

    
    public KeyValueIterator<Windowed<K>, V> All() {
        if (!open) {
            throw new InvalidStateStoreException("Store is not open");
        }
        List<KeyValuePair<Windowed<K>, V>> results = new ArrayList<>();
        foreach (long now in data.keySet()) {
            NavigableDictionary<K, V> kvMap = data.get(now);
            if (kvMap != null) {
                foreach (Entry<K, V> entry in kvMap.entrySet()) {
                    results.add(new KeyValuePair<>(new Windowed<>(entry.getKey(), new TimeWindow(now, now + windowSize)), entry.getValue()));
                }
            }
        }
        Iterator<KeyValuePair<Windowed<K>, V>> iterator = results.iterator();

        return new KeyValueIterator<Windowed<K>, V>() {
            
            public void close() {}

            
            public Windowed<K> peekNextKey() {
                throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
            }

            
            public bool hasNext() {
                return iterator.hasNext();
            }

            
            public KeyValuePair<Windowed<K>, V> next() {
                return iterator.next();
            }


            
            public void remove() {
                throw new UnsupportedOperationException("remove() not supported in " + getClass().getName());
            }
        };
    }

    
    
    public KeyValueIterator<Windowed<K>, V> FetchAll(long timeFrom, long timeTo) {
        if (!open) {
            throw new InvalidStateStoreException("Store is not open");
        }
        List<KeyValuePair<Windowed<K>, V>> results = new ArrayList<>();
        foreach (long now in data.keySet()) {
            if (!(now >= timeFrom && now <= timeTo)) {
                continue;
            }
            NavigableDictionary<K, V> kvMap = data.get(now);
            if (kvMap != null) {
                foreach (Entry<K, V> entry in kvMap.entrySet()) {
                    results.add(new KeyValuePair<>(new Windowed<>(entry.getKey(), new TimeWindow(now, now + windowSize)), entry.getValue()));
                }
            }
        }
        Iterator<KeyValuePair<Windowed<K>, V>> iterator = results.iterator();

        return new KeyValueIterator<Windowed<K>, V>() {
            
            public void close() {}

            
            public Windowed<K> peekNextKey() {
                throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
            }

            
            public bool hasNext() {
                return iterator.hasNext();
            }

            
            public KeyValuePair<Windowed<K>, V> next() {
                return iterator.next();
            }


            
            public void remove() {
                throw new UnsupportedOperationException("remove() not supported in " + getClass().getName());
            }
        };
    }

    
    public KeyValueIterator<Windowed<K>, V> FetchAll(Instant from, Instant to) {// throws IllegalArgumentException
        return fetchAll(
            ApiUtils.validateMillisecondInstant(from, prepareMillisCheckFailMsgPrefix(from, "from")),
            ApiUtils.validateMillisecondInstant(to, prepareMillisCheckFailMsgPrefix(to, "to")));
    }

    
    
    public KeyValueIterator<Windowed<K>, V> Fetch(K from, K to, long timeFrom, long timeTo) {
        if (!open) {
            throw new InvalidStateStoreException("Store is not open");
        }
        List<KeyValuePair<Windowed<K>, V>> results = new ArrayList<>();
        for (long now = timeFrom; now <= timeTo; now++) {
            NavigableDictionary<K, V> kvMap = data.get(now);
            if (kvMap != null) {
                foreach (Entry<K, V> entry in kvMap.subMap(from, true, to, true).entrySet()) {
                    results.add(new KeyValuePair<>(new Windowed<>(entry.getKey(), new TimeWindow(now, now + windowSize)), entry.getValue()));
                }
            }
        }
        Iterator<KeyValuePair<Windowed<K>, V>> iterator = results.iterator();

        return new KeyValueIterator<Windowed<K>, V>() {
            
            public void close() {}

            
            public Windowed<K> peekNextKey() {
                throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
            }

            
            public bool hasNext() {
                return iterator.hasNext();
            }

            
            public KeyValuePair<Windowed<K>, V> next() {
                return iterator.next();
            }


            
            public void remove() {
                throw new UnsupportedOperationException("remove() not supported in " + getClass().getName());
            }
        };
    }

     public KeyValueIterator<Windowed<K>, V> Fetch(K from,
                                                            K to,
                                                            Instant fromTime,
                                                            Instant toTime) {// throws IllegalArgumentException
        return fetch(
            from,
            to, 
            ApiUtils.validateMillisecondInstant(fromTime, prepareMillisCheckFailMsgPrefix(fromTime, "fromTime")),
            ApiUtils.validateMillisecondInstant(toTime, prepareMillisCheckFailMsgPrefix(toTime, "toTime")));
    }

    public void Put(K key, V value, long timestamp) {
        if (!data.containsKey(timestamp)) {
            data.put(timestamp, new TreeMap<>());
        }
        data.get(timestamp).put(key, value);
    }

    
    public string Name() {
        return null;
    }

    
    public void Init(ProcessorContext context, StateStore root) {}

    
    public void Flush() {}

    
    public void Close() {}

    
    public bool Persistent() {
        return false;
    }

    
    public bool IsOpen() {
        return open;
    }

    void SetOpen(bool open) {
        this.open = open;
    }

    private class TheWindowStoreIterator<E> : WindowStoreIterator<E> {

        private Iterator<KeyValuePair<long, E>> underlying;

        TheWindowStoreIterator(Iterator<KeyValuePair<long, E>> underlying) {
            this.underlying = underlying;
        }

        
        public void Close() {}

        
        public long PeekNextKey() {
            throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
        }

        
        public bool HasNext() {
            return underlying.hasNext();
        }

        
        public KeyValuePair<long, E> Next() {
            return underlying.next();
        }

        
        public void Remove() {
            throw new UnsupportedOperationException("remove() not supported in " + getClass().getName());
        }
    }
}
