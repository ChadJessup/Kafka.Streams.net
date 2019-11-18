
//    public class KeyValueToTimestampedKeyValueByteStoreAdapter : IKeyValueStore<Bytes, byte[]>
//    {
//        IKeyValueStore<Bytes, byte[]> store;

//        KeyValueToTimestampedKeyValueByteStoreAdapter(IKeyValueStore<Bytes, byte[]> store)
//        {
//            if (!store.persistent())
//            {
//                throw new System.ArgumentException("Provided store must be a persistent store, but it is not.");
//            }
//            this.store = store;
//        }

//        public void put(Bytes key,
//                        byte[] valueWithTimestamp)
//        {
//            store.Add(key, valueWithTimestamp == null
//                ? null
//                : rawValue(valueWithTimestamp));
//        }

//        public byte[] putIfAbsent(Bytes key,
//                                  byte[] valueWithTimestamp)
//        {
//            return convertToTimestampedFormat(store.putIfAbsent(
//                key,
//                valueWithTimestamp == null
//                ? null
//                : rawValue(valueWithTimestamp)));
//        }

//        public void putAll(List<KeyValue<Bytes, byte[]>> entries)
//        {
//            foreach (KeyValue<Bytes, byte[]> entry in entries)
//            {
//                byte[] valueWithTimestamp = entry.value;
//                store.Add(entry.key, valueWithTimestamp == null ? null : rawValue(valueWithTimestamp));
//            }
//        }

//        public byte[] delete(Bytes key)
//        {
//            return convertToTimestampedFormat(store.delete(key));
//        }

//        public void init(IProcessorContext<Bytes, byte[]> context,
//                         IStateStore root)
//        {
//            store.init(context, root);
//        }

//        public void flush()
//        {
//            store.flush();
//        }

//        public void close()
//        {
//            store.close();
//        }

//        public bool persistent()
//        {
//            return true;
//        }

//        public bool isOpen()
//        {
//            return store.isOpen();
//        }

//        public byte[] get(Bytes key)
//        {
//            return convertToTimestampedFormat(store[key]);
//        }

//        public IKeyValueIterator<Bytes, byte[]> range(Bytes from,
//                                                     Bytes to)
//        {
//            return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.range(from, to));
//        }

//        public IKeyValueIterator<Bytes, byte[]> all()
//        {
//            return new KeyValueToTimestampedKeyValueIteratorAdapter<Bytes, byte[]>(store.all());
//        }

//        public override long approximateNumEntries()
//        {
//            return store.approximateNumEntries();
//        }

//    }
//}