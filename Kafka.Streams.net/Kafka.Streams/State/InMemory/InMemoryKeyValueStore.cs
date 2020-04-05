
//using Kafka.Common.Utils;
//using Kafka.Streams.Processors.Interfaces;
//using Microsoft.Extensions.Logging;

//namespace Kafka.Streams.State.Internals
//{
//    public class InMemoryKeyValueStore<K, V> : IKeyValueStore<Bytes, byte[]>
//    {
//        private string name;
//        private ConcurrentDictionary<Bytes, byte[]> map = new ConcurrentSkipListMap<>();
//        private volatile bool open = false;

//        private static ILogger LOG = new LoggerFactory().CreateLogger<InMemoryKeyValueStore>();

//        public InMemoryKeyValueStore(string name)
//        {
//            this.name = name;
//        }

//        public override string name { get; }

//        public override void init(IProcessorContext<K, V> context,
//                         IStateStore root)
//        {

//            if (root != null)
//            {
//                // register the store
//                context.register(root, (key, value) =>
//    {
//        // this is a delete
//        if (value == null)
//        {
//            delete(Bytes.Wrap(key));
//        }
//        else
//        {
//            put(Bytes.Wrap(key), value);
//        }
//    });
//            }

//            open = true;
//        }

//        public override bool persistent()
//        {
//            return false;
//        }

//        public override bool isOpen()
//        {
//            return open;
//        }

//        public override byte[] get(Bytes key)
//        {
//            return map[key];
//        }

//        public override void put(Bytes key, byte[] value)
//        {
//            if (value == null)
//            {
//                map.Remove(key);
//            }
//            else
//            {
//                map.Add(key, value);
//            }
//        }

//        public override byte[] putIfAbsent(Bytes key, byte[] value)
//        {
//            byte[] originalValue = get(key);
//            if (originalValue == null)
//            {
//                put(key, value);
//            }
//            return originalValue;
//        }

//        public override void putAll(List<KeyValuePair<Bytes, byte[]>> entries)
//        {
//            foreach (KeyValuePair<Bytes, byte[]> entry in entries)
//            {
//                put(entry.key, entry.value);
//            }
//        }

//        public override byte[] delete(Bytes key)
//        {
//            return map.Remove(key);
//        }

//        public override IKeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to)
//        {

//            if (from.CompareTo(to) > 0)
//            {
//                LOG.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. "
//                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
//                    "Note that the built-in numerical serdes do not follow this for negative numbers");
//                return KeyValueIterators.emptyIterator();
//            }

//            return new DelegatingPeekingKeyValueIterator<>(
//                name,
//                new InMemoryKeyValueIterator(map.subMap(from, true, to, true).iterator()));
//        }

//        public override IKeyValueIterator<Bytes, byte[]> all()
//        {
//            return new DelegatingPeekingKeyValueIterator<>(
//                name,
//                new InMemoryKeyValueIterator(map.iterator()));
//        }

//        public override long approximateNumEntries
//        {
//            return map.size();
//        }

//        public override void flush()
//        {
//            // do-nothing since it is in-memory
//        }

//        public override void close()
//        {
//            map.clear();
//            open = false;
//        }
//    }
//}