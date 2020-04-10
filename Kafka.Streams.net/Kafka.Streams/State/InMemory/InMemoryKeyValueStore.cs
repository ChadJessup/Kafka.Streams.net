
//using Kafka.Common.Utils;
//using Kafka.Streams.Processors.Interfaces;
//using Microsoft.Extensions.Logging;

//namespace Kafka.Streams.State.Internals
//{
//    public class InMemoryKeyValueStore<K, V> : IKeyValueStore<Bytes, byte[]>
//    {
//        private string Name;
//        private ConcurrentDictionary<Bytes, byte[]> map = new ConcurrentSkipListMap<>();
//        private volatile bool open = false;

//        private static ILogger LOG = new LoggerFactory().CreateLogger<InMemoryKeyValueStore>();

//        public InMemoryKeyValueStore(string Name)
//        {
//            this.Name = Name;
//        }

//        public override string Name { get; }

//        public override void Init(IProcessorContext context,
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
//            Put(Bytes.Wrap(key), value);
//        }
//    });
//            }

//            open = true;
//        }

//        public override bool Persistent()
//        {
//            return false;
//        }

//        public override bool IsOpen()
//        {
//            return open;
//        }

//        public override byte[] get(Bytes key)
//        {
//            return map[key];
//        }

//        public override void Put(Bytes key, byte[] value)
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
//                Put(key, value);
//            }
//            return originalValue;
//        }

//        public override void putAll(List<KeyValuePair<Bytes, byte[]>> entries)
//        {
//            foreach (KeyValuePair<Bytes, byte[]> entry in entries)
//            {
//                Put(entry.key, entry.value);
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
//                LOG.LogWarning("Returning empty iterator for Fetch with invalid key range: from > to. "
//                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
//                    "Note that the built-in numerical serdes do not follow this for negative numbers");
//                return KeyValueIterators.emptyIterator();
//            }

//            return new DelegatingPeekingKeyValueIterator<>(
//                Name,
//                new InMemoryKeyValueIterator(map.subMap(from, true, to, true).iterator()));
//        }

//        public override IKeyValueIterator<Bytes, byte[]> All()
//        {
//            return new DelegatingPeekingKeyValueIterator<>(
//                Name,
//                new InMemoryKeyValueIterator(map.iterator()));
//        }

//        public override long approximateNumEntries
//        {
//            return map.size();
//        }

//        public override void Flush()
//        {
//            // do-nothing since it is in-memory
//        }

//        public override void Close()
//        {
//            map.clear();
//            open = false;
//        }
//    }
//}