
//using Kafka.Common.Utils;
//using Microsoft.Extensions.Logging;
//using System.Runtime.CompilerServices;

//namespace Kafka.Streams.State.Internals
//{

//    public class MemoryNavigableLRUCache : MemoryLRUCache
//    {

//        private static ILogger LOG = new LoggerFactory().CreateLogger<MemoryNavigableLRUCache>();

//        public MemoryNavigableLRUCache(string Name, int maxCacheSize)
//            : base(Name, maxCacheSize)
//        {
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

//            TreeMap<Bytes, byte[]> treeMap = toTreeMap();
//            return new DelegatingPeekingKeyValueIterator<>(Name,
//                new MemoryNavigableLRUCache.CacheIterator(treeMap.navigableKeySet()
//                    .subSet(from, true, to, true).iterator(), treeMap));
//        }

//        public override IKeyValueIterator<Bytes, byte[]> All()
//        {
//            TreeMap<Bytes, byte[]> treeMap = toTreeMap();
//            return new MemoryNavigableLRUCache.CacheIterator(treeMap.navigableKeySet().iterator(), treeMap);
//        }

//        [MethodImpl(MethodImplOptions.Synchronized)]
//        private TreeMap<Bytes, byte[]> toTreeMap()
//        {
//            return new TreeMap<>(this.map);
//        }
//    }
//}
