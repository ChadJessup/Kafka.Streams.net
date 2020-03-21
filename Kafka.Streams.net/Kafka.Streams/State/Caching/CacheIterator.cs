
//using Kafka.Common.Utils;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Internals
//{
//    public class CacheIterator : IKeyValueIterator<Bytes, byte[]>
//    {
//        private IEnumerator<Bytes> keys;
//        private Dictionary<Bytes, byte[]> entries;
//        private Bytes lastKey;

//        private CacheIterator(IEnumerator<Bytes> keys, Dictionary<Bytes, byte[]> entries)
//        {
//            this.keys = keys;
//            this.entries = entries;
//        }


//        public bool hasNext()
//        {
//            return keys.hasNext();
//        }


//        public KeyValuePair<Bytes, byte[]> next()
//        {
//            lastKey = keys.next();
//            return new KeyValuePair<>(lastKey, entries[lastKey]);
//        }


//        public void Remove()
//        {
//            // do nothing
//        }


//        public void close()
//        {
//            // do nothing
//        }


//        public Bytes peekNextKey()
//        {
//            throw new InvalidOperationException("peekNextKey not supported");
//        }
//    }
//}
