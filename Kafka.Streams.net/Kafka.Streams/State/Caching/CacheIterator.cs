
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


//        public bool HasNext()
//        {
//            return keys.HasNext();
//        }


//        public KeyValuePair<Bytes, byte[]> next()
//        {
//            lastKey = keys.MoveNext();
//            return KeyValuePair.Create(lastKey, entries[lastKey]);
//        }


//        public void Remove()
//        {
//            // do nothing
//        }


//        public void close()
//        {
//            // do nothing
//        }


//        public Bytes PeekNextKey()
//        {
//            throw new InvalidOperationException("peekNextKey not supported");
//        }
//    }
//}
