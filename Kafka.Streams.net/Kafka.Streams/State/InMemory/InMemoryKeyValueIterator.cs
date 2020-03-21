
//using Kafka.Common.Utils;
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.State.Internals
//{
//    public class InMemoryKeyValueIterator : IKeyValueIterator<Bytes, byte[]>
//    {
//        private IEnumerator<KeyValuePair<Bytes, byte[]>> iter;

//        private InMemoryKeyValueIterator(IEnumerator<KeyValuePair<Bytes, byte[]>> iter)
//        {
//            this.iter = iter;
//        }


//        public bool hasNext()
//        {
//            return iter.hasNext();
//        }


//        public KeyValuePair<Bytes, byte[]> next()
//        {
//            KeyValuePair<Bytes, byte[]> entry = iter.next();
//            return new KeyValuePair<>(entry.Key, entry.Value);
//        }


//        public void Remove()
//        {
//            iter.Remove();
//        }


//        public void close()
//        {
//            // do nothing
//        }


//        public Bytes peekNextKey()
//        {
//            throw new InvalidOperationException("peekNextKey() not supported in " + GetType().getName());
//        }
//    }
//}
