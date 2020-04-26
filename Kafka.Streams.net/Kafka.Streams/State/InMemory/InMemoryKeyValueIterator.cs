
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


//        public bool HasNext()
//        {
//            return iter.MoveNext();
//        }


//        public KeyValuePair<Bytes, byte[]> next()
//        {
//            KeyValuePair<Bytes, byte[]> entry = iter.MoveNext();
//            return KeyValuePair.Create(entry.Key, entry.Value);
//        }


//        public void Remove()
//        {
//            iter.Remove();
//        }


//        public void Close()
//        {
//            // do nothing
//        }


//        public Bytes PeekNextKey()
//        {
//            throw new InvalidOperationException("PeekNextKey() not supported in " + GetType().getName());
//        }
//    }
//}
