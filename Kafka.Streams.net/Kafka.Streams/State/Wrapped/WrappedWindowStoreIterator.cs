
//using Kafka.Common.Utils;
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.State.Internals
//{
//    public class WrappedWindowStoreIterator : IWindowStoreIterator<byte[]>
//    {
//        IKeyValueIterator<Bytes, byte[]> bytesIterator;

//        public WrappedWindowStoreIterator(
//            IKeyValueIterator<Bytes, byte[]> bytesIterator)
//        {
//            this.bytesIterator = bytesIterator;
//        }


//        public long peekNextKey()
//        {
//            return WindowKeySchema.extractStoreTimestamp(bytesIterator.peekNextKey().Get());
//        }


//        public bool hasNext()
//        {
//            return bytesIterator.hasNext();
//        }


//        public KeyValuePair<long, byte[]> next()
//        {
//            KeyValuePair<Bytes, byte[]> next = bytesIterator.MoveNext();
//            long timestamp = WindowKeySchema.extractStoreTimestamp(next.key());
//            return KeyValuePair.pair(timestamp, next.value);
//        }


//        public void Remove()
//        {
//            throw new InvalidOperationException("Remove() is not supported in " + GetType().getName());
//        }


//        public void close()
//        {
//            bytesIterator.close();
//        }
//    }
//}
