
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
//            return WindowKeySchema.extractStoreTimestamp(bytesIterator.peekNextKey().get());
//        }


//        public bool hasNext()
//        {
//            return bytesIterator.hasNext();
//        }


//        public KeyValue<long, byte[]> next()
//        {
//            KeyValue<Bytes, byte[]> next = bytesIterator.next();
//            long timestamp = WindowKeySchema.extractStoreTimestamp(next.key());
//            return KeyValue.pair(timestamp, next.value);
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
