
//using Kafka.Common.Utils;
//using Kafka.Streams.KStream;
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.State.Internals
//{
//    public class WrappedKeyValueIterator : IKeyValueIterator<Windowed<Bytes>, byte[]>
//    {
//        IKeyValueIterator<Bytes, byte[]> bytesIterator;
//        long windowSize;

//        public WrappedKeyValueIterator(IKeyValueIterator<Bytes, byte[]> bytesIterator,
//                                long windowSize)
//        {
//            this.bytesIterator = bytesIterator;
//            this.windowSize = windowSize;
//        }


//        public Windowed<Bytes> peekNextKey()
//        {
//            byte[] nextKey = bytesIterator.peekNextKey().Get();
//            return WindowKeySchema.fromStoreBytesKey(nextKey, windowSize);
//        }


//        public bool hasNext()
//        {
//            return bytesIterator.hasNext();
//        }


//        public KeyValuePair<Windowed<Bytes>, byte[]> next()
//        {
//            KeyValuePair<Bytes, byte[]> next = bytesIterator.MoveNext();
//            return KeyValuePair.pair(WindowKeySchema.fromStoreBytesKey(next.key(), windowSize), next.value);
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
