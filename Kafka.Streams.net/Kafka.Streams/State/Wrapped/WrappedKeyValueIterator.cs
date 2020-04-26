
//using Kafka.Common.Utils;
//using Kafka.Streams.KStream;
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.State.Internals
//{
//    public class WrappedKeyValueIterator : IKeyValueIterator<IWindowed<Bytes>, byte[]>
//    {
//        IKeyValueIterator<Bytes, byte[]> bytesIterator;
//        long windowSize;

//        public WrappedKeyValueIterator(IKeyValueIterator<Bytes, byte[]> bytesIterator,
//                                long windowSize)
//        {
//            this.bytesIterator = bytesIterator;
//            this.windowSize = windowSize;
//        }


//        public IWindowed<Bytes> PeekNextKey()
//        {
//            byte[] nextKey = bytesIterator.PeekNextKey().Get();
//            return WindowKeySchema.fromStoreBytesKey(nextKey, windowSize);
//        }


//        public bool HasNext()
//        {
//            return bytesIterator.MoveNext();
//        }


//        public KeyValuePair<IWindowed<Bytes>, byte[]> next()
//        {
//            KeyValuePair<Bytes, byte[]> next = bytesIterator.MoveNext();
//            return KeyValuePair.pair(WindowKeySchema.fromStoreBytesKey(next.key(), windowSize), next.value);
//        }


//        public void Remove()
//        {
//            throw new InvalidOperationException("Remove() is not supported in " + GetType().getName());
//        }


//        public void Close()
//        {
//            bytesIterator.Close();
//        }
//    }
//}
