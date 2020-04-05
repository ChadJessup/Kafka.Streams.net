
//using Kafka.Common.Utils;
//using Kafka.Streams.KStream;
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.State.Windowed
//{
//    public class WindowStoreIteratorWrapper
//    {
//        private IKeyValueIterator<Bytes, byte[]> bytesIterator;
//        private long windowSize;

//        public WindowStoreIteratorWrapper(IKeyValueIterator<Bytes, byte[]> bytesIterator,
//                                   long windowSize)
//        {
//            this.bytesIterator = bytesIterator;
//            this.windowSize = windowSize;
//        }

//        public IWindowStoreIterator<byte[]> valuesIterator()
//        {
//            return new WrappedWindowStoreIterator(bytesIterator);
//        }

//        public IKeyValueIterator<Windowed<Bytes>, byte[]> keyValueIterator()
//        {
//            return null; // new WrappedKeyValueIterator(bytesIterator, windowSize);
//        }
//    }
//}
