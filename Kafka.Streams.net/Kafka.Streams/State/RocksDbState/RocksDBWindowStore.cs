
//using Kafka.Common.Utils;
//using Kafka.Streams.KStream;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.State.Internals
//{

//    public class RocksDbWindowStore
//        : WrappedStateStore<ISegmentedBytesStore, object, object>,
//     IWindowStore<Bytes, byte[]>
//    {
//        private bool retainDuplicates;
//        private long windowSize;

//        private IProcessorContext<Bytes, byte[]> context;
//        private int seqnum = 0;

//        public RocksDbWindowStore(ISegmentedBytesStore bytesStore,
//                           bool retainDuplicates,
//                           long windowSize)
//            : base(bytesStore)
//        {
//            this.retainDuplicates = retainDuplicates;
//            this.windowSize = windowSize;
//        }

//        public void init(IProcessorContext<Bytes, byte[]> context, IStateStore root)
//        {
//            this.context = context;
//            base.Init(context, root);
//        }

//        public void put(Bytes key, byte[] value)
//        {
//            put(key, value, context.timestamp());
//        }

//        public void put(Bytes key, byte[] value, long windowStartTimestamp)
//        {
//            maybeUpdateSeqnumForDups();

//            wrapped.Add(WindowKeySchema.toStoreKeyBinary(key, windowStartTimestamp, seqnum), value);
//        }

//        public byte[] fetch(Bytes key, long timestamp)
//        {
//            byte[] bytesValue = wrapped.Get(WindowKeySchema.toStoreKeyBinary(key, timestamp, seqnum));
//            if (bytesValue == null)
//            {
//                return null;
//            }
//            return bytesValue;
//        }


//        public IWindowStoreIterator<byte[]> fetch(Bytes key, long timeFrom, long timeTo)
//        {
//            IKeyValueIterator<Bytes, byte[]> bytesIterator = wrapped.Fetch(key, timeFrom, timeTo);
//            return new WindowStoreIteratorWrapper(bytesIterator, windowSize).valuesIterator();
//        }


//        public IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from,
//                                                               Bytes to,
//                                                               long timeFrom,
//                                                               long timeTo)
//        {
//            IKeyValueIterator<Bytes, byte[]> bytesIterator = wrapped.Fetch(from, to, timeFrom, timeTo);
//            return new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
//        }

//        public IKeyValueIterator<Windowed<Bytes>, byte[]> all()
//        {
//            IKeyValueIterator<Bytes, byte[]> bytesIterator = wrapped.all();
//            return new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
//        }


//        public IKeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom, long timeTo)
//        {
//            IKeyValueIterator<Bytes, byte[]> bytesIterator = wrapped.fetchAll(timeFrom, timeTo);
//            return new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
//        }

//        private void maybeUpdateSeqnumForDups()
//        {
//            if (retainDuplicates)
//            {
//                seqnum = (seqnum + 1) & 0x7FFFFFFF;
//            }
//        }
//    }
//}