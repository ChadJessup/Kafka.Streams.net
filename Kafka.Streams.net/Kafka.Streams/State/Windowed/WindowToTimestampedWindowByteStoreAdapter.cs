
//using Kafka.Common.Utils;
//using Kafka.Streams.KStream;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State.Interfaces;
//using System;

//namespace Kafka.Streams.State.Windowed
//{
//    public class WindowToTimestampedWindowByteStoreAdapter : IWindowStore<Bytes, byte[]>
//    {
//        IWindowStore<Bytes, byte[]> store;

//        public WindowToTimestampedWindowByteStoreAdapter(IWindowStore<Bytes, byte[]> store)
//        {
//            if (!store.persistent())
//            {
//                throw new System.ArgumentException("Provided store must be a persistent store, but it is not.");
//            }
//            this.store = store;
//        }

//        public override void put(Bytes key,
//                        byte[] valueWithTimestamp)
//        {
//            store.Add(key, valueWithTimestamp == null ? null : RawValue(valueWithTimestamp));
//        }

//        public override void put(Bytes key,
//                        byte[] valueWithTimestamp,
//                        long windowStartTimestamp)
//        {
//            store.Add(key, valueWithTimestamp == null ? null : RawValue(valueWithTimestamp), windowStartTimestamp);
//        }

//        public override byte[] fetch(Bytes key,
//                            long time)
//        {
//            return convertToTimestampedFormat(store.Fetch(key, time));
//        }



//        public IWindowStoreIterator<byte[]> fetch(Bytes key,
//                                                 long timeFrom,
//                                                 long timeTo)
//        {
//            return new WindowToTimestampedWindowIteratorAdapter(store.Fetch(key, timeFrom, timeTo));
//        }

//        public override IWindowStoreIterator<byte[]> fetch(Bytes key,
//                                                 DateTime from,
//                                                 DateTime to)
//        {
//            return new WindowToTimestampedWindowIteratorAdapter(store.Fetch(key, from, to));
//        }



//        public IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from,
//                                                               Bytes to,
//                                                               long timeFrom,
//                                                               long timeTo)
//        {
//            return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.Fetch(from, to, timeFrom, timeTo));
//        }

//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from,
//                                                               Bytes to,
//                                                               DateTime fromTime,
//                                                               DateTime toTime)
//        {
//            return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.Fetch(from, to, fromTime, toTime));
//        }

//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> all()
//        {
//            return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.all());
//        }



//        public IKeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom,
//                                                                  long timeTo)
//        {
//            return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.fetchAll(timeFrom, timeTo));
//        }

//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(DateTime from,
//                                                                  DateTime to)
//        {
//            return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.fetchAll(from, to));
//        }

//        public string name => store.name;

//        public override void init(IProcessorContext<K, V> context,
//                         IStateStore root)
//        {
//            store.Init(context, root);
//        }

//        public override void flush()
//        {
//            store.flush();
//        }

//        public override void close()
//        {
//            store.close();
//        }

//        public override bool persistent()
//        {
//            return true;
//        }

//        public override bool isOpen()
//        {
//            return store.isOpen();
//        }
//    }
//}