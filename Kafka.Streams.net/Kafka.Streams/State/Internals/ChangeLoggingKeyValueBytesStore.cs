
//using Kafka.Common.Utils;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Internals
//{
//    public class ChangeLoggingKeyValueBytesStore
//        : WrappedStateStore<IKeyValueStore<Bytes, byte[]>, byte[], byte[]>
//    , IKeyValueStore<Bytes, byte[]>
//    {

//        public StoreChangeLogger<Bytes, byte[]> changeLogger { get; }

//        public ChangeLoggingKeyValueBytesStore(IKeyValueStore<Bytes, byte[]> inner)
//            : base(inner)
//        {
//        }

//        public override void init(IProcessorContext<K, V> context,
//                         IStateStore root)
//        {
//            base.init(context, root);
//            string topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), name);
//            changeLogger = new StoreChangeLogger<>(
//                name,
//                context,
//                new StateSerdes<>(topic, Serdes.Bytes(), Serdes.ByteArray()));

//            // if the inner store is an LRU cache,.Add the eviction listener to log removed record
//            if (wrapped is MemoryLRUCache)
//            {
//                ((MemoryLRUCache)wrapped).setWhenEldestRemoved((key, value)=>
//    {
//                    // pass null to indicate removal
//                    log(key, null);
//                });
//            }
//        }

//        public override long approximateNumEntries()
//        {
//            return wrapped.approximateNumEntries();
//        }

//        public override void put(Bytes key,
//                        byte[] value)
//        {
//            wrapped.Add(key, value);
//            log(key, value);
//        }

//        public override byte[] putIfAbsent(Bytes key,
//                                  byte[] value)
//        {
//            byte[] previous = wrapped.putIfAbsent(key, value);
//            if (previous == null)
//            {
//                // then it was absent
//                log(key, value);
//            }
//            return previous;
//        }

//        public override void putAll(List<KeyValue<Bytes, byte[]>> entries)
//        {
//            wrapped.putAll(entries);
//            foreach (KeyValue<Bytes, byte[]> entry in entries)
//            {
//                log(entry.key, entry.value);
//            }
//        }

//        public override byte[] delete(Bytes key)
//        {
//            byte[] oldValue = wrapped.delete(key);
//            log(key, null);
//            return oldValue;
//        }

//        public override byte[] get(Bytes key)
//        {
//            return wrapped[key];
//        }

//        public override IKeyValueIterator<Bytes, byte[]> range(Bytes from,
//                                                     Bytes to)
//        {
//            return wrapped.range(from, to);
//        }

//        public override IKeyValueIterator<Bytes, byte[]> all()
//        {
//            return wrapped.all();
//        }

//        void log(Bytes key,
//                 byte[] value)
//        {
//            changeLogger.logChange(key, value);
//        }
//    }
//}