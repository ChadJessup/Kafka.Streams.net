﻿//using Kafka.Streams.State;
//using Kafka.Streams.Processor.Interfaces;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KStreamReduceValueGetter : IKTableValueGetter<K, V>
//    {
//        private ITimestampedKeyValueStore<K, V> store;



//        public void init(IProcessorContext context)
//        {
//            store = (ITimestampedKeyValueStore<K, V>)context.getStateStore(storeName);
//        }


//        public ValueAndTimestamp<V> get(K key)
//        {
//            return store[key];
//        }


//        public void close() { }
//    }
//}