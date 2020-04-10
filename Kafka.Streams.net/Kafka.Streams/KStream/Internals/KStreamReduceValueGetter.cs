//using Kafka.Streams.State;
//using Kafka.Streams.Processors.Interfaces;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KStreamReduceValueGetter : IKTableValueGetter<K, V>
//    {
//        private ITimestampedKeyValueStore<K, V> store;



//        public void Init(IProcessorContext context)
//        {
//            store = (ITimestampedKeyValueStore<K, V>)context.getStateStore(storeName);
//        }


//        public ValueAndTimestamp<V> get(K key)
//        {
//            return store[key];
//        }


//        public void Close() { }
//    }
//}