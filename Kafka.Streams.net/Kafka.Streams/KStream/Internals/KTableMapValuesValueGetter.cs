

//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableMapValuesValueGetter : IKTableValueGetter<K, V1>
//    {
//        private IKTableValueGetter<K, V> parentGetter;

//        KTableMapValuesValueGetter(IKTableValueGetter<K, V> parentGetter)
//        {
//            this.parentGetter = parentGetter;
//        }


//        public void init(IProcessorContext context)
//        {
//            parentGetter.init(context);
//        }


//        public ValueAndTimestamp<V1> get(K key)
//        {
//            return computeValueAndTimestamp(key, parentGetter[key]);
//        }


//        public void close()
//        {
//            parentGetter.close();
//        }
//    }
//}