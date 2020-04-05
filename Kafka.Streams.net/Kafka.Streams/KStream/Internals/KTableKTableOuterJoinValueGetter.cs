
//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableKTableOuterJoinValueGetter : IKTableValueGetter<K, R>
//    {

//        private IKTableValueGetter<K, V1> valueGetter1;
//        private IKTableValueGetter<K, V2> valueGetter2;

//        KTableKTableOuterJoinValueGetter(IKTableValueGetter<K, V1> valueGetter1,
//                                          IKTableValueGetter<K, V2> valueGetter2)
//        {
//            this.valueGetter1 = valueGetter1;
//            this.valueGetter2 = valueGetter2;
//        }


//        public void init(IProcessorContext context)
//        {
//            valueGetter1.Init(context);
//            valueGetter2.Init(context);
//        }


//        public ValueAndTimestamp<R> get(K key)
//        {
//            R newValue = null;

//            ValueAndTimestamp<V1> valueAndTimestamp1 = valueGetter1[key];
//            V1 value1;
//            long timestamp1;
//            if (valueAndTimestamp1 == null)
//            {
//                value1 = null;
//                timestamp1 = UNKNOWN;
//            }
//            else
//            {

//                value1 = valueAndTimestamp1.value();
//                timestamp1 = valueAndTimestamp1.timestamp();
//            }

//            ValueAndTimestamp<V2> valueAndTimestamp2 = valueGetter2[key];
//            V2 value2;
//            long timestamp2;
//            if (valueAndTimestamp2 == null)
//            {
//                value2 = null;
//                timestamp2 = UNKNOWN;
//            }
//            else
//            {

//                value2 = valueAndTimestamp2.value();
//                timestamp2 = valueAndTimestamp2.timestamp();
//            }

//            if (value1 != null || value2 != null)
//            {
//                newValue = joiner.apply(value1, value2);
//            }

//            return ValueAndTimestamp.Make(newValue, Math.Max(timestamp1, timestamp2));
//        }


//        public void close()
//        {
//            valueGetter1.close();
//            valueGetter2.close();
//        }
//    }
//}
