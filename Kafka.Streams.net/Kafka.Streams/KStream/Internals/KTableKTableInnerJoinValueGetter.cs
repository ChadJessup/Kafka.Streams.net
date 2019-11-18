

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableKTableInnerJoinValueGetter : IKTableValueGetter<K, R>
//    {

//        private IKTableValueGetter<K, V1> valueGetter1;
//        private IKTableValueGetter<K, V2> valueGetter2;

//        KTableKTableInnerJoinValueGetter(IKTableValueGetter<K, V1> valueGetter1,
//                                          IKTableValueGetter<K, V2> valueGetter2)
//        {
//            this.valueGetter1 = valueGetter1;
//            this.valueGetter2 = valueGetter2;
//        }


//        public void init(IProcessorContext context)
//        {
//            valueGetter1.init(context);
//            valueGetter2.init(context);
//        }


//        public ValueAndTimestamp<R> get(K key)
//        {
//            ValueAndTimestamp<V1> valueAndTimestamp1 = valueGetter1[key];
//            V1 value1 = getValueOrNull(valueAndTimestamp1);

//            if (value1 != null)
//            {
//                ValueAndTimestamp<V2> valueAndTimestamp2 = valueGetter2[keyValueMapper.apply(key, value1)];
//                V2 value2 = getValueOrNull(valueAndTimestamp2);

//                if (value2 != null)
//                {
//                    return ValueAndTimestamp.make(
//                        joiner.apply(value1, value2),
//                        Math.Max(valueAndTimestamp1.timestamp(), valueAndTimestamp2.timestamp()));
//                }
//                else
//                {

//                    return null;
//                }
//            }
//            else
//            {

//                return null;
//            }
//        }


//        public void close()
//        {
//            valueGetter1.close();
//            valueGetter2.close();
//        }
//    }
//}
