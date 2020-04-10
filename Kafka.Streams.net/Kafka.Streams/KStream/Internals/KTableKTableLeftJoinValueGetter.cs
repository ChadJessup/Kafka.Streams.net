﻿

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableKTableLeftJoinValueGetter : IKTableValueGetter<K, R>
//    {

//        private IKTableValueGetter<K, V1> valueGetter1;
//        private IKTableValueGetter<K, V2> valueGetter2;

//        KTableKTableLeftJoinValueGetter(IKTableValueGetter<K, V1> valueGetter1,
//                                         IKTableValueGetter<K, V2> valueGetter2)
//        {
//            this.valueGetter1 = valueGetter1;
//            this.valueGetter2 = valueGetter2;
//        }


//        public void Init(IProcessorContext context)
//        {
//            valueGetter1.Init(context);
//            valueGetter2.Init(context);
//        }


//        public ValueAndTimestamp<R> get(K key)
//        {
//            ValueAndTimestamp<V1> valueAndTimestamp1 = valueGetter1[key];
//            V1 value1 = ValueAndTimestamp.GetValueOrNull(valueAndTimestamp1);

//            if (value1 != null)
//            {
//                ValueAndTimestamp<V2> valueAndTimestamp2 = valueGetter2[key];
//                V2 value2 = ValueAndTimestamp.GetValueOrNull(valueAndTimestamp2);
//                long resultTimestamp;
//                if (valueAndTimestamp2 == null)
//                {
//                    resultTimestamp = valueAndTimestamp1.timestamp();
//                }
//                else
//                {

//                    resultTimestamp = Math.Max(valueAndTimestamp1.timestamp(), valueAndTimestamp2.timestamp());
//                }
//                return ValueAndTimestamp.Make(joiner.apply(value1, value2), resultTimestamp);
//            }
//            else
//            {

//                return null;
//            }
//        }


//        public void Close()
//        {
//            valueGetter1.Close();
//            valueGetter2.Close();
//        }
//    }
//}
