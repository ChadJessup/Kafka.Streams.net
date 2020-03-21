﻿

//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State;
//using System;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableKTableRightJoinValueGetter<K, R, V1, V2> : IKTableValueGetter<K, R>
//    {

//        private IKTableValueGetter<K, V1> valueGetter1;
//        private IKTableValueGetter<K, V2> valueGetter2;

//        KTableKTableRightJoinValueGetter(IKTableValueGetter<K, V1> valueGetter1,
//                                          IKTableValueGetter<K, V2> valueGetter2)
//        {
//            this.valueGetter1 = valueGetter1;
//            this.valueGetter2 = valueGetter2;
//        }


//        public void init(IProcessorContext<K, V1> context)
//        {
//            valueGetter1.init(context);
//            valueGetter2.init(context);
//        }


//        public ValueAndTimestamp<R> get(K key)
//        {
//            ValueAndTimestamp<V2> valueAndTimestamp2 = valueGetter2.get(key);
//            V2 value2 = ValueAndTimestamp.GetValueOrNull(valueAndTimestamp2);

//            if (value2 != null)
//            {
//                ValueAndTimestamp<V1> valueAndTimestamp1 = valueGetter1.get(key);
//                V1 value1 = ValueAndTimestamp.GetValueOrNull(valueAndTimestamp1);
//                long resultTimestamp;
//                if (valueAndTimestamp1 == null)
//                {
//                    resultTimestamp = valueAndTimestamp2.timestamp;
//                }
//                else
//                {

//                    resultTimestamp = Math.Max(valueAndTimestamp1.timestamp, valueAndTimestamp2.timestamp);
//                }
//                return ValueAndTimestamp<V1>.make(joiner.apply(value1, value2), resultTimestamp);
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
