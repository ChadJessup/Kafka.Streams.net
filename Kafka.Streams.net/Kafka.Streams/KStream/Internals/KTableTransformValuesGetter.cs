///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements. See the NOTICE file distributed with
// * this work for.Additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */

//using Kafka.Streams.Processor.Interfaces;
//using Kafka.Streams.Processor.Internals;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableTransformValuesGetter<K, V, V1> : IKTableValueGetter<K, V1>
//    {
//        private IKTableValueGetter<K, V> parentGetter;
//        private IValueTransformerWithKey<K, V, V1> valueTransformer;

//        public KTableTransformValuesGetter(
//            IKTableValueGetter<K, V> parentGetter,
//            IValueTransformerWithKey<K, V, V1> valueTransformer)
//        {
//            this.parentGetter = parentGetter = parentGetter ?? throw new System.ArgumentNullException("parentGetter", nameof(parentGetter));
//            this.valueTransformer = valueTransformer = valueTransformer ?? throw new System.ArgumentNullException("valueTransformer", nameof(valueTransformer));
//        }

//        public void init(IProcessorContext context)
//        {
//            parentGetter.init(context);
//            valueTransformer.init(new ForwardingDisabledProcessorContext(context));
//        }


//        public ValueAndTimestamp<V1> get(K key)
//        {
//            ValueAndTimestamp<V> valueAndTimestamp = parentGetter[key];
//            return ValueAndTimestamp.make(
//                valueTransformer.transform(key, getValueOrNull(valueAndTimestamp)),
//                valueAndTimestamp == null ? UNKNOWN : valueAndTimestamp.timestamp());
//        }


//        public void close()
//        {
//            parentGetter.close();
//            valueTransformer.close();
//        }
//    }
//}
