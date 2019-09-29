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
//using Kafka.Streams.Errors;
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State;
//using System;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableReduceProcessor<K, V> : AbstractProcessor<K, Change<V>>
//    {

//        private ITimestampedKeyValueStore<K, V> store;
//        private TimestampedTupleForwarder<K, V> tupleForwarder;

//        public void init(IProcessorContext context)
//        {
//            base.init(context);
//            store = (ITimestampedKeyValueStore<K, V>)context.getStateStore(storeName);
//            tupleForwarder = new TimestampedTupleForwarder<K, V>(
//                store,
//                context,
//                new TimestampedCacheFlushListener<K, V>(context),
//                sendOldValues);
//        }

//        /**
//         * @throws StreamsException if key is null
//         */

//        public override void process(K key, Change<V> value)
//        {
//            // the keys should never be null
//            if (key == null)
//            {
//                throw new StreamsException("Record key for KTable reduce operator with state " + storeName + " should not be null.");
//            }

//            ValueAndTimestamp<V> oldAggAndTimestamp = store[key];
//            V oldAgg = getValueOrNull(oldAggAndTimestamp);
//            V intermediateAgg;
//            long newTimestamp;

//            // first try to Remove the old value
//            if (value.oldValue != null && oldAgg != null)
//            {
//                intermediateAgg = removeReducer.apply(oldAgg, value.oldValue);
//                newTimestamp = Math.Max(context.timestamp(), oldAggAndTimestamp.timestamp());
//            }
//            else
//            {
//                intermediateAgg = oldAgg;
//                newTimestamp = context.timestamp();
//            }

//            // then try to.Add the new value
//            V newAgg;
//            if (value.newValue != null)
//            {
//                if (intermediateAgg == null)
//                {
//                    newAgg = value.newValue;
//                }
//                else
//                {
//                    newAgg = addReducer.apply(intermediateAgg, value.newValue);
//                    newTimestamp = Math.Max(context.timestamp(), oldAggAndTimestamp.timestamp);
//                }
//            }
//            else
//            {
//                newAgg = intermediateAgg;
//            }

//            // update the store with the new value
//            store.Add(key, ValueAndTimestamp<V>.make(newAgg, newTimestamp));
//            tupleForwarder.maybeForward(key, newAgg, sendOldValues ? oldAgg : null, newTimestamp);
//        }
//    }
//}
