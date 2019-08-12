﻿///*
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
//using Kafka.Streams.Processor;
//using Kafka.Streams.Processor.Interfaces;
//using Kafka.Streams.State;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableFilterProcessor<K, V> : AbstractProcessor<K, Change<V>>
//    {
//        private ITimestampedKeyValueStore<K, V> store;
//        private TimestampedTupleForwarder<K, V> tupleForwarder;

//        public void init(IProcessorContext<K, V> context)
//        {
//            base.init(context);
//            if (queryableName != null)
//            {
//                store = (ITimestampedKeyValueStore<K, V>)context.getStateStore(queryableName);
//                tupleForwarder = new TimestampedTupleForwarder<K, V>(
//                    store,
//                    context,
//                    new TimestampedCacheFlushListener<K, V>(context),
//                    sendOldValues);
//            }
//        }


//        public void process(K key, Change<V> change)
//        {
//            V newValue = computeValue(key, change.newValue);
//            V oldValue = sendOldValues ? computeValue(key, change.oldValue) : null;

//            if (sendOldValues && oldValue == null && newValue == null)
//            {
//                return; // unnecessary to forward here.
//            }

//            if (queryableName != null)
//            {
//                store.Add(key, ValueAndTimestamp.make(newValue, context.timestamp()));
//                tupleForwarder.maybeForward(key, newValue, oldValue);
//            }
//            else
//            {

//                context.forward(key, new Change<>(newValue, oldValue));
//            }
//        }
//    }
//}