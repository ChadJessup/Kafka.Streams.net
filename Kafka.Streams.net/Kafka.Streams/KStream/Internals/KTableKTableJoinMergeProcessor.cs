﻿/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
namespace Kafka.Streams.KStream.Internals
{
    private class KTableKTableJoinMergeProcessor : AbstractProcessor<K, Change<V>>
    {
        private TimestampedKeyValueStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;



        public void init(IProcessorContext context)
        {
            base.init(context);
            if (queryableName != null)
            {
                store = (TimestampedKeyValueStore<K, V>)context.getStateStore(queryableName);
                tupleForwarder = new TimestampedTupleForwarder<>(
                    store,
                    context,
                    new TimestampedCacheFlushListener<>(context),
                    sendOldValues);
            }
        }


        public void process(K key, Change<V> value)
        {
            if (queryableName != null)
            {
                store.Add(key, ValueAndTimestamp.make(value.newValue, context().timestamp()));
                tupleForwarder.maybeForward(key, value.newValue, sendOldValues ? value.oldValue : null);
            }
            else
            {

                if (sendOldValues)
                {
                    context().forward(key, value);
                }
                else
                {

                    context().forward(key, new Change<>(value.newValue, null));
                }
            }
        }
    }
}
