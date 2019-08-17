/*
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
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processor;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableMapValuesProcessor<K, V, V1> : AbstractProcessor<K, Change<V>>
    {
        private ITimestampedKeyValueStore<K, V1> store;
        private TimestampedTupleForwarder<K, V1> tupleForwarder;
        private string queryableName;
        private IValueMapperWithKey<K, V, V1> mapper;
        private bool sendOldValues;

        public KTableMapValuesProcessor(IValueMapperWithKey<K, V, V1> mapper)
        {
            this.mapper = mapper;
        }

        public override void init(IProcessorContext<K, Change<V>> context)
        {
            base.init(context);
            if (queryableName != null)
            {
                store = (ITimestampedKeyValueStore<K, V1>)context.getStateStore(queryableName);

                tupleForwarder = new TimestampedTupleForwarder<K, V1>(
                    store,
                    context,
                    new TimestampedCacheFlushListener<K, V1>(context),
                    sendOldValues);
            }
        }


        public override void process(K key, Change<V> change)
        {
            V1 newValue = computeValue(key, change.newValue);
            V1 oldValue = sendOldValues
                ? computeValue(key, change.oldValue)
                : default;

            if (queryableName != null)
            {
                store.Add(key, ValueAndTimestamp<V>.make(newValue, context.timestamp()));
                tupleForwarder.maybeForward(key, newValue, oldValue);
            }
            else
            {
                context.forward(key, new Change<V1>(newValue, oldValue));
            }
        }

        private V1 computeValue(K key, V value)
        {
            V1 newValue = default;

            if (value != null)
            {
                newValue = mapper.apply(key, value);
            }

            return newValue;
        }
    }
}