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
using Kafka.Streams.Errors;
using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableMapProcessor<K, V> : AbstractProcessor<K, Change<V>>
    {

        /**
         * @throws StreamsException if key is null
         */

        public override void process(K key, Change<V> change)
        {
            // the original key should never be null
            if (key == null)
            {
                throw new StreamsException("Record key for the grouping KTable should not be null.");
            }

            // if the value is null, we do not need to forward its selected key-value further
            //KeyValue<K, V> newPair = change.newValue == null ? null : mapper.apply(key, change.newValue);
            //KeyValue<K, V> oldPair = change.oldValue == null ? null : mapper.apply(key, change.oldValue);

            // if the selected repartition key or value is null, skip
            // forward oldPair first, to be consistent with reduce and aggregate
//            if (oldPair != null && oldPair.key != null && oldPair.value != null)
            {
                //context.forward(oldPair.key, new Change<K>(null, oldPair.value));
            }

            //if (newPair != null && newPair.key != null && newPair.value != null)
            //{
            //    context.forward(newPair.key, new Change<K>(newPair.value, null));
            //}

        }
    }
}