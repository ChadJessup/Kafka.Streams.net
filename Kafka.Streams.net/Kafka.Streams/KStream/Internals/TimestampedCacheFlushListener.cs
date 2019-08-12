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
using Kafka.Streams.Processor;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;

namespace Kafka.Streams.KStream.Internals
{
    public class TimestampedCacheFlushListener<K, V> : ICacheFlushListener<K, ValueAndTimestamp<V>>
    {
        private IInternalProcessorContext<K, V> context;
        private ProcessorNode<K, V> myNode;

        public TimestampedCacheFlushListener(IProcessorContext<K, V> context)
        {
            this.context = (IInternalProcessorContext<K, V>)context;
            myNode = this.context.currentNode();
        }

        public void apply(
            K key,
            ValueAndTimestamp<V> newValue,
            ValueAndTimestamp<V> oldValue,
            long timestamp)
        {
            var prev = context.currentNode();
            context.setCurrentNode(myNode);
            try
            {
                context.forward(
                    key,
                    new Change<ValueAndTimestamp<V>>(newValue, oldValue),
                    To.all().withTimestamp(newValue != null ? newValue.timestamp : timestamp));
            }
            finally
            {
                context.setCurrentNode(prev);
            }
        }
    }
}