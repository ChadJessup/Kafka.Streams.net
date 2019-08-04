/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
namespace Kafka.streams.kstream.internals;

import org.apache.kafka.streams.processor.IProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.CacheFlushListener;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

class TimestampedCacheFlushListener<K, V> : CacheFlushListener<K, ValueAndTimestamp<V>> {
    private  InternalProcessorContext context;
    private  ProcessorNode myNode;

    TimestampedCacheFlushListener( IProcessorContext context)
{
        this.context = (InternalProcessorContext) context;
        myNode = this.context.currentNode();
    }

    
    public void apply( K key,
                       ValueAndTimestamp<V> newValue,
                       ValueAndTimestamp<V> oldValue,
                       long timestamp)
{
         ProcessorNode prev = context.currentNode();
        context.setCurrentNode(myNode);
        try {
            context.forward(
                key,
                new Change<>(getValueOrNull(newValue), getValueOrNull(oldValue)),
                To.all().withTimestamp(newValue != null ? newValue.timestamp() : timestamp));
        } finally {
            context.setCurrentNode(prev);
        }
    }
}
