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
package org.apache.kafka.streams.state.internals;

using Kafka.Common.serialization.Serde;
using Kafka.Common.Utils.Bytes;
using Kafka.Common.Utils.Time;
using Kafka.Streams.Processor.IProcessorContext;
using Kafka.Streams.Processor.internals.ProcessorStateManager;
using Kafka.Streams.State.StateSerdes;
using Kafka.Streams.State.TimestampedWindowStore;
using Kafka.Streams.State.ValueAndTimestamp;
using Kafka.Streams.State.WindowStore;

/**
 * A Metered {@link MeteredTimestampedWindowStore} wrapper that is used for recording operation metrics, and hence its
 * inner WindowStore implementation do not need to provide its own metrics collecting functionality.
 * The inner {@link WindowStore} of this class is of type &lt;Bytes,byte[]&gt;, hence we use {@link Serde}s
 * to convert from &lt;K,ValueAndTimestamp&lt;V&gt&gt; to &lt;Bytes,byte[]&gt;
 * @param <K>
 * @param <V>
 */
class MeteredTimestampedWindowStore<K, V>
    : MeteredWindowStore<K, ValueAndTimestamp<V>>
    : TimestampedWindowStore<K, V>
{

    MeteredTimestampedWindowStore(WindowStore<Bytes, byte[]> inner,
                                  long windowSizeMs,
                                  string metricScope,
                                  Time time,
                                  ISerde<K> keySerde,
                                  ISerde<ValueAndTimestamp<V>> valueSerde)
{
        super(inner, windowSizeMs, metricScope, time, keySerde, valueSerde);
    }

    @SuppressWarnings("unchecked")
    @Override
    void initStoreSerde(IProcessorContext context)
{
        serdes = new StateSerdes<>(
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), name()),
            keySerde == null ? (ISerde<K>) context.keySerde() : keySerde,
            valueSerde == null ? new ValueAndTimestampSerde<>((ISerde<V>) context.valueSerde()) : valueSerde);
    }
}
