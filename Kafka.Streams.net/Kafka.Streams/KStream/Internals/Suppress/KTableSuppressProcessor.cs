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
using Confluent.Kafka;
using Kafka.Common.Metrics;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processor;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals;
using Kafka.Streams.State.Internals;
using System;

namespace Kafka.Streams.KStream.Internals.Suppress
{
    public class KTableSuppressProcessor<K, V> : IProcessor<K, Change<V>>
    {
        private readonly long maxRecords;
        private readonly long maxBytes;
        private readonly long suppressDurationMillis;
        private readonly ITimeDefinition<K, V> bufferTimeDefinition;
        private readonly BufferFullStrategy bufferFullStrategy;
        private readonly bool safeToDropTombstones;
        private readonly string storeName;

        //private TimeOrderedKeyValueBuffer<K, V> buffer;
        private IInternalProcessorContext internalProcessorContext;
        private readonly Sensor suppressionEmitSensor;
        private long observedStreamTime = -1L;// ConsumeResult.NO_TIMESTAMP;

        //public KTableSuppressProcessor(SuppressedInternal<K, V> suppress, string storeName)
        //{
        //    this.storeName = storeName;
        //    //requireNonNull(suppress);
        //    maxRecords = suppress.bufferConfig.maxRecords();
        //    maxBytes = suppress.bufferConfig.maxBytes();
        //    suppressDurationMillis = suppress.timeToWaitForMoreEvents().toMillis();
        //    bufferTimeDefinition = suppress.timeDefinition;
        //    bufferFullStrategy = suppress.bufferConfig.bufferFullStrategy();
        //    safeToDropTombstones = suppress.safeToDropTombstones;
        //}

        public void init(IProcessorContext context)
        {
            internalProcessorContext = (IInternalProcessorContext)context;
//            suppressionEmitSensor = Sensors.suppressionEmitSensor(internalProcessorContext);

            //buffer = requireNonNull((TimeOrderedKeyValueBuffer<K, V>)context.getStateStore(storeName));
            //buffer.setSerdesIfNull((ISerde<K>)context.keySerde, (ISerde<V>)context.valueSerde);
        }


        public void process(K key, Change<V> value)
        {
            observedStreamTime = Math.Max(observedStreamTime, internalProcessorContext.timestamp);
            buffer(key, value);
            enforceConstraints();
        }

        private void buffer(K key, Change<V> value)
        {
            long bufferTime = bufferTimeDefinition.time(internalProcessorContext, key);

          //  buffer.Add(bufferTime, key, value, internalProcessorContext.recordContext());
        }

        private void enforceConstraints()
        {
            long streamTime = observedStreamTime;
            long expiryTime = streamTime - suppressDurationMillis;

            //buffer.evictWhile(() => buffer.minTimestamp() <= expiryTime, this.emit);

            if (overCapacity())
            {
                switch (bufferFullStrategy)
                {
                    //case EMIT:
                    //    buffer.evictWhile(this.overCapacity, this.emit);
                    //    return;
                    //case SHUT_DOWN:
                    //    throw new StreamsException(string.Format(
                    //        "%s buffer exceeded its max capacity. Currently [%d/%d] records and [%d/%d] bytes.",
                    //        internalProcessorContext.currentNode().name,
                    //        //buffer.numRecords(), maxRecords,
                    //        //buffer.bufferSize(), maxBytes));
                    //        null, null));

                    default:
                        throw new InvalidOperationException(
                            "The bufferFullStrategy [" + bufferFullStrategy +
                                "] is not implemented. This is a bug in Kafka Streams.");
                }
            }
        }

        private bool overCapacity()
        {
            return false; // buffer.numRecords() > maxRecords || buffer.bufferSize() > maxBytes;
        }

        private void emit(Eviction<K, V> toEmit)
        {
            if (shouldForward(toEmit.value))
            {
                ProcessorRecordContext prevRecordContext = internalProcessorContext.recordContext;
                internalProcessorContext.setRecordContext(toEmit.recordContext);

                try
                {
                    internalProcessorContext.forward(toEmit.key, toEmit.value);
                    suppressionEmitSensor.record();
                }
                finally
                {
                    internalProcessorContext.setRecordContext(prevRecordContext);
                }
            }
        }

        private bool shouldForward(Change<V> value)
        {
            return value.newValue != null || !safeToDropTombstones;
        }

        public void close()
        {
        }
    }
}
