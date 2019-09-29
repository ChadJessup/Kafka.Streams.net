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
//using Kafka.Common.Metrics;
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals.Metrics;
//using Kafka.Streams.State;
//using Microsoft.Extensions.Logging;
//using System;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KStreamWindowAggregateProcessor<K, V, Agg> : AbstractProcessor<K, V>
//    {
//        private ITimestampedWindowStore<K, Agg> windowStore;
//        private TimestampedTupleForwarder<Windowed<K>, Agg> tupleForwarder;
//        private StreamsMetricsImpl metrics;
//        private IInternalProcessorContext<K, V> internalProcessorContext;
//        private Sensor lateRecordDropSensor;
//        private Sensor skippedRecordsSensor;
//        private long observedStreamTime = ConsumeResult.NO_TIMESTAMP;



//        public void init(IProcessorContext context)
//        {
//            base.init(context);
//            internalProcessorContext = (IInternalProcessorContext)context;

//            metrics = internalProcessorContext.metrics;

//            lateRecordDropSensor = Sensors.lateRecordDropSensor(internalProcessorContext);
//            skippedRecordsSensor = ThreadMetrics.skipRecordSensor(metrics);
//            windowStore = (TimestampedWindowStore<K, Agg>)context.getStateStore(storeName);
//            tupleForwarder = new TimestampedTupleForwarder<>(
//                windowStore,
//                context,
//                new TimestampedCacheFlushListener<>(context),
//                sendOldValues);
//        }


//        public void process(K key, V value)
//        {
//            if (key == null)
//            {
//                log.LogWarning(
//                    "Skipping record due to null key. value=[{}] topic=[{}] partition=[{}] offset=[{}]",
//                    value, context.Topic, context.partition(), context.offset()
//                );
//                skippedRecordsSensor.record();
//                return;
//            }

//            // first get the matching windows
//            long timestamp = context.timestamp();
//            observedStreamTime = Math.Max(observedStreamTime, timestamp);
//            long closeTime = observedStreamTime - windows.gracePeriodMs();

//            Dictionary<long, W> matchedWindows = windows.windowsFor(timestamp);

//            // try update the window, and create the new window for the rest of unmatched window that do not exist yet
//            foreach (var entry in matchedWindows)
//            {
//                long windowStart = entry.Key;
//                long windowEnd = entry.Value.end();
//                if (windowEnd > closeTime)
//                {
//                    ValueAndTimestamp<Agg> oldAggAndTimestamp = windowStore.fetch(key, windowStart);
//                    Agg oldAgg = getValueOrNull(oldAggAndTimestamp);

//                    Agg newAgg;
//                    long newTimestamp;

//                    if (oldAgg == null)
//                    {
//                        oldAgg = initializer.apply();
//                        newTimestamp = context.timestamp();
//                    }
//                    else
//                    {

//                        newTimestamp = Math.Max(context.timestamp(), oldAggAndTimestamp.timestamp());
//                    }

//                    newAgg = aggregator.apply(key, value, oldAgg);

//                    // update the store with the new value
//                    windowStore.Add(key, ValueAndTimestamp.make(newAgg, newTimestamp), windowStart);
//                    tupleForwarder.maybeForward(
//                        new Windowed<>(key, entry.Value),
//                        newAgg,
//                        sendOldValues ? oldAgg : null,
//                        newTimestamp);
//                }
//                else
//                {

//                    log.LogDebug(
//                        "Skipping record for expired window. " +
//                            "key=[{}] " +
//                            "topic=[{}] " +
//                            "partition=[{}] " +
//                            "offset=[{}] " +
//                            "timestamp=[{}] " +
//                            "window=[{},{}] " +
//                            "expiration=[{}] " +
//                            "streamTime=[{}]",
//                        key,
//                        context.Topic,
//                        context.partition(),
//                        context.offset(),
//                        context.timestamp(),
//                        windowStart, windowEnd,
//                        closeTime,
//                        observedStreamTime
//                    );
//                    lateRecordDropSensor.record();
//                }
//            }
//        }
//    }
//}
