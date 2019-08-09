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
using Kafka.Streams.Processor;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public partial class KTableKTableRightJoin<K, R, V1, V2>
    {
        public class KTableKTableRightJoinProcessor<K, V1> : AbstractProcessor<K, Change<V1>>
        {

            private IKTableValueGetter<K, V2> valueGetter;
            private StreamsMetricsImpl metrics;
            private Sensor skippedRecordsSensor;

            KTableKTableRightJoinProcessor(IKTableValueGetter<K, V2> valueGetter)
            {
                this.valueGetter = valueGetter;
            }


            public void init(IProcessorContext context)
            {
                base.init(context);
                metrics = (StreamsMetricsImpl)context.metrics();
                skippedRecordsSensor = ThreadMetrics.skipRecordSensor(metrics);
                valueGetter.init(context);
            }


            public void process(K key, Change<V1> change)
            {
                // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
                if (key == null)
                {
                    LOG.LogWarning(
                        "Skipping record due to null key. change=[{}] topic=[{}] partition=[{}] offset=[{}]",
                        change, context.Topic, context.partition(), context.offset()
                    );
                    skippedRecordsSensor.record();
                    return;
                }

                R newValue;
                long resultTimestamp;
                R oldValue = null;

                ValueAndTimestamp<V2> valueAndTimestampLeft = valueGetter[key];
                V2 valueLeft = getValueOrNull(valueAndTimestampLeft);
                if (valueLeft == null)
                {
                    return;
                }

                resultTimestamp = Math.Max(context.timestamp(), valueAndTimestampLeft.timestamp());

                // joiner == "reverse joiner"
                newValue = joiner.apply(change.newValue, valueLeft);

                if (sendOldValues)
                {
                    // joiner == "reverse joiner"
                    oldValue = joiner.apply(change.oldValue, valueLeft);
                }

                context.forward(key, new Change<>(newValue, oldValue), To.all().withTimestamp(resultTimestamp));
            }


            public void close()
            {
                valueGetter.close();
            }
        }

    }