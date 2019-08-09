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
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{

















    class KTableKTableLeftJoin<K, R, V1, V2> : KTableKTableAbstractJoin<K, R, V1, V2>
    {
        private static ILogger LOG = new LoggerFactory().CreateLogger<KTableKTableLeftJoin>();

        public KTableKTableLeftJoin(
            KTableImpl<K, object, V1> table1,
            KTableImpl<K, object, V2> table2,
            IValueJoiner<V1, V2, R> joiner)
            : base(table1, table2, joiner)
        {
        }


        public Processor<K, Change<V1>> get()
        {
            return new KTableKTableLeftJoinProcessor(valueGetterSupplier2());
        }


        public IKTableValueGetterSupplier<K, R> view()
        {
            return new KTableKTableLeftJoinValueGetterSupplier(valueGetterSupplier1, valueGetterSupplier2);
        }

        private KTableKTableLeftJoinValueGetterSupplier : KTableKTableAbstractJoinValueGetterSupplier<K, R, V1, V2> {

        KTableKTableLeftJoinValueGetterSupplier(IKTableValueGetterSupplier<K, V1> valueGetterSupplier1,
                                                 IKTableValueGetterSupplier<K, V2> valueGetterSupplier2)
            : base(valueGetterSupplier1, valueGetterSupplier2)
        {
        }

        public IKTableValueGetter<K, R> get()
        {
            return new KTableKTableLeftJoinValueGetter(valueGetterSupplier1(), valueGetterSupplier2());
        }
    }


    private class KTableKTableLeftJoinProcessor : AbstractProcessor<K, Change<V1>>
    {

        private IKTableValueGetter<K, V2> valueGetter;
        private StreamsMetricsImpl metrics;
        private Sensor skippedRecordsSensor;

        KTableKTableLeftJoinProcessor(IKTableValueGetter<K, V2> valueGetter)
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

            R newValue = null;
            long resultTimestamp;
            R oldValue = null;

            ValueAndTimestamp<V2> valueAndTimestampRight = valueGetter[key];
            V2 value2 = getValueOrNull(valueAndTimestampRight);
            long timestampRight;

            if (value2 == null)
            {
                if (change.newValue == null && change.oldValue == null)
                {
                    return;
                }
                timestampRight = UNKNOWN;
            }
            else
            {

                timestampRight = valueAndTimestampRight.timestamp();
            }

            resultTimestamp = Math.Max(context.timestamp(), timestampRight);

            if (change.newValue != null)
            {
                newValue = joiner.apply(change.newValue, value2);
            }

            if (sendOldValues && change.oldValue != null)
            {
                oldValue = joiner.apply(change.oldValue, value2);
            }

            context.forward(key, new Change<>(newValue, oldValue), To.all().withTimestamp(resultTimestamp));
        }


        public void close()
        {
            valueGetter.close();
        }
    }

    private class KTableKTableLeftJoinValueGetter : IKTableValueGetter<K, R>
    {

        private IKTableValueGetter<K, V1> valueGetter1;
        private IKTableValueGetter<K, V2> valueGetter2;

        KTableKTableLeftJoinValueGetter(IKTableValueGetter<K, V1> valueGetter1,
                                         IKTableValueGetter<K, V2> valueGetter2)
        {
            this.valueGetter1 = valueGetter1;
            this.valueGetter2 = valueGetter2;
        }


        public void init(IProcessorContext context)
        {
            valueGetter1.init(context);
            valueGetter2.init(context);
        }


        public ValueAndTimestamp<R> get(K key)
        {
            ValueAndTimestamp<V1> valueAndTimestamp1 = valueGetter1[key];
            V1 value1 = getValueOrNull(valueAndTimestamp1);

            if (value1 != null)
            {
                ValueAndTimestamp<V2> valueAndTimestamp2 = valueGetter2[key];
                V2 value2 = getValueOrNull(valueAndTimestamp2);
                long resultTimestamp;
                if (valueAndTimestamp2 == null)
                {
                    resultTimestamp = valueAndTimestamp1.timestamp();
                }
                else
                {

                    resultTimestamp = Math.Max(valueAndTimestamp1.timestamp(), valueAndTimestamp2.timestamp());
                }
                return ValueAndTimestamp.make(joiner.apply(value1, value2), resultTimestamp);
            }
            else
            {

                return null;
            }
        }


        public void close()
        {
            valueGetter1.close();
            valueGetter2.close();
        }
    }

}
