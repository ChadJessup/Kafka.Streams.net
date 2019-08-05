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
namespace Kafka.Streams.KStream.Internals
{
















    class KTableKTableRightJoin<K, R, V1, V2> : KTableKTableAbstractJoin<K, R, V1, V2>
    {
        private static ILogger LOG = new LoggerFactory().CreateLogger < KTableKTableRightJoin);

        KTableKTableRightJoin(KTableImpl<K, ?, V1> table1,
                               KTableImpl<K, ?, V2> table2,
                               ValueJoiner<V1, V2, R> joiner)
            : base(table1, table2, joiner)
        {
        }


        public Processor<K, Change<V1>> get()
        {
            return new KTableKTableRightJoinProcessor(valueGetterSupplier2());
        }


        public KTableValueGetterSupplier<K, R> view()
        {
            return new KTableKTableRightJoinValueGetterSupplier(valueGetterSupplier1, valueGetterSupplier2);
        }

        private KTableKTableRightJoinValueGetterSupplier : KTableKTableAbstractJoinValueGetterSupplier<K, R, V1, V2> {

        KTableKTableRightJoinValueGetterSupplier(KTableValueGetterSupplier<K, V1> valueGetterSupplier1,
                                                  KTableValueGetterSupplier<K, V2> valueGetterSupplier2)
            : base(valueGetterSupplier1, valueGetterSupplier2)
        {
        }

        public KTableValueGetter<K, R> get()
        {
            return new KTableKTableRightJoinValueGetter(valueGetterSupplier1(), valueGetterSupplier2());
        }
    }

    private class KTableKTableRightJoinProcessor : AbstractProcessor<K, Change<V1>>
    {

        private KTableValueGetter<K, V2> valueGetter;
        private StreamsMetricsImpl metrics;
        private Sensor skippedRecordsSensor;

        KTableKTableRightJoinProcessor(KTableValueGetter<K, V2> valueGetter)
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
                    change, context().topic(), context().partition(), context().offset()
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

            resultTimestamp = Math.Max(context().timestamp(), valueAndTimestampLeft.timestamp());

            // joiner == "reverse joiner"
            newValue = joiner.apply(change.newValue, valueLeft);

            if (sendOldValues)
            {
                // joiner == "reverse joiner"
                oldValue = joiner.apply(change.oldValue, valueLeft);
            }

            context().forward(key, new Change<>(newValue, oldValue), To.all().withTimestamp(resultTimestamp));
        }


        public void close()
        {
            valueGetter.close();
        }
    }

    private class KTableKTableRightJoinValueGetter : KTableValueGetter<K, R>
    {

        private KTableValueGetter<K, V1> valueGetter1;
        private KTableValueGetter<K, V2> valueGetter2;

        KTableKTableRightJoinValueGetter(KTableValueGetter<K, V1> valueGetter1,
                                          KTableValueGetter<K, V2> valueGetter2)
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
            ValueAndTimestamp<V2> valueAndTimestamp2 = valueGetter2[key];
            V2 value2 = getValueOrNull(valueAndTimestamp2);

            if (value2 != null)
            {
                ValueAndTimestamp<V1> valueAndTimestamp1 = valueGetter1[key];
                V1 value1 = getValueOrNull(valueAndTimestamp1);
                long resultTimestamp;
                if (valueAndTimestamp1 == null)
                {
                    resultTimestamp = valueAndTimestamp2.timestamp();
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
