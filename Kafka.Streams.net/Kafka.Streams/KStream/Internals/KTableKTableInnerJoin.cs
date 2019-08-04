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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.IProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

class KTableKTableInnerJoin<K, R, V1, V2> : KTableKTableAbstractJoin<K, R, V1, V2> {
    private static  Logger LOG = LoggerFactory.getLogger(KTableKTableInnerJoin.class);

    private  IKeyValueMapper<K, V1, K> keyValueMapper = (key, value) -> key;

    KTableKTableInnerJoin( KTableImpl<K, ?, V1> table1,
                           KTableImpl<K, ?, V2> table2,
                           ValueJoiner<? super V1, ? super V2, ? : R> joiner)
{
        super(table1, table2, joiner);
    }

    
    public Processor<K, Change<V1>> get()
{
        return new KTableKTableJoinProcessor(valueGetterSupplier2()];
    }

    
    public KTableValueGetterSupplier<K, R> view()
{
        return new KTableKTableInnerJoinValueGetterSupplier(valueGetterSupplier1, valueGetterSupplier2);
    }

    private class KTableKTableInnerJoinValueGetterSupplier : KTableKTableAbstractJoinValueGetterSupplier<K, R, V1, V2> {

        KTableKTableInnerJoinValueGetterSupplier( KTableValueGetterSupplier<K, V1> valueGetterSupplier1,
                                                  KTableValueGetterSupplier<K, V2> valueGetterSupplier2)
{
            super(valueGetterSupplier1, valueGetterSupplier2);
        }

        public KTableValueGetter<K, R> get()
{
            return new KTableKTableInnerJoinValueGetter(valueGetterSupplier1(), valueGetterSupplier2()];
        }
    }

    private class KTableKTableJoinProcessor : AbstractProcessor<K, Change<V1>> {

        private  KTableValueGetter<K, V2> valueGetter;
        private StreamsMetricsImpl metrics;
        private Sensor skippedRecordsSensor;

        KTableKTableJoinProcessor( KTableValueGetter<K, V2> valueGetter)
{
            this.valueGetter = valueGetter;
        }

        
        public void init( IProcessorContext context)
{
            super.init(context);
            metrics = (StreamsMetricsImpl) context.metrics();
            skippedRecordsSensor = ThreadMetrics.skipRecordSensor(metrics);
            valueGetter.init(context);
        }

        
        public void process( K key,  Change<V1> change)
{
            // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
            if (key == null)
{
                LOG.warn(
                    "Skipping record due to null key. change=[{}] topic=[{}] partition=[{}] offset=[{}]",
                    change, context().topic(), context().partition(), context().offset()
                );
                skippedRecordsSensor.record();
                return;
            }

            R newValue = null;
             long resultTimestamp;
            R oldValue = null;

             ValueAndTimestamp<V2> valueAndTimestampRight = valueGetter[key];
             V2 valueRight = getValueOrNull(valueAndTimestampRight);
            if (valueRight == null)
{
                return;
            }

            resultTimestamp = Math.Max(context().timestamp(), valueAndTimestampRight.timestamp());

            if (change.newValue != null)
{
                newValue = joiner.apply(change.newValue, valueRight);
            }

            if (sendOldValues && change.oldValue != null)
{
                oldValue = joiner.apply(change.oldValue, valueRight);
            }

            context().forward(key, new Change<>(newValue, oldValue), To.all().withTimestamp(resultTimestamp));
        }

        
        public void close()
{
            valueGetter.close();
        }
    }

    private class KTableKTableInnerJoinValueGetter : KTableValueGetter<K, R> {

        private  KTableValueGetter<K, V1> valueGetter1;
        private  KTableValueGetter<K, V2> valueGetter2;

        KTableKTableInnerJoinValueGetter( KTableValueGetter<K, V1> valueGetter1,
                                          KTableValueGetter<K, V2> valueGetter2)
{
            this.valueGetter1 = valueGetter1;
            this.valueGetter2 = valueGetter2;
        }

        
        public void init( IProcessorContext context)
{
            valueGetter1.init(context);
            valueGetter2.init(context);
        }

        
        public ValueAndTimestamp<R> get( K key)
{
             ValueAndTimestamp<V1> valueAndTimestamp1 = valueGetter1[key];
             V1 value1 = getValueOrNull(valueAndTimestamp1);

            if (value1 != null)
{
                 ValueAndTimestamp<V2> valueAndTimestamp2 = valueGetter2[keyValueMapper.apply(key, value1)];
                 V2 value2 = getValueOrNull(valueAndTimestamp2);

                if (value2 != null)
{
                    return ValueAndTimestamp.make(
                        joiner.apply(value1, value2),
                        Math.Max(valueAndTimestamp1.timestamp(), valueAndTimestamp2.timestamp()));
                } else {
                    return null;
                }
            } else {
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
