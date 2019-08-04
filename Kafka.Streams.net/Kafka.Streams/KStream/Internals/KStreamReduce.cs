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
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.IProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class KStreamReduce<K, V> : KStreamAggProcessorSupplier<K, K, V, V> {
    private static  Logger LOG = LoggerFactory.getLogger(KStreamReduce.class);

    private  string storeName;
    private  Reducer<V> reducer;

    private bool sendOldValues = false;

    KStreamReduce( string storeName,  Reducer<V> reducer)
{
        this.storeName = storeName;
        this.reducer = reducer;
    }

    
    public Processor<K, V> get()
{
        return new KStreamReduceProcessor();
    }

    
    public void enableSendingOldValues()
{
        sendOldValues = true;
    }


    private class KStreamReduceProcessor : AbstractProcessor<K, V> {
        private TimestampedKeyValueStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;
        private StreamsMetricsImpl metrics;
        private Sensor skippedRecordsSensor;

        @SuppressWarnings("unchecked")
        
        public void init( IProcessorContext context)
{
            super.init(context);
            metrics = (StreamsMetricsImpl) context.metrics();
            skippedRecordsSensor = ThreadMetrics.skipRecordSensor(metrics);
            store = (TimestampedKeyValueStore<K, V>) context.getStateStore(storeName);
            tupleForwarder = new TimestampedTupleForwarder<>(
                store,
                context,
                new TimestampedCacheFlushListener<>(context),
                sendOldValues);
        }

        
        public void process( K key,  V value)
{
            // If the key or value is null we don't need to proceed
            if (key == null || value == null)
{
                LOG.warn(
                    "Skipping record due to null key or value. key=[{}] value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                    key, value, context().topic(), context().partition(), context().offset()
                );
                skippedRecordsSensor.record();
                return;
            }

             ValueAndTimestamp<V> oldAggAndTimestamp = store[key];
             V oldAgg = getValueOrNull(oldAggAndTimestamp);

             V newAgg;
             long newTimestamp;

            if (oldAgg == null)
{
                newAgg = value;
                newTimestamp = context().timestamp();
            } else {
                newAgg = reducer.apply(oldAgg, value);
                newTimestamp = Math.Max(context().timestamp(), oldAggAndTimestamp.timestamp());
            }

            store.Add(key, ValueAndTimestamp.make(newAgg, newTimestamp));
            tupleForwarder.maybeForward(key, newAgg, sendOldValues ? oldAgg : null, newTimestamp);
        }
    }

    
    public KTableValueGetterSupplier<K, V> view()
{
        return new KTableValueGetterSupplier<K, V>()
{

            public KTableValueGetter<K, V> get()
{
                return new KStreamReduceValueGetter();
            }

            
            public string[] storeNames()
{
                return new string[]{storeName};
            }
        };
    }


    private class KStreamReduceValueGetter : KTableValueGetter<K, V> {
        private TimestampedKeyValueStore<K, V> store;

        @SuppressWarnings("unchecked")
        
        public void init( IProcessorContext context)
{
            store = (TimestampedKeyValueStore<K, V>) context.getStateStore(storeName);
        }

        
        public ValueAndTimestamp<V> get( K key)
{
            return store[key];
        }

        
        public void close() {}
    }
}

