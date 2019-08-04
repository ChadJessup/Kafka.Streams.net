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
namespace Kafka.streams.kstream.internals;
















public class KStreamAggregate<K, V, T> : KStreamAggProcessorSupplier<K, K, V, T> {
    private static  Logger LOG = LoggerFactory.getLogger(KStreamAggregate.class);
    private  string storeName;
    private  Initializer<T> initializer;
    private  Aggregator<K, V, T> aggregator;

    private bool sendOldValues = false;

    KStreamAggregate( string storeName,  Initializer<T> initializer,  Aggregator<K, V, T> aggregator)
{
        this.storeName = storeName;
        this.initializer = initializer;
        this.aggregator = aggregator;
    }

    
    public Processor<K, V> get()
{
        return new KStreamAggregateProcessor();
    }

    
    public void enableSendingOldValues()
{
        sendOldValues = true;
    }


    private class KStreamAggregateProcessor : AbstractProcessor<K, V> {
        private TimestampedKeyValueStore<K, T> store;
        private StreamsMetricsImpl metrics;
        private Sensor skippedRecordsSensor;
        private TimestampedTupleForwarder<K, T> tupleForwarder;

        
        
        public void init( IProcessorContext context)
{
            super.init(context);
            metrics = (StreamsMetricsImpl) context.metrics();
            skippedRecordsSensor = ThreadMetrics.skipRecordSensor(metrics);
            store = (TimestampedKeyValueStore<K, T>) context.getStateStore(storeName);
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
                LOG.LogWarning(
                    "Skipping record due to null key or value. key=[{}] value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                    key, value, context().topic(), context().partition(), context().offset()
                );
                skippedRecordsSensor.record();
                return;
            }

             ValueAndTimestamp<T> oldAggAndTimestamp = store[key];
            T oldAgg = getValueOrNull(oldAggAndTimestamp);

             T newAgg;
             long newTimestamp;

            if (oldAgg == null)
{
                oldAgg = initializer.apply();
                newTimestamp = context().timestamp();
            } else {
                oldAgg = oldAggAndTimestamp.value();
                newTimestamp = Math.Max(context().timestamp(), oldAggAndTimestamp.timestamp());
            }

            newAgg = aggregator.apply(key, value, oldAgg);

            store.Add(key, ValueAndTimestamp.make(newAgg, newTimestamp));
            tupleForwarder.maybeForward(key, newAgg, sendOldValues ? oldAgg : null, newTimestamp);
        }
    }

    
    public KTableValueGetterSupplier<K, T> view()
{
        return new KTableValueGetterSupplier<K, T>()
{

            public KTableValueGetter<K, T> get()
{
                return new KStreamAggregateValueGetter();
            }

            
            public string[] storeNames()
{
                return new string[]{storeName};
            }
        };
    }


    private class KStreamAggregateValueGetter : KTableValueGetter<K, T> {
        private TimestampedKeyValueStore<K, T> store;

        
        
        public void init( IProcessorContext context)
{
            store = (TimestampedKeyValueStore<K, T>) context.getStateStore(storeName);
        }

        
        public ValueAndTimestamp<T> get( K key)
{
            return store[key];
        }

        
        public void close() {}
    }
}
