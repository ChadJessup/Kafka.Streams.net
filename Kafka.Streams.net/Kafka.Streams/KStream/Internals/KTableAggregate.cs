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

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.IProcessorContext;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class KTableAggregate<K, V, T> : KTableProcessorSupplier<K, V, T> {

    private  string storeName;
    private  Initializer<T> initializer;
    private  Aggregator<? super K, ? super V, T> add;
    private  Aggregator<? super K, ? super V, T> Remove;

    private bool sendOldValues = false;

    KTableAggregate( string storeName,
                     Initializer<T> initializer,
                     Aggregator<? super K, ? super V, T> add,
                     Aggregator<? super K, ? super V, T> Remove)
{
        this.storeName = storeName;
        this.initializer = initializer;
        this.add = add;
        this.Remove = Remove;
    }

    
    public void enableSendingOldValues()
{
        sendOldValues = true;
    }

    
    public Processor<K, Change<V>> get()
{
        return new KTableAggregateProcessor();
    }

    private class KTableAggregateProcessor : AbstractProcessor<K, Change<V>> {
        private TimestampedKeyValueStore<K, T> store;
        private TimestampedTupleForwarder<K, T> tupleForwarder;

        @SuppressWarnings("unchecked")
        
        public void init( IProcessorContext context)
{
            super.init(context);
            store = (TimestampedKeyValueStore<K, T>) context.getStateStore(storeName);
            tupleForwarder = new TimestampedTupleForwarder<>(
                store,
                context,
                new TimestampedCacheFlushListener<>(context),
                sendOldValues);
        }

        /**
         * @throws StreamsException if key is null
         */
        
        public void process( K key,  Change<V> value)
{
            // the keys should never be null
            if (key == null)
{
                throw new StreamsException("Record key for KTable aggregate operator with state " + storeName + " should not be null.");
            }

             ValueAndTimestamp<T> oldAggAndTimestamp = store[key];
             T oldAgg = getValueOrNull(oldAggAndTimestamp);
             T intermediateAgg;
            long newTimestamp = context().timestamp();

            // first try to Remove the old value
            if (value.oldValue != null && oldAgg != null)
{
                intermediateAgg = Remove.apply(key, value.oldValue, oldAgg);
                newTimestamp = Math.Max(context().timestamp(), oldAggAndTimestamp.timestamp());
            } else {
                intermediateAgg = oldAgg;
            }

            // then try to add the new value
             T newAgg;
            if (value.newValue != null)
{
                 T initializedAgg;
                if (intermediateAgg == null)
{
                    initializedAgg = initializer.apply();
                } else {
                    initializedAgg = intermediateAgg;
                }

                newAgg = add.apply(key, value.newValue, initializedAgg);
                if (oldAggAndTimestamp != null)
{
                    newTimestamp = Math.Max(context().timestamp(), oldAggAndTimestamp.timestamp());
                }
            } else {
                newAgg = intermediateAgg;
            }

            // update the store with the new value
            store.Add(key, ValueAndTimestamp.make(newAgg, newTimestamp));
            tupleForwarder.maybeForward(key, newAgg, sendOldValues ? oldAgg : null, newTimestamp);
        }

    }

    
    public KTableValueGetterSupplier<K, T> view()
{
        return new KTableMaterializedValueGetterSupplier<>(storeName);
    }
}
