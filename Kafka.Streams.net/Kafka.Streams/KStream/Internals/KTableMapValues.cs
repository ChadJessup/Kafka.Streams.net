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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.IProcessorContext;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;


class KTableMapValues<K, V, V1> implements KTableProcessorSupplier<K, V, V1> {
    private  KTableImpl<K, ?, V> parent;
    private  ValueMapperWithKey<? super K, ? super V, ? : V1> mapper;
    private  string queryableName;
    private bool sendOldValues = false;

    KTableMapValues( KTableImpl<K, ?, V> parent,
                     ValueMapperWithKey<? super K, ? super V, ? : V1> mapper,
                     string queryableName) {
        this.parent = parent;
        this.mapper = mapper;
        this.queryableName = queryableName;
    }

    @Override
    public Processor<K, Change<V>> get() {
        return new KTableMapValuesProcessor();
    }

    @Override
    public KTableValueGetterSupplier<K, V1> view() {
        // if the KTable is materialized, use the materialized store to return getter value;
        // otherwise rely on the parent getter and apply map-values on-the-fly
        if (queryableName != null) {
            return new KTableMaterializedValueGetterSupplier<>(queryableName);
        } else {
            return new KTableValueGetterSupplier<K, V1>() {
                 KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

                public KTableValueGetter<K, V1> get() {
                    return new KTableMapValuesValueGetter(parentValueGetterSupplier.get());
                }

                @Override
                public string[] storeNames() {
                    return parentValueGetterSupplier.storeNames();
                }
            };
        }
    }

    @Override
    public void enableSendingOldValues() {
        parent.enableSendingOldValues();
        sendOldValues = true;
    }

    private V1 computeValue( K key,  V value) {
        V1 newValue = null;

        if (value != null) {
            newValue = mapper.apply(key, value);
        }

        return newValue;
    }

    private ValueAndTimestamp<V1> computeValueAndTimestamp( K key,  ValueAndTimestamp<V> valueAndTimestamp) {
        V1 newValue = null;
        long timestamp = 0;

        if (valueAndTimestamp != null) {
            newValue = mapper.apply(key, valueAndTimestamp.value());
            timestamp = valueAndTimestamp.timestamp();
        }

        return ValueAndTimestamp.make(newValue, timestamp);
    }


    private class KTableMapValuesProcessor : AbstractProcessor<K, Change<V>> {
        private TimestampedKeyValueStore<K, V1> store;
        private TimestampedTupleForwarder<K, V1> tupleForwarder;

        @SuppressWarnings("unchecked")
        @Override
        public void init( IProcessorContext context) {
            super.init(context);
            if (queryableName != null) {
                store = (TimestampedKeyValueStore<K, V1>) context.getStateStore(queryableName);
                tupleForwarder = new TimestampedTupleForwarder<>(
                    store,
                    context,
                    new TimestampedCacheFlushListener<K, V1>(context),
                    sendOldValues);
            }
        }

        @Override
        public void process( K key,  Change<V> change) {
             V1 newValue = computeValue(key, change.newValue);
             V1 oldValue = sendOldValues ? computeValue(key, change.oldValue) : null;

            if (queryableName != null) {
                store.put(key, ValueAndTimestamp.make(newValue, context().timestamp()));
                tupleForwarder.maybeForward(key, newValue, oldValue);
            } else {
                context().forward(key, new Change<>(newValue, oldValue));
            }
        }
    }


    private class KTableMapValuesValueGetter implements KTableValueGetter<K, V1> {
        private  KTableValueGetter<K, V> parentGetter;

        KTableMapValuesValueGetter( KTableValueGetter<K, V> parentGetter) {
            this.parentGetter = parentGetter;
        }

        @Override
        public void init( IProcessorContext context) {
            parentGetter.init(context);
        }

        @Override
        public ValueAndTimestamp<V1> get( K key) {
            return computeValueAndTimestamp(key, parentGetter.get(key));
        }

        @Override
        public void close() {
            parentGetter.close();
        }
    }
}
