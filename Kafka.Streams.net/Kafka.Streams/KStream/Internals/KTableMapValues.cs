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










class KTableMapValues<K, V, V1> : KTableProcessorSupplier<K, V, V1> {
    private  KTableImpl<K, ?, V> parent;
    private  ValueMapperWithKey<K, V, V1> mapper;
    private  string queryableName;
    private bool sendOldValues = false;

    KTableMapValues( KTableImpl<K, ?, V> parent,
                     ValueMapperWithKey<K, V, V1> mapper,
                     string queryableName)
{
        this.parent = parent;
        this.mapper = mapper;
        this.queryableName = queryableName;
    }

    
    public Processor<K, Change<V>> get()
{
        return new KTableMapValuesProcessor();
    }

    
    public KTableValueGetterSupplier<K, V1> view()
{
        // if the KTable is materialized, use the materialized store to return getter value;
        // otherwise rely on the parent getter and apply map-values on-the-fly
        if (queryableName != null)
{
            return new KTableMaterializedValueGetterSupplier<>(queryableName);
        } else
{

            return new KTableValueGetterSupplier<K, V1>()
{
                 KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

                public KTableValueGetter<K, V1> get()
{
                    return new KTableMapValuesValueGetter(parentValueGetterSupplier());
                }

                
                public string[] storeNames()
{
                    return parentValueGetterSupplier.storeNames();
                }
            };
        }
    }

    
    public void enableSendingOldValues()
{
        parent.enableSendingOldValues();
        sendOldValues = true;
    }

    private V1 computeValue( K key,  V value)
{
        V1 newValue = null;

        if (value != null)
{
            newValue = mapper.apply(key, value);
        }

        return newValue;
    }

    private ValueAndTimestamp<V1> computeValueAndTimestamp( K key,  ValueAndTimestamp<V> valueAndTimestamp)
{
        V1 newValue = null;
        long timestamp = 0;

        if (valueAndTimestamp != null)
{
            newValue = mapper.apply(key, valueAndTimestamp.value());
            timestamp = valueAndTimestamp.timestamp();
        }

        return ValueAndTimestamp.make(newValue, timestamp);
    }


    private KTableMapValuesProcessor : AbstractProcessor<K, Change<V>> {
        private TimestampedKeyValueStore<K, V1> store;
        private TimestampedTupleForwarder<K, V1> tupleForwarder;

        
        
        public void init( IProcessorContext context)
{
            base.init(context);
            if (queryableName != null)
{
                store = (TimestampedKeyValueStore<K, V1>) context.getStateStore(queryableName);
                tupleForwarder = new TimestampedTupleForwarder<>(
                    store,
                    context,
                    new TimestampedCacheFlushListener<K, V1>(context),
                    sendOldValues);
            }
        }

        
        public void process( K key,  Change<V> change)
{
             V1 newValue = computeValue(key, change.newValue);
             V1 oldValue = sendOldValues ? computeValue(key, change.oldValue) : null;

            if (queryableName != null)
{
                store.Add(key, ValueAndTimestamp.make(newValue, context().timestamp()));
                tupleForwarder.maybeForward(key, newValue, oldValue);
            } else
{

                context().forward(key, new Change<>(newValue, oldValue));
            }
        }
    }


    private KTableMapValuesValueGetter : KTableValueGetter<K, V1> {
        private  KTableValueGetter<K, V> parentGetter;

        KTableMapValuesValueGetter( KTableValueGetter<K, V> parentGetter)
{
            this.parentGetter = parentGetter;
        }

        
        public void init( IProcessorContext context)
{
            parentGetter.init(context);
        }

        
        public ValueAndTimestamp<V1> get( K key)
{
            return computeValueAndTimestamp(key, parentGetter[key));
        }

        
        public void close()
{
            parentGetter.close();
        }
    }
}
