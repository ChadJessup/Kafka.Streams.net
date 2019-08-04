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
namespace Kafka.Streams.KStream.Internals {








class KTableFilter<K, V> : KTableProcessorSupplier<K, V, V> {
    private  KTableImpl<K, ?, V> parent;
    private  Predicate<K, V> predicate;
    private  bool filterNot;
    private  string queryableName;
    private bool sendOldValues = false;

    KTableFilter( KTableImpl<K, ?, V> parent,
                  Predicate<K, V> predicate,
                  bool filterNot,
                  string queryableName)
{
        this.parent = parent;
        this.predicate = predicate;
        this.filterNot = filterNot;
        this.queryableName = queryableName;
    }

    
    public Processor<K, Change<V>> get()
{
        return new KTableFilterProcessor();
    }

    
    public void enableSendingOldValues()
{
        parent.enableSendingOldValues();
        sendOldValues = true;
    }

    private V computeValue( K key,  V value)
{
        V newValue = null;

        if (value != null && (filterNot ^ predicate.test(key, value)))
{
            newValue = value;
        }

        return newValue;
    }

    private ValueAndTimestamp<V> computeValue( K key,  ValueAndTimestamp<V> valueAndTimestamp)
{
        ValueAndTimestamp<V> newValueAndTimestamp = null;

        if (valueAndTimestamp != null)
{
             V value = valueAndTimestamp.value();
            if (filterNot ^ predicate.test(key, value))
{
                newValueAndTimestamp = valueAndTimestamp;
            }
        }

        return newValueAndTimestamp;
    }


    private KTableFilterProcessor : AbstractProcessor<K, Change<V>> {
        private TimestampedKeyValueStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;

        
        
        public void init( IProcessorContext context)
{
            base.init(context);
            if (queryableName != null)
{
                store = (TimestampedKeyValueStore<K, V>) context.getStateStore(queryableName);
                tupleForwarder = new TimestampedTupleForwarder<>(
                    store,
                    context,
                    new TimestampedCacheFlushListener<>(context),
                    sendOldValues);
            }
        }

        
        public void process( K key,  Change<V> change)
{
             V newValue = computeValue(key, change.newValue);
             V oldValue = sendOldValues ? computeValue(key, change.oldValue) : null;

            if (sendOldValues && oldValue == null && newValue == null)
{
                return; // unnecessary to forward here.
            }

            if (queryableName != null)
{
                store.Add(key, ValueAndTimestamp.make(newValue, context().timestamp()));
                tupleForwarder.maybeForward(key, newValue, oldValue);
            } else {
                context().forward(key, new Change<>(newValue, oldValue));
            }
        }
    }

    
    public KTableValueGetterSupplier<K, V> view()
{
        // if the KTable is materialized, use the materialized store to return getter value;
        // otherwise rely on the parent getter and apply filter on-the-fly
        if (queryableName != null)
{
            return new KTableMaterializedValueGetterSupplier<>(queryableName);
        } else {
            return new KTableValueGetterSupplier<K, V>()
{
                 KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

                public KTableValueGetter<K, V> get()
{
                    return new KTableFilterValueGetter(parentValueGetterSupplier());
                }

                
                public string[] storeNames()
{
                    return parentValueGetterSupplier.storeNames();
                }
            };
        }
    }


    private KTableFilterValueGetter : KTableValueGetter<K, V> {
        private  KTableValueGetter<K, V> parentGetter;

        KTableFilterValueGetter( KTableValueGetter<K, V> parentGetter)
{
            this.parentGetter = parentGetter;
        }

        
        
        public void init( IProcessorContext context)
{
            parentGetter.init(context);
        }

        
        public ValueAndTimestamp<V> get( K key)
{
            return computeValue(key, parentGetter[key));
        }

        
        public void close()
{
            parentGetter.close();
        }
    }

}
