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
namespace Kafka.Streams.KStream.Internals.Suppress
{
    public class KTableSuppressProcessorSupplier<K, V> : IKTableProcessorSupplier<K, V, V>
    {
        private SuppressedInternal<K> suppress;
        private string storeName;
        private KTableImpl<K, object, V> parentKTable;

        public KTableSuppressProcessorSupplier(SuppressedInternal<K> suppress,
                                                string storeName,
                                                KTableImpl<K, object, V> parentKTable)
        {
            this.Suppress = suppress;
            this.storeName = storeName;
            this.parentKTable = parentKTable;
            // The suppress buffer requires seeing the old values, to support the prior value view.
            parentKTable.enableSendingOldValues();
        }


        public Processor<K, Change<V>> get()
        {
            return new KTableSuppressProcessor<>(suppress, storeName);
        }


        public KTableValueGetterSupplier<K, V> view()
        {
            KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parentKTable.valueGetterSupplier();
            return new KTableValueGetterSupplier<K, V>()
            {

            }
            }
        public KTableValueGetter<K, V> get()
        {
            KTableValueGetter<K, V> parentGetter = parentValueGetterSupplier[];
            //            return new KTableValueGetter<K, V>()
            //            {
            //                    private TimeOrderedKeyValueBuffer<K, V> buffer;
            //        public void init(IProcessorContext context)
            //        {
            //            parentGetter.init(context);
            //            // the main processor is responsible for the buffer's lifecycle
            //            buffer = requireNonNull((TimeOrderedKeyValueBuffer<K, V>)context.getStateStore(storeName));
            //        }


            //        public ValueAndTimestamp<V> get(K key)
            //        {
            //            Maybe<ValueAndTimestamp<V>> maybeValue = buffer.priorValueForBuffered(key);
            //            if (maybeValue.isDefined())
            //            {
            //                return maybeValue.getNullableValue();
            //            }
            //            else
            //            {

            //                // not buffered, so the suppressed view is equal to the parent view
            //                return parentGetter[key];
            //            }
            //        }


            //        public void close()
            //        {
            //            parentGetter.close();
            //            // the main processor is responsible for the buffer's lifecycle
            //        }
            //    };
            //}


            //public string[] storeNames()
            //{
            //    string[] parentStores = parentValueGetterSupplier.storeNames();
            //    string[] stores = new string[1 + parentStores.Length];
            //    System.arraycopy(parentStores, 0, stores, 1, parentStores.Length);
            //    stores[0] = storeName;
            //    return stores;
            //}
            //        };
        }


        public void enableSendingOldValues()
        {
            parentKTable.enableSendingOldValues();
        }

        private static class KTableSuppressProcessor<K, V> : Processor<K, Change<V>>
        {
            private long maxRecords;
            private long maxBytes;
            private long suppressDurationMillis;
            private TimeDefinition<K> bufferTimeDefinition;
            private BufferFullStrategy bufferFullStrategy;
            private bool safeToDropTombstones;
            private string storeName;

            private TimeOrderedKeyValueBuffer<K, V> buffer;
            private IInternalProcessorContext internalProcessorContext;
            private Sensor suppressionEmitSensor;
            private long observedStreamTime = ConsumeResult.NO_TIMESTAMP;

            private KTableSuppressProcessor(SuppressedInternal<K> suppress, string storeName)
            {
                this.storeName = storeName;
                requireNonNull(suppress);
                maxRecords = suppress.bufferConfig().maxRecords();
                maxBytes = suppress.bufferConfig().maxBytes();
                suppressDurationMillis = suppress.timeToWaitForMoreEvents().toMillis();
                bufferTimeDefinition = suppress.timeDefinition();
                bufferFullStrategy = suppress.bufferConfig().bufferFullStrategy();
                safeToDropTombstones = suppress.safeToDropTombstones();
            }



            public void init(IProcessorContext context)
            {
                internalProcessorContext = (IInternalProcessorContext)context;
                suppressionEmitSensor = Sensors.suppressionEmitSensor(internalProcessorContext);

                buffer = requireNonNull((TimeOrderedKeyValueBuffer<K, V>)context.getStateStore(storeName));
                buffer.setSerdesIfNull((ISerde<K>)context.keySerde(), (ISerde<V>)context.valueSerde());
            }


            public void process(K key, Change<V> value)
            {
                observedStreamTime = Math.Max(observedStreamTime, internalProcessorContext.timestamp());
                buffer(key, value);
                enforceConstraints();
            }

            private void buffer(K key, Change<V> value)
            {
                long bufferTime = bufferTimeDefinition.time(internalProcessorContext, key);

                buffer.Add(bufferTime, key, value, internalProcessorContext.recordContext());
            }

            private void enforceConstraints()
            {
                long streamTime = observedStreamTime;
                long expiryTime = streamTime - suppressDurationMillis;

                buffer.evictWhile(()->buffer.minTimestamp() <= expiryTime, this::emit);

                if (overCapacity())
                {
                    switch (bufferFullStrategy)
                    {
                        case EMIT:
                            buffer.evictWhile(this::overCapacity, this::emit);
                            return;
                        case SHUT_DOWN:
                            throw new StreamsException(string.Format(
                                "%s buffer exceeded its max capacity. Currently [%d/%d] records and [%d/%d] bytes.",
                                internalProcessorContext.currentNode().name(),
                                buffer.numRecords(), maxRecords,
                                buffer.bufferSize(), maxBytes
                            ));
                        default:
                            throw new InvalidOperationException(
                                "The bufferFullStrategy [" + bufferFullStrategy +
                                    "] is not implemented. This is a bug in Kafka Streams."
                            );
                    }
                }
            }

            private bool overCapacity()
            {
                return buffer.numRecords() > maxRecords || buffer.bufferSize() > maxBytes;
            }

            private void emit(TimeOrderedKeyValueBuffer.Eviction<K, V> toEmit)
            {
                if (shouldForward(toEmit.value()))
                {
                    ProcessorRecordContext prevRecordContext = internalProcessorContext.recordContext();
                    internalProcessorContext.setRecordContext(toEmit.recordContext());
                    try
                    {

                        internalProcessorContext.forward(toEmit.key(), toEmit.value());
                        suppressionEmitSensor.record();
                    }
                    finally
                    {

                        internalProcessorContext.setRecordContext(prevRecordContext);
                    }
                }
            }

            private bool shouldForward(Change<V> value)
            {
                return value.newValue != null || !safeToDropTombstones;
            }


            public void close()
            {
            }
        }
    }
}