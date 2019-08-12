///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements. See the NOTICE file distributed with
// * this work for.Additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//using Confluent.Kafka;
//using Kafka.Common.Metrics;
//using Kafka.Common.Utils;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.KStream.Internals.Metrics;
//using Kafka.Streams.Processor.Interfaces;
//using Kafka.Streams.Processor.Internals;
//using System;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Internals
//{
//    public partial class InMemoryTimeOrderedKeyValueBuffer<K, V> : TimeOrderedKeyValueBuffer<K, V>
//    {
//        private static BytesSerializer KEY_SERIALIZER = new BytesSerializer();
//        private static ByteArraySerializer VALUE_SERIALIZER = new ByteArraySerializer();
//        private static RecordHeaders V_1_CHANGELOG_HEADERS =
//            new RecordHeaders(new Header[] { new RecordHeader("v", new byte[] { (byte)1 }) });
//        private static RecordHeaders V_2_CHANGELOG_HEADERS =
//            new RecordHeaders(new Header[] { new RecordHeader("v", new byte[] { (byte)2 }) });

//        private Dictionary<Bytes, BufferKey> index = new Dictionary<Bytes, BufferKey>();
//        private TreeMap<BufferKey, BufferValue> sortedMap = new TreeMap<>();

//        private HashSet<Bytes> dirtyKeys = new HashSet<Bytes>();
//        private string storeName;
//        private bool loggingEnabled;

//        private ISerde<K> keySerde;
//        private FullChangeSerde<V> valueSerde;

//        private long memBufferSize = 0L;
//        private long minTimestamp = long.MaxValue;
//        private RecordCollector collector;
//        private string changelogTopic;
//        private Sensor bufferSizeSensor;
//        private Sensor bufferCountSensor;

//        private volatile bool open;

//        private int partition;

//        private InMemoryTimeOrderedKeyValueBuffer(string storeName,
//                                                  bool loggingEnabled,
//                                                  ISerde<K> keySerde,
//                                                  ISerde<V> valueSerde)
//        {
//            this.storeName = storeName;
//            this.loggingEnabled = loggingEnabled;
//            this.keySerde = keySerde;
//            this.valueSerde = FullChangeSerde.wrap(valueSerde);
//        }

//        public override string name => storeName;

//        public override bool persistent()
//        {
//            return false;
//        }

//        public override void setSerdesIfNull(ISerde<K> keySerde, ISerde<V> valueSerde)
//        {
//            this.keySerde = this.keySerde == null ? keySerde : this.keySerde;
//            this.valueSerde = this.valueSerde == null ? FullChangeSerde.wrap(valueSerde) : this.valueSerde;
//        }

//        public void init(IProcessorContext<K, V> context, IStateStore root)
//        {
//            IInternalProcessorContext<K, V> internalProcessorContext = (IInternalProcessorContext)context;

//            bufferSizeSensor = Sensors.createBufferSizeSensor(this, internalProcessorContext);
//            bufferCountSensor = Sensors.createBufferCountSensor(this, internalProcessorContext);

//            context.register(root, (RecordBatchingStateRestoreCallback)this.restoreBatch);
//            if (loggingEnabled)
//            {
//                collector = ((RecordCollector.Supplier)context).recordCollector();
//                changelogTopic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName);
//            }
//            updateBufferMetrics();
//            open = true;
//            partition = context.taskId().partition;
//        }

//        public bool isOpen()
//        {
//            return open;
//        }

//        public override void close()
//        {
//            open = false;
//            index.clear();
//            sortedMap.clear();
//            dirtyKeys.clear();
//            memBufferSize = 0;
//            minTimestamp = long.MaxValue;
//            updateBufferMetrics();
//        }

//        public override void flush()
//        {
//            if (loggingEnabled)
//            {
//                // counting on this getting called before the record collector's flush
//                foreach (Bytes key in dirtyKeys)
//                {

//                    BufferKey bufferKey = index[key];

//                    if (bufferKey == null)
//                    {
//                        // The record was evicted from the buffer. Send a tombstone.
//                        logTombstone(key);
//                    }
//                    else
//                    {
//                        BufferValue value = sortedMap[bufferKey];

//                        logValue(key, bufferKey, value);
//                    }
//                }
//                dirtyKeys.clear();
//            }
//        }

//        private void logValue(Bytes key, BufferKey bufferKey, BufferValue value)
//        {

//            int sizeOfBufferTime = sizeof(long);
//            ByteBuffer buffer = value.Serialize(sizeOfBufferTime);
//            buffer.putLong(bufferKey.time);

//            collector.send(
//                changelogTopic,
//                key,
//                buffer.array(),
//                V_2_CHANGELOG_HEADERS,
//                partition,
//                null,
//                KEY_SERIALIZER,
//                VALUE_SERIALIZER
//            );
//        }

//        private void logTombstone(Bytes key)
//        {
//            collector.send(changelogTopic,
//                           key,
//                           null,
//                           null,
//                           partition,
//                           null,
//                           KEY_SERIALIZER,
//                           VALUE_SERIALIZER
//            );
//        }

//        private void restoreBatch(List<ConsumeResult<byte[], byte[]>> batch)
//        {
//            foreach (ConsumeResult<byte[], byte[]> record in batch)
//            {
//                Bytes key = Bytes.wrap(record.key());
//                if (record.value() == null)
//                {
//                    // This was a tombstone. Delete the record.
//                    BufferKey bufferKey = index.Remove(key);
//                    if (bufferKey != null)
//                    {
//                        BufferValue removed = sortedMap.Remove(bufferKey);
//                        if (removed != null)
//                        {
//                            memBufferSize -= computeRecordSize(bufferKey.key, removed);
//                        }
//                        if (bufferKey.time == minTimestamp)
//                        {
//                            minTimestamp = sortedMap.isEmpty() ? long.MaxValue : sortedMap.firstKey().time();
//                        }
//                    }

//                    if (record.partition() != partition)
//                    {
//                        throw new InvalidOperationException(
//                            string.Format(
//                                "record partition [%d] is being restored by the wrong suppress partition [%d]",
//                                record.partition(),
//                                partition
//                            )
//                        );
//                    }
//                }
//                else
//                {
//                    if (record.headers().lastHeader("v") == null)
//                    {
//                        // in this case, the changelog value is just the serialized record value
//                        ByteBuffer timeAndValue = ByteBuffer.wrap(record.value());
//                        long time = timeAndValue.getLong();
//                        byte[] changelogValue = new byte[record.value().Length - 8];
//                        timeAndValue[changelogValue];

//                        Change<byte[]> change = requireNonNull(FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(changelogValue));

//                        ProcessorRecordContext recordContext = new ProcessorRecordContext(
//                            record.timestamp(),
//                            record.offset(),
//                            record.partition(),
//                            record.Topic,
//                            record.headers()
//                        );

//                        cleanPut(
//                            time,
//                            key,
//                            new BufferValue(
//                                index.ContainsKey(key)
//                                    ? internalPriorValueForBuffered(key)
//                                    : change.oldValue,
//                                change.oldValue,
//                                change.newValue,
//                                recordContext
//                            )
//                        );
//                    }
//                    else if (V_1_CHANGELOG_HEADERS.lastHeader("v").Equals(record.headers().lastHeader("v")))
//                    {
//                        // in this case, the changelog value is a serialized ContextualRecord
//                        ByteBuffer timeAndValue = ByteBuffer.wrap(record.value());
//                        long time = timeAndValue.getLong();
//                        byte[] changelogValue = new byte[record.value().Length - 8];
//                        timeAndValue[changelogValue];

//                        ContextualRecord contextualRecord = ContextualRecord.Deserialize(ByteBuffer.wrap(changelogValue));
//                        Change<byte[]> change = requireNonNull(FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(contextualRecord.value()));

//                        cleanPut(
//                            time,
//                            key,
//                            new BufferValue(
//                                index.ContainsKey(key)
//                                    ? internalPriorValueForBuffered(key)
//                                    : change.oldValue,
//                                change.oldValue,
//                                change.newValue,
//                                contextualRecord.recordContext()
//                            )
//                        );
//                    }
//                    else if (V_2_CHANGELOG_HEADERS.lastHeader("v").Equals(record.headers().lastHeader("v")))
//                    {
//                        // in this case, the changelog value is a serialized BufferValue

//                        ByteBuffer valueAndTime = ByteBuffer.wrap(record.value());
//                        BufferValue bufferValue = BufferValue.Deserialize(valueAndTime);
//                        long time = valueAndTime.getLong();
//                        cleanPut(time, key, bufferValue);
//                    }
//                    else
//                    {
//                        throw new System.ArgumentException("Restoring apparently invalid changelog record: " + record);
//                    }
//                }
//                if (record.partition() != partition)
//                {
//                    throw new InvalidOperationException(
//                        string.Format(
//                            "record partition [%d] is being restored by the wrong suppress partition [%d]",
//                            record.partition(),
//                            partition
//                        )
//                    );
//                }
//            }
//            updateBufferMetrics();
//        }

//        public override void evictWhile(Supplier<bool> predicate,
//                               IConsumer<K, Eviction<K, V>> callback)
//        {
//            IEnumerator<KeyValuePair<BufferKey, BufferValue>> @delegate = sortedMap.iterator();
//            int evictions = 0;

//            if (predicate())
//            {
//                KeyValuePair<BufferKey, BufferValue> next = null;
//                if (@delegate.hasNext())
//                {
//                    next = @delegate.next();
//                }

//                // predicate being true means we read one record, call the callback, and then Remove it
//                while (next != null && predicate())
//                {
//                    if (next.Key.time != minTimestamp)
//                    {
//                        throw new InvalidOperationException(
//                            "minTimestamp [" + minTimestamp + "] did not match the actual min timestamp [" +
//                                next.Key.time + "]"
//                        );
//                    }
//                    K key = keySerde.Deserializer.Deserialize(changelogTopic, next.Key.key.get());
//                    BufferValue bufferValue = next.Value;
//                    Change<V> value = valueSerde.deserializeParts(
//                        changelogTopic,
//                        new Change<>(bufferValue.newValue, bufferValue.oldValue)
//                    );
//                    callback.accept(new Eviction<>(key, value, bufferValue.context));

//                    @delegate.Remove();
//                    index.Remove(next.Key.key);

//                    dirtyKeys.Add(next.Key.key);

//                    memBufferSize -= computeRecordSize(next.Key.key, bufferValue);

//                    // peek at the next record so we can update the minTimestamp
//                    if (@delegate.hasNext())
//                    {
//                        next = @delegate.next();
//                        minTimestamp = next == null ? long.MaxValue : next.Key.time;
//                    }
//                    else
//                    {
//                        next = null;
//                        minTimestamp = long.MaxValue;
//                    }

//                    evictions++;
//                }
//            }
//            if (evictions > 0)
//            {
//                updateBufferMetrics();
//            }
//        }

//        public override Maybe<ValueAndTimestamp<V>> priorValueForBuffered(K key)
//        {
//            Bytes serializedKey = Bytes.wrap(keySerde.Serializer.Serialize(changelogTopic, key));
//            if (index.ContainsKey(serializedKey))
//            {
//                byte[] serializedValue = internalPriorValueForBuffered(serializedKey);

//                V deserializedValue = valueSerde.innerSerde().Deserializer.Deserialize(
//                    changelogTopic,
//                    serializedValue
//                );

//                // it's unfortunately not possible to know this, unless we materialize the suppressed result, since our only
//                // knowledge of the prior value is what the upstream processor sends us as the "old value" when we first
//                // buffer something.
//                return Maybe.defined(ValueAndTimestamp.make(deserializedValue, RecordQueue.UNKNOWN));
//            }
//            else
//            {
//                return Maybe.undefined();
//            }
//        }

//        private byte[] internalPriorValueForBuffered(Bytes key)
//        {
//            BufferKey bufferKey = index[key];
//            if (bufferKey == null)
//            {
//                throw new NoSuchElementException("Key [" + key + "] is not in the buffer.");
//            }
//            else
//            {
//                BufferValue bufferValue = sortedMap[bufferKey];
//                return bufferValue.priorValue;
//            }
//        }

//        public override void put(long time,
//                        K key,
//                        Change<V> value,
//                        ProcessorRecordContext recordContext)
//        {
//            requireNonNull(value, "value cannot be null");
//            requireNonNull(recordContext, "recordContext cannot be null");

//            Bytes serializedKey = Bytes.wrap(keySerde.Serializer.Serialize(changelogTopic, key));
//            Change<byte[]> serialChange = valueSerde.serializeParts(changelogTopic, value);

//            BufferValue buffered = getBuffered(serializedKey);
//            byte[] serializedPriorValue;
//            if (buffered == null)
//            {
//                V priorValue = value.oldValue;
//                serializedPriorValue = valueSerde.innerSerde().Serializer.Serialize(changelogTopic, priorValue);
//            }
//            else
//            {
//                serializedPriorValue = buffered.priorValue;
//            }

//            cleanPut(
//                time,
//                serializedKey,
//                new BufferValue(serializedPriorValue, serialChange.oldValue, serialChange.newValue, recordContext)
//            );
//            dirtyKeys.Add(serializedKey);
//            updateBufferMetrics();
//        }

//        private BufferValue getBuffered(Bytes key)
//        {
//            BufferKey bufferKey = index[key];
//            return bufferKey == null ? null : sortedMap[bufferKey];
//        }

//        private void cleanPut(long time, Bytes key, BufferValue value)
//        {
//            // non-resetting semantics:
//            // if there was a previous version of the same record,
//            // then insert the new record in the same place in the priority queue

//            BufferKey previousKey = index[key];
//            if (previousKey == null)
//            {
//                BufferKey nextKey = new BufferKey(time, key);
//                index.Add(key, nextKey);
//                sortedMap.Add(nextKey, value);
//                minTimestamp = Math.Min(minTimestamp, time);
//                memBufferSize += computeRecordSize(key, value);
//            }
//            else
//            {
//                BufferValue removedValue = sortedMap.Add(previousKey, value);
//                memBufferSize =
//                    memBufferSize
//                        + computeRecordSize(key, value)
//                        - (removedValue == null ? 0 : computeRecordSize(key, removedValue));
//            }
//        }

//        public override int numRecords()
//        {
//            return index.size();
//        }

//        public override long bufferSize()
//        {
//            return memBufferSize;
//        }

//        private static long computeRecordSize(Bytes key, BufferValue value)
//        {
//            long size = 0L;
//            size += 8; // buffer time
//            size += key.get().Length;
//            if (value != null)
//            {
//                size += value.residentMemorySizeEstimate();
//            }
//            return size;
//        }

//        private void updateBufferMetrics()
//        {
//            bufferSizeSensor.record(memBufferSize);
//            bufferCountSensor.record(index.size());
//        }

//        public override string ToString()
//        {
//            return "InMemoryTimeOrderedKeyValueBuffer{" +
//                "storeName='" + storeName + '\'' +
//                ", changelogTopic='" + changelogTopic + '\'' +
//                ", open=" + open +
//                ", loggingEnabled=" + loggingEnabled +
//                ", minTimestamp=" + minTimestamp +
//                ", memBufferSize=" + memBufferSize +
//                ", \n\tdirtyKeys=" + dirtyKeys +
//                ", \n\tindex=" + index +
//                ", \n\tsortedMap=" + sortedMap +
//                '}';
//        }
//    }
//}
