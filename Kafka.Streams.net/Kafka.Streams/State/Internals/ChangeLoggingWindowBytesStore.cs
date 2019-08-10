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
namespace Kafka.Streams.State.Internals
{


    using Kafka.Common.serialization.Serdes;
    using Kafka.Common.Utils.Bytes;
    using Kafka.Streams.KStream.Windowed;
    using Kafka.Streams.Processor.IProcessorContext;
    using Kafka.Streams.Processor.IStateStore;
    using Kafka.Streams.Processor.Internals.ProcessorStateManager;
    using Kafka.Streams.State.IKeyValueIterator;
    using Kafka.Streams.State.StateSerdes;
    using Kafka.Streams.State.IWindowStore;
    using Kafka.Streams.State.WindowStoreIterator;

    /**
     * Simple wrapper around a {@link WindowStore} to support writing
     * updates to a changelog
     */
    class ChangeLoggingWindowBytesStore
        : WrappedStateStore<IWindowStore<Bytes, byte[]>, byte[], byte[]>
    : IWindowStore<Bytes, byte[]>
    {

        private bool retainDuplicates;
        private IProcessorContext<K, V> context;
        private int seqnum = 0;

        StoreChangeLogger<Bytes, byte[]> changeLogger;

        ChangeLoggingWindowBytesStore(IWindowStore<Bytes, byte[]> bytesStore,
                                      bool retainDuplicates)
        {
            base(bytesStore);
            this.retainDuplicates = retainDuplicates;
        }

        public override void init(IProcessorContext<K, V> context,
                         IStateStore root)
        {
            this.context = context;
            base.init(context, root);
            string topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), name);
            changeLogger = new StoreChangeLogger<>(
                name,
                context,
                new StateSerdes<>(topic, Serdes.Bytes(), Serdes.ByteArray()));
        }

        public override byte[] fetch(Bytes key,
                            long timestamp)
        {
            return wrapped.fetch(key, timestamp);
        }


        public override WindowStoreIterator<byte[]> fetch(Bytes key,
                                                 long from,
                                                 long to)
        {
            return wrapped.fetch(key, from, to);
        }


        public override IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes keyFrom,
                                                               Bytes keyTo,
                                                               long from,
                                                               long to)
        {
            return wrapped.fetch(keyFrom, keyTo, from, to);
        }

        public override IKeyValueIterator<Windowed<Bytes>, byte[]> all()
        {
            return wrapped.all();
        }


        public override IKeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom,
                                                                  long timeTo)
        {
            return wrapped.fetchAll(timeFrom, timeTo);
        }

        public override void put(Bytes key, byte[] value)
        {
            // Note: It's incorrect to bypass the wrapped store here by delegating to another method,
            // but we have no alternative. We must send a timestamped key to the changelog, which means
            // we need to know what timestamp gets used for the record. Hopefully, we can deprecate this
            // method in the future to resolve the situation.
            put(key, value, context.timestamp());
        }

        public override void put(Bytes key,
                        byte[] value,
                        long windowStartTimestamp)
        {
            wrapped.Add(key, value, windowStartTimestamp);
            log(WindowKeySchema.toStoreKeyBinary(key, windowStartTimestamp, maybeUpdateSeqnumForDups()), value);
        }

        void log(Bytes key,
                 byte[] value)
        {
            changeLogger.logChange(key, value);
        }

        private int maybeUpdateSeqnumForDups()
        {
            if (retainDuplicates)
            {
                seqnum = (seqnum + 1) & 0x7FFFFFFF;
            }
            return seqnum;
        }
    }
}
