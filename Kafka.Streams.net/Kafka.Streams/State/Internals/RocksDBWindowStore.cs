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
//using Kafka.Common.Utils;
//using Kafka.Streams.KStream;
//using Kafka.Streams.Processor.Interfaces;
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.State.Internals
//{

//    public class RocksDbWindowStore
//        : WrappedStateStore<ISegmentedBytesStore, object, object>,
//     IWindowStore<Bytes, byte[]>
//    {
//        private bool retainDuplicates;
//        private long windowSize;

//        private IProcessorContext<Bytes, byte[]> context;
//        private int seqnum = 0;

//        public RocksDbWindowStore(ISegmentedBytesStore bytesStore,
//                           bool retainDuplicates,
//                           long windowSize)
//            : base(bytesStore)
//        {
//            this.retainDuplicates = retainDuplicates;
//            this.windowSize = windowSize;
//        }

//        public void init(IProcessorContext<Bytes, byte[]> context, IStateStore root)
//        {
//            this.context = context;
//            base.init(context, root);
//        }

//        public void put(Bytes key, byte[] value)
//        {
//            put(key, value, context.timestamp());
//        }

//        public void put(Bytes key, byte[] value, long windowStartTimestamp)
//        {
//            maybeUpdateSeqnumForDups();

//            wrapped.Add(WindowKeySchema.toStoreKeyBinary(key, windowStartTimestamp, seqnum), value);
//        }

//        public byte[] fetch(Bytes key, long timestamp)
//        {
//            byte[] bytesValue = wrapped.get(WindowKeySchema.toStoreKeyBinary(key, timestamp, seqnum));
//            if (bytesValue == null)
//            {
//                return null;
//            }
//            return bytesValue;
//        }


//        public IWindowStoreIterator<byte[]> fetch(Bytes key, long timeFrom, long timeTo)
//        {
//            IKeyValueIterator<Bytes, byte[]> bytesIterator = wrapped.fetch(key, timeFrom, timeTo);
//            return new WindowStoreIteratorWrapper(bytesIterator, windowSize).valuesIterator();
//        }


//        public IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from,
//                                                               Bytes to,
//                                                               long timeFrom,
//                                                               long timeTo)
//        {
//            IKeyValueIterator<Bytes, byte[]> bytesIterator = wrapped.fetch(from, to, timeFrom, timeTo);
//            return new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
//        }

//        public IKeyValueIterator<Windowed<Bytes>, byte[]> all()
//        {
//            IKeyValueIterator<Bytes, byte[]> bytesIterator = wrapped.all();
//            return new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
//        }


//        public IKeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom, long timeTo)
//        {
//            IKeyValueIterator<Bytes, byte[]> bytesIterator = wrapped.fetchAll(timeFrom, timeTo);
//            return new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator();
//        }

//        private void maybeUpdateSeqnumForDups()
//        {
//            if (retainDuplicates)
//            {
//                seqnum = (seqnum + 1) & 0x7FFFFFFF;
//            }
//        }
//    }
//}