﻿///*
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
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.State.Internals
//{
//    public class WrappedWindowStoreIterator : IWindowStoreIterator<byte[]>
//    {
//        IKeyValueIterator<Bytes, byte[]> bytesIterator;

//        public WrappedWindowStoreIterator(
//            IKeyValueIterator<Bytes, byte[]> bytesIterator)
//        {
//            this.bytesIterator = bytesIterator;
//        }


//        public long peekNextKey()
//        {
//            return WindowKeySchema.extractStoreTimestamp(bytesIterator.peekNextKey().get());
//        }


//        public bool hasNext()
//        {
//            return bytesIterator.hasNext();
//        }


//        public KeyValue<long, byte[]> next()
//        {
//            KeyValue<Bytes, byte[]> next = bytesIterator.next();
//            long timestamp = WindowKeySchema.extractStoreTimestamp(next.key());
//            return KeyValue.pair(timestamp, next.value);
//        }


//        public void Remove()
//        {
//            throw new InvalidOperationException("Remove() is not supported in " + GetType().getName());
//        }


//        public void close()
//        {
//            bytesIterator.close();
//        }
//    }
//}
