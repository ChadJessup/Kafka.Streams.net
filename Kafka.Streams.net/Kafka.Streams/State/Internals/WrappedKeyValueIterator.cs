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
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.State.Internals
//{
//    public class WrappedKeyValueIterator : IKeyValueIterator<Windowed<Bytes>, byte[]>
//    {
//        IKeyValueIterator<Bytes, byte[]> bytesIterator;
//        long windowSize;

//        public WrappedKeyValueIterator(IKeyValueIterator<Bytes, byte[]> bytesIterator,
//                                long windowSize)
//        {
//            this.bytesIterator = bytesIterator;
//            this.windowSize = windowSize;
//        }


//        public Windowed<Bytes> peekNextKey()
//        {
//            byte[] nextKey = bytesIterator.peekNextKey().get();
//            return WindowKeySchema.fromStoreBytesKey(nextKey, windowSize);
//        }


//        public bool hasNext()
//        {
//            return bytesIterator.hasNext();
//        }


//        public KeyValue<Windowed<Bytes>, byte[]> next()
//        {
//            KeyValue<Bytes, byte[]> next = bytesIterator.next();
//            return KeyValue.pair(WindowKeySchema.fromStoreBytesKey(next.key(), windowSize), next.value);
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
