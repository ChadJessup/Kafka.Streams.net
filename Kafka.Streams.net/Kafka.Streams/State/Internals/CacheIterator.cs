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
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Internals
//{
//    public class CacheIterator : IKeyValueIterator<Bytes, byte[]>
//    {
//        private IEnumerator<Bytes> keys;
//        private Dictionary<Bytes, byte[]> entries;
//        private Bytes lastKey;

//        private CacheIterator(IEnumerator<Bytes> keys, Dictionary<Bytes, byte[]> entries)
//        {
//            this.keys = keys;
//            this.entries = entries;
//        }


//        public bool hasNext()
//        {
//            return keys.hasNext();
//        }


//        public KeyValue<Bytes, byte[]> next()
//        {
//            lastKey = keys.next();
//            return new KeyValue<>(lastKey, entries[lastKey]);
//        }


//        public void Remove()
//        {
//            // do nothing
//        }


//        public void close()
//        {
//            // do nothing
//        }


//        public Bytes peekNextKey()
//        {
//            throw new InvalidOperationException("peekNextKey not supported");
//        }
//    }
//}
