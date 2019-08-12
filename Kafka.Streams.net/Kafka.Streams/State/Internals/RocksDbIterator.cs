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
//using Kafka.Streams.Errors;
//using Kafka.Streams.State.Interfaces;
//using System.Collections.Generic;
//using System.Runtime.CompilerServices;

//namespace Kafka.Streams.State.Internals
//{
//    public class RocksDbIterator : AbstractIterator<KeyValue<Bytes, byte[]>>
//       , IKeyValueIterator<Bytes, byte[]>
//    {
//        private string storeName;
//        private RocksIterator iter;
//        private HashSet<IKeyValueIterator<Bytes, byte[]>> openIterators;

//        private volatile bool open = true;

//        private KeyValue<Bytes, byte[]> next;

//        public RocksDbIterator(string storeName,
//                        RocksIterator iter,
//                        HashSet<IKeyValueIterator<Bytes, byte[]>> openIterators)
//        {
//            this.storeName = storeName;
//            this.iter = iter;
//            this.openIterators = openIterators;
//        }

//        [MethodImpl(MethodImplOptions.Synchronized)]
//        public override bool hasNext()
//        {
//            if (!open)
//            {
//                throw new InvalidStateStoreException(string.Format("RocksDb iterator for store %s has closed", storeName));
//            }
//            return base.hasNext();
//        }

//        public override KeyValue<Bytes, byte[]> makeNext()
//        {
//            if (!iter.isValid())
//            {
//                return allDone();
//            }
//            else
//            {
//                next = getKeyValue();
//                iter.next();
//                return next;
//            }
//        }

//        private KeyValue<Bytes, byte[]> getKeyValue()
//        {
//            return new KeyValue<Bytes, byte[]>(new Bytes(iter.key()), iter.value());
//        }

//        public override void Remove()
//        {
//            throw new InvalidOperationException("RocksDb iterator does not support Remove()");
//        }

//        [MethodImpl(MethodImplOptions.Synchronized)]
//        public override void close()
//        {
//            openIterators.Remove(this);
//            iter.close();
//            open = false;
//        }

//        public override Bytes peekNextKey()
//        {
//            if (!hasNext())
//            {
//                throw new NoSuchElementException();
//            }
//            return next.key;
//        }
//    }
//}