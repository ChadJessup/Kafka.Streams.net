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
//namespace Kafka.Streams.State.Internals
//{
//    public class CompositeKeyValueIterator<K, V, StoreType> : IKeyValueIterator<K, V>
//    {

//        private IEnumerator<StoreType> storeIterator;
//        private INextIteratorFunction<K, V, StoreType> nextIteratorFunction;

//        private IKeyValueIterator<K, V> current;

//        CompositeKeyValueIterator(IEnumerator<StoreType> underlying,
//                                  INextIteratorFunction<K, V, StoreType> nextIteratorFunction)
//        {
//            this.storeIterator = underlying;
//            this.nextIteratorFunction = nextIteratorFunction;
//        }

//        public override void close()
//        {
//            if (current != null)
//            {
//                current.close();
//                current = null;
//            }
//        }

//        public override K peekNextKey()
//        {
//            throw new InvalidOperationException("peekNextKey not supported");
//        }

//        public override bool hasNext()
//        {
//            while ((current == null || !current.hasNext())
//                    && storeIterator.hasNext())
//            {
//                close();
//                current = nextIteratorFunction.apply(storeIterator.next());
//            }
//            return current != null && current.hasNext();
//        }


//        public override KeyValue<K, V> next()
//        {
//            if (!hasNext())
//            {
//                throw new NoSuchElementException();
//            }
//            return current.next();
//        }

//        public override void Remove()
//        {
//            throw new InvalidOperationException("Remove not supported");
//        }
//    }
//}