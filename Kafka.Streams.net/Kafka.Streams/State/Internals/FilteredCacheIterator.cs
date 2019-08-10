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
using Kafka.Common.Utils;
using Kafka.Streams.State.Interfaces;
using System;
using System.Collections;

namespace Kafka.Streams.State.Internals
{
    public class FilteredCacheIterator : IPeekingKeyValueIterator<Bytes, LRUCacheEntry>
    {
        private IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator;
        private HasNextCondition hasNextCondition;
        private IPeekingKeyValueIterator<Bytes, LRUCacheEntry> wrappedIterator;

        public KeyValue<Bytes, LRUCacheEntry> Current { get; }
        object IEnumerator.Current { get; }

        public FilteredCacheIterator(
            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
            HasNextCondition hasNextCondition,
            ICacheFunction cacheFunction)
        {
            this.cacheIterator = cacheIterator;
            this.hasNextCondition = hasNextCondition;
            this.wrappedIterator = null; // new IPeekingKeyValueIterator<Bytes, LRUCacheEntry>()
                                         //    {


            //    public KeyValue<Bytes, LRUCacheEntry> peekNext()
            //    {
            //        return cachedPair(cacheIterator.peekNext());
            //    }


            //    public void close()
            //    {
            //        cacheIterator.close();
            //    }


            //    public Bytes peekNextKey()
            //    {
            //        return cacheFunction.key(cacheIterator.peekNextKey());
            //    }


            //    public bool hasNext()
            //    {
            //        return cacheIterator.hasNext();
            //    }


            //    public KeyValue<Bytes, LRUCacheEntry> next()
            //    {
            //        return cachedPair(cacheIterator.next());
            //    }

            //    private KeyValue<Bytes, LRUCacheEntry> cachedPair(KeyValue<Bytes, LRUCacheEntry> next)
            //    {
            //        return KeyValue.pair(cacheFunction.key(next.key), next.value);
            //    }


            //    public void Remove()
            //    {
            //        cacheIterator.Remove();
            //    }
            //};
        }

        public void close()
        {
            // no-op
        }

        public Bytes peekNextKey()
        {
            if (!hasNext())
            {
                throw new IndexOutOfRangeException();
            }
            return cacheIterator.peekNextKey();
        }

        public bool hasNext()
        {
            return hasNextCondition.hasNext(wrappedIterator);
        }

        public KeyValue<Bytes, LRUCacheEntry> MoveNext()
        {
            if (!hasNext())
            {
                throw new IndexOutOfRangeException();
            }

            return cacheIterator.peekNext();
        }

        public void Remove()
        {
            throw new InvalidOperationException();
        }

        public KeyValue<Bytes, LRUCacheEntry> peekNext()
        {
            if (!hasNext())
            {
                throw new IndexOutOfRangeException();
            }
            return cacheIterator.peekNext();
        }

        bool IEnumerator.MoveNext()
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}