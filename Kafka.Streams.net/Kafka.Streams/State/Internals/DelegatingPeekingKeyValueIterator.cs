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



    /**
     * Optimized {@link KeyValueIterator} used when the same element could be peeked multiple times.
     */
    public class DelegatingPeekingKeyValueIterator<K, V> : KeyValueIterator<K, V>, PeekingKeyValueIterator<K, V>
    {
        private KeyValueIterator<K, V> underlying;
        private string storeName;
        private KeyValue<K, V> next;

        private volatile bool open = true;

        public DelegatingPeekingKeyValueIterator(string storeName, KeyValueIterator<K, V> underlying)
        {
            this.storeName = storeName;
            this.underlying = underlying;
        }

        public override synchronized K peekNextKey()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }
            return next.key;
        }

        public override synchronized void close()
        {
            underlying.close();
            open = false;
        }

        public override synchronized bool hasNext()
        {
            if (!open)
            {
                throw new InvalidStateStoreException(string.Format("Store %s has closed", storeName));
            }
            if (next != null)
            {
                return true;
            }

            if (!underlying.hasNext())
            {
                return false;
            }

            next = underlying.next();
            return true;
        }

        public override synchronized KeyValue<K, V> next()
    {
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }
            KeyValue<K, V> result = next;
            next = null;
            return result;
        }

        public override void Remove()
        {
            throw new InvalidOperationException("Remove() is not supported in " + GetType().getName());
        }

        public override KeyValue<K, V> peekNext()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }
            return next;
        }
    }
}