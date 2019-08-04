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

namespace Kafka.Streams.State.Internals;

using Kafka.Streams.KeyValue;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.WindowStoreIterator;



class KeyValueIterators
{

    private static class EmptyKeyValueIterator<K, V> : KeyValueIterator<K, V>
{

        
        public void close()
{
        }

        
        public K peekNextKey()
{
            throw new NoSuchElementException();
        }

        
        public bool hasNext()
{
            return false;
        }

        
        public KeyValue<K, V> next()
{
            throw new NoSuchElementException();
        }

        
        public void Remove()
{
        }
    }

    private static class EmptyWindowStoreIterator<V> : EmptyKeyValueIterator<long, V>
        : WindowStoreIterator<V>
{
    }

    private static KeyValueIterator EMPTY_ITERATOR = new EmptyKeyValueIterator();
    private static WindowStoreIterator EMPTY_WINDOW_STORE_ITERATOR = new EmptyWindowStoreIterator();


    
    staticKeyValueIterator<K, V> emptyIterator()
{
        return (KeyValueIterator<K, V>) EMPTY_ITERATOR;
    }

    
    static <V> WindowStoreIterator<V> emptyWindowStoreIterator()
{
        return (WindowStoreIterator<V>) EMPTY_WINDOW_STORE_ITERATOR;
    }
}
