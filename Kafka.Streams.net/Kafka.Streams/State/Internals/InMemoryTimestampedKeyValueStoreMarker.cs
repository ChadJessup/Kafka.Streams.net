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
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public class InMemoryTimestampedKeyValueStoreMarker : IKeyValueStore<Bytes, byte[]>, ITimestampedBytesStore
    {
        IKeyValueStore<Bytes, byte[]> wrapped;

        public InMemoryTimestampedKeyValueStoreMarker(IKeyValueStore<Bytes, byte[]> wrapped)
        {
            if (wrapped.persistent())
            {
                throw new System.ArgumentException("Provided store must not be a persistent store, but it is.");
            }
            this.wrapped = wrapped;
        }


        public void init(IProcessorContext<K, V> context,
                         IStateStore root)
        {
            wrapped.init(context, root);
        }


        public void put(Bytes key,
                        byte[] value)
        {
            wrapped.Add(key, value);
        }


        public byte[] putIfAbsent(Bytes key,
                                  byte[] value)
        {
            return wrapped.putIfAbsent(key, value);
        }


        public void putAll(List<KeyValue<Bytes, byte[]>> entries)
        {
            wrapped.putAll(entries);
        }


        public byte[] delete(Bytes key)
        {
            return wrapped.delete(key);
        }


        public byte[] get(Bytes key)
        {
            return wrapped[key];
        }


        public IKeyValueIterator<Bytes, byte[]> range(Bytes from,
                                                     Bytes to)
        {
            return wrapped.range(from, to);
        }


        public IKeyValueIterator<Bytes, byte[]> all()
        {
            return wrapped.all();
        }


        public long approximateNumEntries()
        {
            return wrapped.approximateNumEntries();
        }


        public void flush()
        {
            wrapped.flush();
        }


        public void close()
        {
            wrapped.close();
        }


        public bool isOpen()
        {
            return wrapped.isOpen();
        }


        public string name => wrapped.name;

        public bool persistent()
        {
            return false;
        }
    }
}
