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

namespace Kafka.Streams.State.Internals
{
    public class InMemoryTimestampedWindowStoreMarker
    : IWindowStore<Bytes, byte[]>, ITimestampedBytesStore
    {

        private IWindowStore<Bytes, byte[]> wrapped;

        public InMemoryTimestampedWindowStoreMarker(IWindowStore<Bytes, byte[]> wrapped)
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


        public void put(Bytes key,
                        byte[] value,
                        long windowStartTimestamp)
        {
            wrapped.Add(key, value, windowStartTimestamp);
        }


        public byte[] fetch(Bytes key,
                            long time)
        {
            return wrapped.fetch(key, time);
        }



        public WindowStoreIterator<byte[]> fetch(Bytes key,
                                                 long timeFrom,
                                                 long timeTo)
        {
            return wrapped.fetch(key, timeFrom, timeTo);
        }



        public IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from,
                                                               Bytes to,
                                                               long timeFrom,
                                                               long timeTo)
        {
            return wrapped.fetch(from, to, timeFrom, timeTo);
        }



        public IKeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom,
                                                                  long timeTo)
        {
            return wrapped.fetchAll(timeFrom, timeTo);
        }


        public IKeyValueIterator<Windowed<Bytes>, byte[]> all()
        {
            return wrapped.all();
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
