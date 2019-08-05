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
    public class WindowToTimestampedWindowByteStoreAdapter : WindowStore<Bytes, byte[]>
    {
        WindowStore<Bytes, byte[]> store;

        WindowToTimestampedWindowByteStoreAdapter(WindowStore<Bytes, byte[]> store)
        {
            if (!store.persistent())
            {
                throw new System.ArgumentException("Provided store must be a persistent store, but it is not.");
            }
            this.store = store;
        }

        public override void put(Bytes key,
                        byte[] valueWithTimestamp)
        {
            store.Add(key, valueWithTimestamp == null ? null : rawValue(valueWithTimestamp));
        }

        public override void put(Bytes key,
                        byte[] valueWithTimestamp,
                        long windowStartTimestamp)
        {
            store.Add(key, valueWithTimestamp == null ? null : rawValue(valueWithTimestamp), windowStartTimestamp);
        }

        public override byte[] fetch(Bytes key,
                            long time)
        {
            return convertToTimestampedFormat(store.fetch(key, time));
        }



        public WindowStoreIterator<byte[]> fetch(Bytes key,
                                                 long timeFrom,
                                                 long timeTo)
        {
            return new WindowToTimestampedWindowIteratorAdapter(store.fetch(key, timeFrom, timeTo));
        }

        public override WindowStoreIterator<byte[]> fetch(Bytes key,
                                                 Instant from,
                                                 Instant to)
        {
            return new WindowToTimestampedWindowIteratorAdapter(store.fetch(key, from, to));
        }



        public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from,
                                                               Bytes to,
                                                               long timeFrom,
                                                               long timeTo)
        {
            return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.fetch(from, to, timeFrom, timeTo));
        }

        public override KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from,
                                                               Bytes to,
                                                               Instant fromTime,
                                                               Instant toTime)
        {
            return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.fetch(from, to, fromTime, toTime));
        }

        public override KeyValueIterator<Windowed<Bytes>, byte[]> all()
        {
            return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.all());
        }



        public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom,
                                                                  long timeTo)
        {
            return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.fetchAll(timeFrom, timeTo));
        }

        public override KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(Instant from,
                                                                  Instant to)
        {
            return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.fetchAll(from, to));
        }

        public override string name()
        {
            return store.name();
        }

        public override void init(IProcessorContext context,
                         IStateStore root)
        {
            store.init(context, root);
        }

        public override void flush()
        {
            store.flush();
        }

        public override void close()
        {
            store.close();
        }

        public override bool persistent()
        {
            return true;
        }

        public override bool isOpen()
        {
            return store.isOpen();
        }


        private static class WindowToTimestampedWindowIteratorAdapter
        : KeyValueToTimestampedKeyValueIteratorAdapter<long>
        : WindowStoreIterator<byte[]>
        {

            WindowToTimestampedWindowIteratorAdapter(KeyValueIterator<long, byte[]> innerIterator)
            {
                base(innerIterator);
            }
        }

    }
}