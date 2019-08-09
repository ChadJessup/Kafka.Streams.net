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
using Kafka.Streams.State;

namespace Kafka.Streams.State.Internals
{
    public class ReadOnlyWindowStoreFacade<K, V> : ReadOnlyWindowStore<K, V>
    {
        protected ITimestampedWindowStore<K, V> inner;

        protected ReadOnlyWindowStoreFacade(ITimestampedWindowStore<K, V> store)
        {
            inner = store;
        }

        public override V fetch(K key,
                       long time)
        {
            return getValueOrNull(inner.fetch(key, time));
        }



        public WindowStoreIterator<V> fetch(K key,
                                            long timeFrom,
                                            long timeTo)
        {
            return new WindowStoreIteratorFacade<>(inner.fetch(key, timeFrom, timeTo));
        }

        public override WindowStoreIterator<V> fetch(K key,
                                            DateTime from,
                                            DateTime to)
        {
            return new WindowStoreIteratorFacade<>(inner.fetch(key, from, to));
        }



        public IKeyValueIterator<Windowed<K>, V> fetch(K from,
                                                      K to,
                                                      long timeFrom,
                                                      long timeTo)
        {
            return new KeyValueIteratorFacade<>(inner.fetch(from, to, timeFrom, timeTo));
        }

        public override IKeyValueIterator<Windowed<K>, V> fetch(K from,
                                                      K to,
                                                      DateTime fromTime,
                                                      DateTime toTime)
        {
            return new KeyValueIteratorFacade<>(inner.fetch(from, to, fromTime, toTime));
        }

        public IKeyValueIterator<Windowed<K>, V> fetchAll(long timeFrom,
                                                         long timeTo)
        {
            return new KeyValueIteratorFacade<>(inner.fetchAll(timeFrom, timeTo));
        }

        public override IKeyValueIterator<Windowed<K>, V> fetchAll(DateTime from,
                                                         DateTime to)
        {
            IKeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> innerIterator = inner.fetchAll(from, to);
            return new KeyValueIteratorFacade<>(innerIterator);
        }

        public override IKeyValueIterator<Windowed<K>, V> all()
        {
            IKeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> innerIterator = inner.all();
            return new KeyValueIteratorFacade<>(innerIterator);
        }
    }
}