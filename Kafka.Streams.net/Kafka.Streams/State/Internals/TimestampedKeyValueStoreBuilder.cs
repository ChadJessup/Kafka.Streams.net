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
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Interfaces;
using Kafka.Streams.State.Interfaces;
using System;

namespace Kafka.Streams.State.Internals
{
    public class TimestampedKeyValueStoreBuilder<K, V>
        : AbstractStoreBuilder<K, ValueAndTimestamp<V>, ITimestampedKeyValueStore<K, V>>
    {
        private readonly IKeyValueBytesStoreSupplier storeSupplier;

        public TimestampedKeyValueStoreBuilder(
            IKeyValueBytesStoreSupplier storeSupplier,
            ISerde<K> keySerde,
            ISerde<V> valueSerde,
            ITime time)
            : base(
                storeSupplier.name,
                keySerde,
                valueSerde == null ? null : new ValueAndTimestampSerde<V>(valueSerde),
                time)
        {
            storeSupplier = storeSupplier ?? throw new ArgumentNullException(nameof(storeSupplier));

            this.storeSupplier = storeSupplier;
        }

        public override ITimestampedKeyValueStore<K, V> build()
        {
            IKeyValueStore<Bytes, byte[]> store = storeSupplier.get();

            if (!(store is ITimestampedBytesStore))
            {
                if (store.persistent())
                {
                    store = null; // new KeyValueToTimestampedKeyValueByteStoreAdapter(store);
                }
                else
                {
                    store = null; // new InMemoryTimestampedKeyValueStoreMarker(store);
                }
            }

            return null;
            //new MeteredTimestampedKeyValueStore<>(
            //    maybeWrapCaching(maybeWrapLogging(store)),
            //    storeSupplier.metricsScope(),
            //    time,
            //    keySerde,
            //    valueSerde);
        }

        private IKeyValueStore<Bytes, byte[]> maybeWrapCaching(IKeyValueStore<Bytes, byte[]> inner)
        {
            if (!enableCaching)
            {
                return inner;
            }

            return null;// new CachingKeyValueStore(inner);
        }

        private IKeyValueStore<Bytes, byte[]> maybeWrapLogging(IKeyValueStore<Bytes, byte[]> inner)
        {
            if (!enableLogging)
            {
                return inner;
            }

            return null;// new ChangeLoggingTimestampedKeyValueBytesStore(inner);
        }
    }
}