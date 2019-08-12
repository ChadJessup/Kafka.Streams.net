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

namespace Kafka.Streams.State.Internals
{
    public class TimestampedWindowStoreBuilder<K, V>
        : AbstractStoreBuilder<K, ValueAndTimestamp<V>, ITimestampedWindowStore<K, V>>
    {

        private IWindowBytesStoreSupplier storeSupplier;

        public TimestampedWindowStoreBuilder(IWindowBytesStoreSupplier storeSupplier,
                                             ISerde<K> keySerde,
                                             ISerde<V> valueSerde,
                                             ITime time)
            : base(storeSupplier.name, keySerde, valueSerde == null ? null : new ValueAndTimestampSerde<>(valueSerde), time)
        {
            storeSupplier = storeSupplier ?? throw new System.ArgumentNullException("bytesStoreSupplier can't be null", nameof(storeSupplier));
            this.storeSupplier = storeSupplier;
        }

        public override ITimestampedWindowStore<K, V> build()
        {
            IWindowStore<Bytes, byte[]> store = storeSupplier.get();
            if (!(store is ITimestampedBytesStore))
            {
                if (store.persistent())
                {
                    store = new WindowToTimestampedWindowByteStoreAdapter(store);
                }
                else
                {
                    store = new InMemoryTimestampedWindowStoreMarker(store);
                }
            }
            return new MeteredTimestampedWindowStore<>(
                maybeWrapCaching(maybeWrapLogging(store)),
                storeSupplier.windowSize(),
                storeSupplier.metricsScope(),
                time,
                keySerde,
                valueSerde);
        }

        private IWindowStore<Bytes, byte[]> maybeWrapCaching(IWindowStore<Bytes, byte[]> inner)
        {
            if (!enableCaching)
            {
                return inner;
            }
            return new CachingWindowStore(
                inner,
                storeSupplier.windowSize(),
                storeSupplier.segmentIntervalMs());
        }

        private IWindowStore<Bytes, byte[]> maybeWrapLogging(IWindowStore<Bytes, byte[]> inner)
        {
            if (!enableLogging)
            {
                return inner;
            }
            return new ChangeLoggingTimestampedWindowBytesStore(inner, storeSupplier.retainDuplicates());
        }

        public long retentionPeriod()
        {
            return storeSupplier.retentionPeriod();
        }
    }
}