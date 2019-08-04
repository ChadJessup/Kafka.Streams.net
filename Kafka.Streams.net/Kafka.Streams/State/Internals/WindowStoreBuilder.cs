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
package org.apache.kafka.streams.state.internals;

using Kafka.Common.serialization.Serde;
using Kafka.Common.Utils.Bytes;
using Kafka.Common.Utils.Time;
using Kafka.Streams.State.WindowBytesStoreSupplier;
using Kafka.Streams.State.WindowStore;

public WindowStoreBuilder<K, V> : AbstractStoreBuilder<K, V, WindowStore<K, V>>
{

    private WindowBytesStoreSupplier storeSupplier;

    public WindowStoreBuilder(WindowBytesStoreSupplier storeSupplier,
                              ISerde<K> keySerde,
                              ISerde<V> valueSerde,
                              ITime time)
{
        base(storeSupplier.name(), keySerde, valueSerde, time);
        this.storeSupplier = storeSupplier;
    }

    public override WindowStore<K, V> build()
{
        return new MeteredWindowStore<>(
            maybeWrapCaching(maybeWrapLogging(storeSupplier())],
            storeSupplier.windowSize(),
            storeSupplier.metricsScope(),
            time,
            keySerde,
            valueSerde);
    }

    private WindowStore<Bytes, byte[]> maybeWrapCaching(WindowStore<Bytes, byte[]> inner)
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

    private WindowStore<Bytes, byte[]> maybeWrapLogging(WindowStore<Bytes, byte[]> inner)
{
        if (!enableLogging)
{
            return inner;
        }
        return new ChangeLoggingWindowBytesStore(inner, storeSupplier.retainDuplicates());
    }

    public long retentionPeriod()
{
        return storeSupplier.retentionPeriod();
    }
}
