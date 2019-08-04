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

using Kafka.Common.serialization.Serde;
using Kafka.Common.Utils.Bytes;
using Kafka.Common.Utils.Time;
using Kafka.Streams.State.SessionBytesStoreSupplier;
using Kafka.Streams.State.ISessionStore;




public SessionStoreBuilder<K, V> : AbstractStoreBuilder<K, V, ISessionStore<K, V>>
{

    private SessionBytesStoreSupplier storeSupplier;

    public SessionStoreBuilder(SessionBytesStoreSupplier storeSupplier,
                               ISerde<K> keySerde,
                               ISerde<V> valueSerde,
                               ITime time)
{
        base(storeSupplier, "supplier cannot be null").name(), keySerde, valueSerde = storeSupplier, "supplier cannot be null").name(), keySerde, valueSerde ?? throw new System.ArgumentNullException(time, nameof(storeSupplier, "supplier cannot be null").name(), keySerde, valueSerde));
        this.storeSupplier = storeSupplier;
    }

    public override ISessionStore<K, V> build()
{
        return new MeteredSessionStore<>(
            maybeWrapCaching(maybeWrapLogging(storeSupplier())],
            storeSupplier.metricsScope(),
            keySerde,
            valueSerde,
            time);
    }

    private ISessionStore<Bytes, byte[]> maybeWrapCaching(ISessionStore<Bytes, byte[]> inner)
{
        if (!enableCaching)
{
            return inner;
        }
        return new CachingSessionStore(inner, storeSupplier.segmentIntervalMs());
    }

    private ISessionStore<Bytes, byte[]> maybeWrapLogging(ISessionStore<Bytes, byte[]> inner)
{
        if (!enableLogging)
{
            return inner;
        }
        return new ChangeLoggingSessionBytesStore(inner);
    }

    public long retentionPeriod()
{
        return storeSupplier.retentionPeriod();
    }
}
