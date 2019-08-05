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
using Kafka.Streams.State.KeyValueBytesStoreSupplier;
using Kafka.Streams.State.KeyValueStore;



public class KeyValueStoreBuilder<K, V> : AbstractStoreBuilder<K, V, IKeyValueStore<K, V>>
{

    private KeyValueBytesStoreSupplier storeSupplier;

    public KeyValueStoreBuilder(KeyValueBytesStoreSupplier storeSupplier,
                                ISerde<K> keySerde,
                                ISerde<V> valueSerde,
                                ITime time)
        : base(storeSupplier.name(), keySerde, valueSerde, time)
    {
        storeSupplier = storeSupplier ?? throw new System.ArgumentNullException("bytesStoreSupplier can't be null", nameof(storeSupplier));
        this.storeSupplier = storeSupplier;
    }

    public override IKeyValueStore<K, V> build()
    {
        return new MeteredKeyValueStore<>(
            maybeWrapCaching(maybeWrapLogging(storeSupplier())],
            storeSupplier.metricsScope(),
            time,
            keySerde,
            valueSerde);
    }

    private IKeyValueStore<Bytes, byte[]> maybeWrapCaching(IKeyValueStore<Bytes, byte[]> inner)
    {
        if (!enableCaching)
        {
            return inner;
        }
        return new CachingKeyValueStore(inner);
    }

    private IKeyValueStore<Bytes, byte[]> maybeWrapLogging(IKeyValueStore<Bytes, byte[]> inner)
    {
        if (!enableLogging)
        {
            return inner;
        }
        return new ChangeLoggingKeyValueBytesStore(inner);
    }
}
