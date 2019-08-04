/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
namespace Kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.IStateStore;
import org.apache.kafka.streams.state.StoreSupplier;

import java.time.Duration;
import java.util.Map;

public class MaterializedInternal<K, V, S : IStateStore> : Materialized<K, V, S> {

    private  bool queriable;

    public MaterializedInternal( Materialized<K, V, S> materialized)
{
        this(materialized, null, null);
    }

    public MaterializedInternal( Materialized<K, V, S> materialized,
                                 InternalNameProvider nameProvider,
                                 string generatedStorePrefix)
{
        super(materialized);

        // if storeName is not provided, the corresponding KTable would never be queryable;
        // but we still need to provide an internal name for it in case we materialize.
        queriable = storeName() != null;
        if (!queriable && nameProvider != null)
{
            storeName = nameProvider.newStoreName(generatedStorePrefix);
        }
    }

    public string queryableStoreName()
{
        return queriable ? storeName() : null;
    }

    public string storeName()
{
        if (storeSupplier != null)
{
            return storeSupplier.name();
        }
        return storeName;
    }

    public StoreSupplier<S> storeSupplier()
{
        return storeSupplier;
    }

    public ISerde<K> keySerde()
{
        return keySerde;
    }

    public ISerde<V> valueSerde()
{
        return valueSerde;
    }

    public bool loggingEnabled()
{
        return loggingEnabled;
    }

    Map<string, string> logConfig()
{
        return topicConfig;
    }

    public bool cachingEnabled()
{
        return cachingEnabled;
    }

    Duration retention()
{
        return retention;
    }
}
