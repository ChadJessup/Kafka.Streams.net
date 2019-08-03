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
package org.apache.kafka.streams.state.internals;

using Kafka.Streams.Errors.InvalidStateStoreException;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.State.QueryableStoreType;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * A wrapper over all of the {@link StateStoreProvider}s in a Topology
 */
public class QueryableStoreProvider
{

    private List<StateStoreProvider> storeProviders;
    private GlobalStateStoreProvider globalStoreProvider;

    public QueryableStoreProvider(List<StateStoreProvider> storeProviders,
                                  GlobalStateStoreProvider globalStateStoreProvider)
{
        this.storeProviders = new ArrayList<>(storeProviders);
        this.globalStoreProvider = globalStateStoreProvider;
    }

    /**
     * Get a composite object wrapping the instances of the {@link IStateStore} with the provided
     * storeName and {@link QueryableStoreType}
     *
     * @param storeName          name of the store
     * @param queryableStoreType accept stores passing {@link QueryableStoreType#accepts(IStateStore)}
     * @param <T>                The expected type of the returned store
     * @return A composite object that wraps the store instances.
     */
    public <T> T getStore(string storeName,
                          QueryableStoreType<T> queryableStoreType)
{
        List<T> globalStore = globalStoreProvider.stores(storeName, queryableStoreType);
        if (!globalStore.isEmpty())
{
            return queryableStoreType.create(new WrappingStoreProvider(singletonList(globalStoreProvider)), storeName);
        }
        List<T> allStores = new ArrayList<>();
        for (StateStoreProvider storeProvider : storeProviders)
{
            allStores.addAll(storeProvider.stores(storeName, queryableStoreType));
        }
        if (allStores.isEmpty())
{
            throw new InvalidStateStoreException("The state store, " + storeName + ", may have migrated to another instance.");
        }
        return queryableStoreType.create(
                new WrappingStoreProvider(storeProviders),
                storeName);
    }
}
