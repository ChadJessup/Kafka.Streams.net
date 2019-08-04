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

using Kafka.Streams.Errors.InvalidStateStoreException;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.State.QueryableStoreType;






/**
 * A wrapper over all of the {@link StateStoreProvider}s in a Topology
 */
public QueryableStoreProvider
{

    private List<StateStoreProvider> storeProviders;
    private GlobalStateStoreProvider globalStoreProvider;

    public QueryableStoreProvider(List<StateStoreProvider> storeProviders,
                                  GlobalStateStoreProvider globalStateStoreProvider)
{
        this.storeProviders = new List<>(storeProviders);
        this.globalStoreProvider = globalStateStoreProvider;
    }

    /**
     * Get a composite object wrapping the instances of the {@link IStateStore} with the provided
     * storeName and {@link QueryableStoreType}
     *
     * @param storeName          name of the store
     * @param queryableStoreType accept stores passing {@link QueryableStoreType#accepts(IStateStore)}
     * @param                The expected type of the returned store
     * @return A composite object that wraps the store instances.
     */
    public T getStore(string storeName,
                          QueryableStoreType<T> queryableStoreType)
{
        List<T> globalStore = globalStoreProvider.stores(storeName, queryableStoreType);
        if (!globalStore.isEmpty())
{
            return queryableStoreType.create(new WrappingStoreProvider(singletonList(globalStoreProvider)), storeName);
        }
        List<T> allStores = new List<>();
        foreach (StateStoreProvider storeProvider in storeProviders)
{
            allStores.AddAll(storeProvider.stores(storeName, queryableStoreType));
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
