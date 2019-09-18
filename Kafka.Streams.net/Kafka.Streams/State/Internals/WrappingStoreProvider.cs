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
using Kafka.Streams.Errors;
using Kafka.Streams.State.Interfaces;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.State.Internals
{
    /**
     * Provides a wrapper over multiple underlying {@link StateStoreProvider}s
     */
    public class WrappingStoreProvider : IStateStoreProvider
    {
        private readonly List<IStateStoreProvider> storeProviders;

        WrappingStoreProvider(List<IStateStoreProvider> storeProviders)
        {
            this.storeProviders = storeProviders;
        }

        /**
         * Provides access to {@link org.apache.kafka.streams.processor.IStateStore}s accepted
         * by {@link QueryableStoreType#accepts(IStateStore)}
         * @param storeName  name of the store
         * @param type      The {@link QueryableStoreType}
         * @param       The type of the Store, for example, {@link org.apache.kafka.streams.state.ReadOnlyKeyValueStore}
         * @return  a List of all the stores with the storeName and are accepted by {@link QueryableStoreType#accepts(IStateStore)}
         */
        public List<T> stores<T>(
            string storeName,
            IQueryableStoreType<T> type)
        {
            List<T> allStores = new List<T>();
            foreach (IStateStoreProvider provider in storeProviders)
            {
                List<T> stores = provider.stores(storeName, type);
                allStores.AddRange(stores);
            }
            if (!allStores.Any())
            {
                throw new InvalidStateStoreException("The state store, " + storeName + ", may have migrated to another instance.");
            }

            return allStores;
        }
    }
}