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
using System.Collections.Generic;

namespace Kafka.Streams.State.Interfaces
{
    /**
     * Provides access to {@link IStateStore}s that have been created
     * as part of the {@link org.apache.kafka.streams.processor.Internals.ProcessorTopology}.
     * To get access to custom stores developers should implement {@link QueryableStoreType}.
     * @see QueryableStoreTypes
     */
    public interface IStateStoreProvider
    {
        /**
         * Find instances of IStateStore that are accepted by {@link QueryableStoreType#accepts} and
         * have the provided storeName.
         *
         * @param storeName             name of the store
         * @param queryableStoreType    filter stores based on this queryableStoreType
         * @param                   The type of the Store
         * @return  List of the instances of the store in this topology. Empty List if not found
         */
        List<T> stores<T>(string storeName, IQueryableStoreType<T> queryableStoreType);
    }
}