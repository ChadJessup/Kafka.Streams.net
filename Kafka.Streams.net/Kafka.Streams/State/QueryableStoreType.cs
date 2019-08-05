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
namespace Kafka.Streams.State;

using Kafka.Streams.KafkaStreams;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.State.Internals.StateStoreProvider;

/**
 * Used to enable querying of custom {@link IStateStore} types via the {@link KafkaStreams} API.
 *
 * @param The store type
 * @see QueryableStoreTypes
 */
public interface QueryableStoreType<T>
{

    /**
     * Called when searching for {@link IStateStore}s to see if they
     * match the type expected by implementors of this interface.
     *
     * @param stateStore    The stateStore
     * @return true if it is a match
     */
    bool accepts(IStateStore stateStore);

    /**
     * Create an instance of {@code T} (usually a facade) that developers can use
     * to query the underlying {@link IStateStore}s.
     *
     * @param storeProvider     provides access to all the underlying IStateStore instances
     * @param storeName         The name of the Store
     * @return a read-only interface over a {@code IStateStore}
     *        (cf. {@link org.apache.kafka.streams.state.QueryableStoreTypes.KeyValueStoreType})
     */
    T create(StateStoreProvider storeProvider,
             string storeName);
}