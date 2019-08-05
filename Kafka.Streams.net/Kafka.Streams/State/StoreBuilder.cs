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

using Kafka.Streams.Processor.IStateStore;



/**
 * Build a {@link IStateStore} wrapped with optional caching and logging.
 * @param  the type of store to build
 */
public interface StoreBuilder<T : IStateStore>
{

    /**
     * Enable caching on the store.
     * @return  this
     */
    StoreBuilder<T> withCachingEnabled();

    /**
     * Disable caching on the store.
     * @return  this
     */
    StoreBuilder<T> withCachingDisabled();

    /**
     * Maintain a changelog for any changes made to the store.
     * Use the provided config to set the config of the changelog topic.
     * @param config  config applied to the changelog topic
     * @return this
     */
    StoreBuilder<T> withLoggingEnabled(Dictionary<string, string> config);

    /**
     * Disable the changelog for store built by this {@link StoreBuilder}.
     * This will turn off fault-tolerance for your store.
     * By default the changelog is enabled.
     * @return this
     */
    StoreBuilder<T> withLoggingDisabled();

    /**
     * Build the store as defined by the builder.
     *
     * @return the built {@link IStateStore}
     */
    T build();

    /**
     * Returns a Map containing any log configs that will be used when creating the changelog for the {@link IStateStore}.
     * <p>
     * Note: any unrecognized configs will be ignored by the Kafka brokers.
     *
     * @return Map containing any log configs to be used when creating the changelog for the {@link IStateStore}
     * If {@code loggingEnabled} returns false, this function will always return an empty map
     */
    Dictionary<string, string> logConfig();

    /**
     * @return {@code true} if the {@link IStateStore} should have logging enabled
     */
    bool loggingEnabled();

    /**
     * Return the name of this state store builder.
     * This must be a valid Kafka topic name; valid characters are ASCII alphanumerics, '.', '_' and '-'.
     *
     * @return the name of this state store builder
     */
    string name();

}
