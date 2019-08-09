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
using Kafka.Common.Utils;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Internals.Kafka.Streams.Internals;
using Kafka.Streams.KStream;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream
{
    /**
     * Used to describe how a {@link IStateStore} should be materialized.
     * You can either provide a custom {@link IStateStore} backend through one of the provided methods accepting a supplier
     * or use the default RocksDb backends by providing just a store name.
     * <p>
     * For example, you can read a topic as {@link KTable} and force a state store materialization to access the content
     * via Interactive Queries API:
     * <pre>{@code
     * StreamsBuilder builder = new StreamsBuilder();
     * KTable<int, int> table = builder.table(
     *   "topicName",
     *   Materialized.As("queryable-store-name"));
     * }</pre>
     *
     * @param type of record key
     * @param type of record value
     * @param type of state store (note: state stores always have key/value types {@code <Bytes,byte[]>}
     *
     * @see org.apache.kafka.streams.state.Stores
     */
    public class Materialized<K, V, S>
            where S : IStateStore
    {
        protected IStoreSupplier<S> storeSupplier;
        protected string storeName;
        public ISerde<V> valueSerde;
        public ISerde<K> keySerde;
        protected bool loggingEnabled = true;
        public bool cachingEnabled { get; private set; } = true;
        protected Dictionary<string, string> topicConfig = new Dictionary<string, string>();
        public TimeSpan retention { get; private set; }

        private Materialized(IStoreSupplier<S> storeSupplier)
        {
            this.storeSupplier = storeSupplier;
        }

        private Materialized(string storeName)
        {
            this.storeName = storeName;
        }

        /**
         * Copy constructor.
         * @param materialized  the {@link Materialized} instance to copy.
         */
        protected Materialized(Materialized<K, V, S> materialized)
        {
            this.storeSupplier = materialized.storeSupplier;
            this.storeName = materialized.storeName;
            this.keySerde = materialized.keySerde;
            this.valueSerde = materialized.valueSerde;
            this.loggingEnabled = materialized.loggingEnabled;
            this.cachingEnabled = materialized.cachingEnabled;
            this.topicConfig = materialized.topicConfig;
            this.retention = materialized.retention;
        }

        /**
         * Materialize a {@link IStateStore} with the given name.
         *
         * @param storeName  the name of the underlying {@link KTable} state store; valid characters are ASCII
         * alphanumerics, '.', '_' and '-'.
         * @param       key type of the store
         * @param       value type of the store
         * @param       type of the {@link IStateStore}
         * @return a new {@link Materialized} instance with the given storeName
         */
        public static Materialized<K, V, S> As(string storeName)
        {
            Named.validate(storeName);

            return new Materialized<K, V, S>(storeName);
        }

        /**
         * Materialize a {@link WindowStore} using the provided {@link WindowBytesStoreSupplier}.
         *
         * Important: Custom sues are allowed here, but they should respect the retention contract:
         * Window stores are required to retain windows at least as long as (window size + window grace period).
         * Stores constructed via {@link org.apache.kafka.streams.state.Stores} already satisfy this contract.
         *
         * @param supplier the {@link WindowBytesStoreSupplier} used to materialize the store
         * @param      key type of the store
         * @param      value type of the store
         * @return a new {@link Materialized} instance with the given supplier
         */
        public static Materialized<K, V, IWindowStore<Bytes, byte[]>> As(IWindowBytesStoreSupplier supplier)
        {
            supplier = supplier ?? throw new System.ArgumentNullException("supplier can't be null", nameof(supplier));
            return new Materialized<K, V, IWindowStore<Bytes, byte[]>>(supplier);
        }

        /**
         * Materialize a {@link ISessionStore} using the provided {@link SessionBytesStoreSupplier}.
         *
         * Important: Custom sues are allowed here, but they should respect the retention contract:
         * Session stores are required to retain windows at least as long as (session inactivity gap + session grace period).
         * Stores constructed via {@link org.apache.kafka.streams.state.Stores} already satisfy this contract.
         *
         * @param supplier the {@link SessionBytesStoreSupplier} used to materialize the store
         * @param      key type of the store
         * @param      value type of the store
         * @return a new {@link Materialized} instance with the given sup
         * plier
         */
        public static Materialized<K, V, ISessionStore<Bytes, byte[]>> As(ISessionBytesStoreSupplier supplier)
        {
            supplier = supplier ?? throw new ArgumentNullException("supplier can't be null", nameof(supplier));

            return new Materialized<K, V, ISessionStore<Bytes, byte[]>>(supplier);
        }

        /**
         * Materialize a {@link KeyValueStore} using the provided {@link KeyValueBytesStoreSupplier}.
         *
         * @param supplier the {@link KeyValueBytesStoreSupplier} used to materialize the store
         * @param      key type of the store
         * @param      value type of the store
         * @return a new {@link Materialized} instance with the given supplier
         */
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> As(IKeyValueBytesStoreSupplier supplier)
        {
            supplier = supplier ?? throw new System.ArgumentNullException("supplier can't be null", nameof(supplier));
            return new Materialized<K, V, IKeyValueStore<Bytes, byte[]>>(supplier);
        }

        /**
         * Materialize a {@link IStateStore} with the provided key and value {@link Serde}s.
         * An internal name will be used for the store.
         *
         * @param keySerde      the key {@link Serde} to use. If the {@link Serde} is null, then the default key
         *                      serde from configs will be used
         * @param valueSerde    the value {@link Serde} to use. If the {@link Serde} is null, then the default value
         *                      serde from configs will be used
         * @param           key type
         * @param           value type
         * @param           store type
         * @return a new {@link Materialized} instance with the given key and value serdes
         */
        public static Materialized<K, V, S> with(
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
        {
            return new Materialized<K, V, S>((string)null).withKeySerde(keySerde).withValueSerde(valueSerde);
        }

        /**
         * Set the valueSerde the materialized {@link IStateStore} will use.
         *
         * @param valueSerde the value {@link Serde} to use. If the {@link Serde} is null, then the default value
         *                   serde from configs will be used. If the serialized bytes is null for put operations,
         *                   it is treated as delete operation
         * @return itself
         */
        public Materialized<K, V, S> withValueSerde(ISerde<V> valueSerde)
        {
            this.valueSerde = valueSerde;
            return this;
        }

        /**
         * Set the keySerde the materialize {@link IStateStore} will use.
         * @param keySerde  the key {@link Serde} to use. If the {@link Serde} is null, then the default key
         *                  serde from configs will be used
         * @return itself
         */
        public Materialized<K, V, S> withKeySerde(ISerde<K> keySerde)
        {
            this.keySerde = keySerde;
            return this;
        }

        /**
         * Indicates that a changelog should be created for the store. The changelog will be created
         * with the provided configs.
         * <p>
         * Note: Any unrecognized configs will be ignored.
         * @param config    any configs that should be applied to the changelog
         * @return itself
         */
        public Materialized<K, V, S> withLoggingEnabled(Dictionary<string, string> config)
        {
            loggingEnabled = true;
            this.topicConfig = config;
            return this;
        }

        /**
         * Disable change logging for the materialized {@link IStateStore}.
         * @return itself
         */
        public Materialized<K, V, S> withLoggingDisabled()
        {
            loggingEnabled = false;
            this.topicConfig.Clear();
            return this;
        }

        /**
         * Enable caching for the materialized {@link IStateStore}.
         * @return itself
         */
        public Materialized<K, V, S> withCachingEnabled()
        {
            cachingEnabled = true;
            return this;
        }

        /**
         * Disable caching for the materialized {@link IStateStore}.
         * @return itself
         */
        public Materialized<K, V, S> withCachingDisabled()
        {
            cachingEnabled = false;
            return this;
        }

        /**
         * Configure retention period for window and session stores. Ignored for key/value stores.
         *
         * Overridden by pre-configured store suppliers
         * ({@link Materialized#As(SessionBytesStoreSupplier)} or {@link Materialized#As(WindowBytesStoreSupplier)}).
         *
         * Note that the retention period must be at least long enough to contain the windowed data's entire life cycle,
         * from window-start through window-end, and for the entire grace period.
         *
         * @param retention the retention time
         * @return itself
         * @throws ArgumentException if retention is negative or can't be represented as {@code long milliseconds}
         */
        public Materialized<K, V, S> withRetention(TimeSpan retention)
        {
            string msgPrefix = ApiUtils.prepareMillisCheckFailMsgPrefix(retention, "retention");
            long retenationMs = ApiUtils.validateMillisecondDuration(retention, msgPrefix);

            if (retenationMs < 0)
            {
                throw new System.ArgumentException("Retention must not be negative.");
            }

            this.retention = retention;

            return this;
        }
    }
}
