using System;
using System.Collections.Generic;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Sessions;
using Kafka.Streams.State.Windowed;

namespace Kafka.Streams.KStream
{
    public class Materialized
    {
        /**
         * Materialize a {@link IStateStore} with the given Name.
         *
         * @param storeName  the Name of the underlying {@link KTable} state store; valid characters are ASCII
         * alphanumerics, '.', '_' and '-'.
         * @param       key type of the store
         * @param       value type of the store
         * @param       type of the {@link IStateStore}
         * @return a new {@link Materialized} instance with the given storeName
         */
        public static Materialized<K, V, ISessionStore<Bytes, byte[]>> As<K, V>(string storeName)
        {
            Named.Validate(storeName);

            return new Materialized<K, V, ISessionStore<Bytes, byte[]>>(storeName);
        }


        /**
         * Materialize a {@link IStateStore} with the given Name.
         *
         * @param storeName  the Name of the underlying {@link KTable} state store; valid characters are ASCII
         * alphanumerics, '.', '_' and '-'.
         * @param       key type of the store
         * @param       value type of the store
         * @param       type of the {@link IStateStore}
         * @return a new {@link Materialized} instance with the given storeName
         */
        public static Materialized<K, V, S> As<K, V, S>(string storeName)
            where S : IStateStore
        {
            Named.Validate(storeName);

            return new Materialized<K, V, S>(storeName);
        }

        /**
         * Materialize a {@link IStateStore} with the provided key and value {@link Serde}s.
         * An internal Name will be used for the store.
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
        public static Materialized<K, V, S> With<K, V, S>(
            ISerde<K>? keySerde,
            ISerde<V>? valueSerde)
                where S : IStateStore
            => new Materialized<K, V, S>(storeName: null)
                .WithKeySerde(keySerde)
                .WithValueSerde(valueSerde);

        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> With<K, V>(
            ISerde<K>? keySerde,
            ISerde<V>? valueSerde)
            => new Materialized<K, V, IKeyValueStore<Bytes, byte[]>>(storeName: null)
                .WithKeySerde(keySerde)
                .WithValueSerde(valueSerde);

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
        public static Materialized<K, V, IWindowStore<Bytes, byte[]>> As<K, V>(IWindowBytesStoreSupplier supplier)
        {
            supplier = supplier ?? throw new ArgumentNullException(nameof(supplier));

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
        public static Materialized<K, V, ISessionStore<Bytes, byte[]>> As<K, V>(ISessionBytesStoreSupplier supplier)
        {
            supplier = supplier ?? throw new ArgumentNullException(nameof(supplier));

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
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> As<K, V>(IKeyValueBytesStoreSupplier supplier)
        {
            return new Materialized<K, V, IKeyValueStore<Bytes, byte[]>>(supplier);
        }
    }

    /**
     * Used to describe how a {@link IStateStore} should be materialized.
     * You can either provide a custom {@link IStateStore} backend through one of the provided methods accepting a supplier
     * or use the default RocksDb backends by providing just a store Name.
     * <p>
     * For example, you can read a topic as {@link KTable} and force a state store materialization to access the content
     * via Interactive Queries API:
     * <pre>{@code
     * StreamsBuilder builder = new StreamsBuilder();
     * KTable<int, int> table = builder.table(
     *   "topicName",
     *   Materialized.As("queryable-store-Name"));
     * }</pre>
     *
     * @param type of record key
     * @param type of record value
     * @param type of state store (note: state stores always have key/value types {@code <Bytes,byte[]>}
     *
     * @see org.apache.kafka.streams.state.Stores
     */
    public class Materialized<K, V, S> : Materialized
        where S : IStateStore
    {
        public Materialized(IStoreSupplier<S> storeSupplier)
        {
            this.StoreSupplier = storeSupplier;
        }

        public Materialized(string? storeName)
        {
            this.StoreName = storeName;
        }

        public IStoreSupplier<S>? StoreSupplier { get; set; }
        public virtual string? StoreName { get; protected set; }
        public ISerde<V>? ValueSerde { get; protected set; }
        public ISerde<K>? KeySerde { get; protected set; }
        public bool LoggingEnabled { get; protected set; } = true;
        public bool CachingEnabled { get; protected set; } = true;
        protected Dictionary<string, string> TopicConfig { get; private set; } = new Dictionary<string, string>();
        public TimeSpan Retention { get; protected set; }

        /**
         * Copy constructor.
         * @param materialized  the {@link Materialized} instance to copy.
         */
        protected Materialized(Materialized<K, V, S> materialized)
        {
            if (materialized is null)
            {
                throw new ArgumentNullException(nameof(materialized));
            }

            this.LoggingEnabled = materialized.LoggingEnabled;
            this.CachingEnabled = materialized.CachingEnabled;
            this.StoreSupplier = materialized.StoreSupplier;
            this.TopicConfig = materialized.TopicConfig;
            this.ValueSerde = materialized.ValueSerde;
            this.StoreName = materialized.StoreName;
            this.Retention = materialized.Retention;
            this.KeySerde = materialized.KeySerde;
        }

        /**
         * Set the valueSerde the materialized {@link IStateStore} will use.
         *
         * @param valueSerde the value {@link Serde} to use. If the {@link Serde} is null, then the default value
         *                   serde from configs will be used. If the serialized bytes is null for Put operations,
         *                   it is treated as delete operation
         * @return itself
         */
        public Materialized<K, V, S> WithValueSerde(ISerde<V>? valueSerde)
        {
            this.ValueSerde = valueSerde;

            return this;
        }

        /**
         * Set the keySerde the materialize {@link IStateStore} will use.
         * @param keySerde  the key {@link Serde} to use. If the {@link Serde} is null, then the default key
         *                  serde from configs will be used
         * @return itself
         */
        public Materialized<K, V, S> WithKeySerde(ISerde<K>? keySerde)
        {
            this.KeySerde = keySerde;

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
        public Materialized<K, V, S> WithLoggingEnabled(Dictionary<string, string> config)
        {
            this.LoggingEnabled = true;
            this.TopicConfig = config;

            return this;
        }

        /**
         * Disable change logging for the materialized {@link IStateStore}.
         * @return itself
         */
        public Materialized<K, V, S> WithLoggingDisabled()
        {
            this.LoggingEnabled = false;
            this.TopicConfig.Clear();

            return this;
        }

        /**
         * Enable caching for the materialized {@link IStateStore}.
         * @return itself
         */
        public Materialized<K, V, S> WithCachingEnabled()
        {
            this.CachingEnabled = true;

            return this;
        }

        /**
         * Disable caching for the materialized {@link IStateStore}.
         * @return itself
         */
        public Materialized<K, V, S> WithCachingDisabled()
        {
            this.CachingEnabled = false;

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
        public Materialized<K, V, S> WithRetention(TimeSpan retention)
        {
            var msgPrefix = ApiUtils.PrepareMillisCheckFailMsgPrefix(retention, "retention");
            var retenationMs = ApiUtils.ValidateMillisecondDuration(retention, msgPrefix);

            if (retenationMs < TimeSpan.Zero)
            {
                throw new ArgumentException("Retention must not be negative.");
            }

            this.Retention = retention;

            return this;
        }
    }
}
