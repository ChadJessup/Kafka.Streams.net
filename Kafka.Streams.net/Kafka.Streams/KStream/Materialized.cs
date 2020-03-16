using Kafka.Streams.Interfaces;
using Kafka.Streams.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Sessions;
using Kafka.Streams.State.Window;
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
    public class Materialized<K, V>
    {
        public Materialized()
        {
        }

        public Materialized(string? storeName)
            : this()
            => this.StoreName = storeName;

        public virtual string? StoreName { get; protected set; }
        public ISerde<V>? ValueSerde { get; protected set; }
        public ISerde<K>? KeySerde { get; protected set; }
        public bool LoggingEnabled { get; protected set; } = true;
        public bool cachingEnabled { get; protected set; } = true;
        protected Dictionary<string, string> TopicConfig { get; set; } = new Dictionary<string, string>();
        public TimeSpan retention { get; protected set; }

        /**
         * Set the valueSerde the materialized {@link IStateStore} will use.
         *
         * @param valueSerde the value {@link Serde} to use. If the {@link Serde} is null, then the default value
         *                   serde from configs will be used. If the serialized bytes is null for put operations,
         *                   it is treated as delete operation
         * @return itself
         */
        public virtual Materialized<K, V> WithValueSerde(ISerde<V>? valueSerde)
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
        public virtual Materialized<K, V> WithKeySerde(ISerde<K> keySerde)
        {
            this.KeySerde = keySerde;

            return this;
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
        public Materialized<K, V> With(
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
            => new Materialized<K, V>()
                .WithKeySerde(keySerde)
                .WithValueSerde(valueSerde);

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
        public static Materialized<K, V> As(string storeName)
        {
            Named.Validate(storeName);

            return new Materialized<K, V>(storeName);
        }
    }

    public class Materialized<K, V, S> : Materialized<K, V>
            where S : IStateStore
    {
        public IStoreSupplier<S> StoreSupplier { get; set; }

        private Materialized(IStoreSupplier<S> storeSupplier)
            : base()
        {
            this.StoreSupplier = storeSupplier;
        }

        private Materialized(string? storeName)
            : base(storeName)
        {
        }

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

            this.StoreSupplier = materialized.StoreSupplier;
            this.StoreName = materialized.StoreName;
            this.KeySerde = materialized.KeySerde;
            this.ValueSerde = materialized.ValueSerde;
            this.LoggingEnabled = materialized.LoggingEnabled;
            this.cachingEnabled = materialized.cachingEnabled;
            this.TopicConfig = materialized.TopicConfig;
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
            Named.Validate(storeName);

            return new Materialized<K, V, S>(storeName);
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
        public static new Materialized<K, V, S> With(
            ISerde<K>? keySerde,
            ISerde<V>? valueSerde)
            => new Materialized<K, V, S>(storeName: null)
                .WithKeySerde(keySerde)
                .WithValueSerde(valueSerde);

        /**
         * Set the valueSerde the materialized {@link IStateStore} will use.
         *
         * @param valueSerde the value {@link Serde} to use. If the {@link Serde} is null, then the default value
         *                   serde from configs will be used. If the serialized bytes is null for put operations,
         *                   it is treated as delete operation
         * @return itself
         */
        public new Materialized<K, V, S> WithValueSerde(ISerde<V>? valueSerde)
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
        public new Materialized<K, V, S> WithKeySerde(ISerde<K>? keySerde)
        {
            this.KeySerde = keySerde;

            return this;
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
        public static Materialized<K, V, ISessionStore<Bytes, byte[]>> As(ISessionBytesStoreSupplier supplier)
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
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> As(IKeyValueBytesStoreSupplier supplier)
        {
            supplier = supplier ?? throw new ArgumentNullException(nameof(supplier));

            return new Materialized<K, V, IKeyValueStore<Bytes, byte[]>>(supplier);
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
            LoggingEnabled = true;
            this.TopicConfig = config;

            return this;
        }

        /**
         * Disable change logging for the materialized {@link IStateStore}.
         * @return itself
         */
        public Materialized<K, V, S> WithLoggingDisabled()
        {
            LoggingEnabled = false;
            this.TopicConfig.Clear();

            return this;
        }

        /**
         * Enable caching for the materialized {@link IStateStore}.
         * @return itself
         */
        public Materialized<K, V, S> WithCachingEnabled()
        {
            cachingEnabled = true;

            return this;
        }

        /**
         * Disable caching for the materialized {@link IStateStore}.
         * @return itself
         */
        public Materialized<K, V, S> WithCachingDisabled()
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
        public Materialized<K, V, S> WithRetention(TimeSpan retention)
        {
            var msgPrefix = ApiUtils.prepareMillisCheckFailMsgPrefix(retention, "retention");
            var retenationMs = ApiUtils.validateMillisecondDuration(retention, msgPrefix);

            if (retenationMs < TimeSpan.Zero)
            {
                throw new ArgumentException("Retention must not be negative.");
            }

            this.retention = retention;

            return this;
        }
    }
}
