using Kafka.Common;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Internals;
using Kafka.Streams.KStream;
using Kafka.Streams.NullModels;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Sessions;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.State.Windowed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;

namespace Kafka.Streams.State
{
    /**
     * Factory for creating state stores in Kafka Streams.
     * <p>
     * When using the high-level DSL, i.e., {@link org.apache.kafka.streams.StreamsBuilder StreamsBuilder}, users create
     * {@link StoreSupplier}s that can be further customized via
     * {@link org.apache.kafka.streams.kstream.Materialized Materialized}.
     * For example, a topic read as {@link org.apache.kafka.streams.kstream.KTable KTable} can be materialized into an
     * in-memory store with custom key/value serdes and caching disabled:
     * <pre>{@code
     * StreamsBuilder builder = new StreamsBuilder();
     * KeyValueBytesStoreSupplier storeSupplier = Stores.InMemoryKeyValueStore("queryable-store-Name");
     * KTable<long,string> table = builder.table(
     *   "topicName",
     *   Materialized.<long,string>As(storeSupplier)
     *               .withKeySerde(Serdes.Long())
     *               .withValueSerde(Serdes.string())
     *               .withCachingDisabled());
     * }</pre>
     * When using the IProcessor API, i.e., {@link org.apache.kafka.streams.Topology Topology}, users create
     * {@link StoreBuilder}s that can be attached to {@link org.apache.kafka.streams.processor.IProcessor IProcessor}s.
     * For example, you can create a {@link org.apache.kafka.streams.kstream.Windowed windowed} RocksDb store with custom
     * changelog topic configuration like:
     * <pre>{@code
     * Topology topology = new Topology();
     * topology.AddProcessor("processorName", ...);
     *
     * Dictionary<string,string> topicConfig = new Dictionary<>();
     * StoreBuilder<WindowStore<int, long>> storeBuilder = Stores
     *   .windowStoreBuilder(
     *     Stores.PersistentWindowStore("queryable-store-Name", ...),
     *     Serdes.int(),
     *     Serdes.Long())
     *   .withLoggingEnabled(topicConfig);
     *
     * topology.AddStateStore(storeBuilder, "processorName");
     * }</pre>
     */
    public class StoresFactory : IStoresFactory
    {
        private readonly ILogger<StoresFactory> logger;
        private readonly IServiceProvider services;

        public StoresFactory(ILogger<StoresFactory> logger, IServiceProvider services)
        {
            this.logger = logger;
            this.services = services;
        }

        /**
         * Create a Persistent {@link KeyValueBytesStoreSupplier}.
         * <p>
         * This store supplier can be passed into a {@link #keyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}.
         * If you want to create a {@link TimestampedKeyValueStore} you should use
         * {@link #PersistentTimestampedKeyValueStore(string)} to create a store supplier instead.
         *
         * @param Name  Name of the store (cannot be {@code null})
         * @return an instance of a {@link KeyValueBytesStoreSupplier} that can be used
         * to build a Persistent key-value store
         */
        public IKeyValueBytesStoreSupplier PersistentKeyValueStore(string Name)
        {
            Name = Name ?? throw new ArgumentNullException(nameof(Name));

            // Since libraries provide this functionality, we
            // want to handle the case where no library was
            // added.
            // If null, provide the Null supplier instead.
            var keyValueBytesStoreSupplier = this.services.GetService<IKeyValueBytesStoreSupplier>()
                ?? this.services.GetRequiredService<NullStoreSupplier>();

            keyValueBytesStoreSupplier.SetName(Name);
            return keyValueBytesStoreSupplier;
        }

        /**
         * Create a Persistent {@link KeyValueBytesStoreSupplier}.
         * <p>
         * This store supplier can be passed into a
         * {@link #TimestampedKeyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}.
         * If you want to create a {@link KeyValueStore} you should use
         * {@link #PersistentKeyValueStore(string)} to create a store supplier instead.
         *
         * @param Name  Name of the store (cannot be {@code null})
         * @return an instance of a {@link KeyValueBytesStoreSupplier} that can be used
         * to build a Persistent key-(timestamp/value) store
         */
        public ITimestampedKeyValueBytesStoreSupplier PersistentTimestampedKeyValueStore(string Name)
        {
            var timeStampedKeyValueBytesStoreSupplier = this.services.GetService<ITimestampedKeyValueBytesStoreSupplier>();

            // Since libraries provide this functionality, we
            // want to handle the case where no library was
            // added.
            // If null, provide the Null supplier instead.
            if (timeStampedKeyValueBytesStoreSupplier == null)
            {
                timeStampedKeyValueBytesStoreSupplier = this.services.GetRequiredService<NullStoreSupplier>();
            }

            timeStampedKeyValueBytesStoreSupplier.SetName(Name);

            return timeStampedKeyValueBytesStoreSupplier;
        }

        //        /**
        //         * Create an in-memory {@link KeyValueBytesStoreSupplier}.
        //         * <p>
        //         * This store supplier can be passed into a {@link #keyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}
        //         * or {@link #TimestampedKeyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}.
        //         *
        //         * @param Name  Name of the store (cannot be {@code null})
        //         * @return an instance of a {@link KeyValueBytesStoreSupplier} than can be used to
        //         * build an in-memory store
        //         */
        //        public KeyValueBytesStoreSupplier InMemoryKeyValueStore(string Name)
        //        {
        //            Name = Name ?? throw new ArgumentNullException(nameof(Name));
        //            return new KeyValueBytesStoreSupplier()
        //            {


        //            public string Name
        //            {
        //                return Name;
        //            }


        //            public IKeyValueStore<Bytes, byte[]> get()
        //            {
        //                return new InMemoryKeyValueStore(Name);
        //            }


        //            public string metricsScope()
        //            {
        //                return "in-memory-state";
        //            }
        //        };
        //    }

        //    /**
        //     * Create a LRU Map {@link KeyValueBytesStoreSupplier}.
        //     * <p>
        //     * This store supplier can be passed into a {@link #keyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}
        //     * or {@link #TimestampedKeyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}.
        //     *
        //     * @param Name          Name of the store (cannot be {@code null})
        //     * @param maxCacheSize  maximum number of items in the LRU (cannot be negative)
        //     * @return an instance of a {@link KeyValueBytesStoreSupplier} that can be used to build
        //     * an LRU Map based store
        //     */
        //    public KeyValueBytesStoreSupplier lruMap(string Name, int maxCacheSize)
        //    {
        //        Name = Name ?? throw new ArgumentNullException(nameof(Name));
        //        if (maxCacheSize < 0)
        //        {
        //            throw new System.ArgumentException("maxCacheSize cannot be negative");
        //        }
        //        return new KeyValueBytesStoreSupplier()
        //        {


        //            public string Name
        //        {
        //            return Name;
        //        }


        //        public IKeyValueStore<Bytes, byte[]> get()
        //        {
        //            return new MemoryNavigableLRUCache(Name, maxCacheSize);
        //        }


        //        public string metricsScope()
        //        {
        //            return "in-memory-lru-state";
        //        }
        //    };
        //}

        /**
         * Create a Persistent {@link WindowBytesStoreSupplier}.
         *
         * @param Name                  Name of the store (cannot be {@code null})
         * @param retentionPeriod      .Length of time to retain data in the store (cannot be negative)
         *                              (note that the retention period must be at least long enough to contain the
         *                              windowed data's entire life cycle, from window-start through window-end,
         *                              and for the entire grace period)
         * @param numSegments           number of db segments (cannot be zero or negative)
         * @param windowSize            size of the windows that are stored (cannot be negative). Note: the window size
         *                              is not stored with the records, so this value is used to compute the keys that
         *                              the store returns. No effort is made to validate this parameter, so you must be
         *                              careful to set it the same as the windowed keys you're actually storing.
         * @param retainDuplicates      whether or not to retain duplicates.
         * @return an instance of {@link WindowBytesStoreSupplier}
         * @deprecated since 2.1 Use {@link Stores#PersistentWindowStore(string, TimeSpan, TimeSpan, bool)} instead
         */
        [Obsolete] // continuing to support Windows#maintainMs/segmentInterval in fallback mode
        public IWindowBytesStoreSupplier PersistentWindowStore(
            string Name,
            TimeSpan retentionPeriod,
            int numSegments,
            TimeSpan windowSize,
            bool retainDuplicates)
        {
            if (numSegments < 2)
            {
                throw new ArgumentException("numSegments cannot be smaller than 2");
            }

            var legacySegmentInterval = TimeSpan.FromMilliseconds(Math.Max(retentionPeriod.TotalMilliseconds / (numSegments - 1), 60_000L));

            return this.PersistentWindowStore(
                Name,
                retentionPeriod,
                windowSize,
                retainDuplicates,
                legacySegmentInterval,
                false);
        }

        /**
         * Create a Persistent {@link WindowBytesStoreSupplier}.
         * <p>
         * This store supplier can be passed into a {@link #windowStoreBuilder(WindowBytesStoreSupplier, Serde, Serde)}.
         * If you want to create a {@link TimestampedWindowStore} you should use
         * {@link #PersistentTimestampedWindowStore(string, TimeSpan, TimeSpan, bool)} to create a store supplier instead.
         *
         * @param Name                  Name of the store (cannot be {@code null})
         * @param retentionPeriod      .Length of time to retain data in the store (cannot be negative)
         *                              (note that the retention period must be at least long enough to contain the
         *                              windowed data's entire life cycle, from window-start through window-end,
         *                              and for the entire grace period)
         * @param windowSize            size of the windows (cannot be negative)
         * @param retainDuplicates      whether or not to retain duplicates.
         * @return an instance of {@link WindowBytesStoreSupplier}
         * @throws ArgumentException if {@code retentionPeriod} or {@code windowSize} can't be represented as {@code long milliseconds}
         */
        public IWindowBytesStoreSupplier PersistentWindowStore(
            string Name,
            TimeSpan retentionPeriod,
            TimeSpan windowSize,
            bool retainDuplicates)
        {
            return this.PersistentWindowStore(
                Name,
                retentionPeriod,
                windowSize,
                retainDuplicates,
                false);
        }

        /**
         * Create a Persistent {@link WindowBytesStoreSupplier}.
         * <p>
         * This store supplier can be passed into a
         * {@link #timestampedWindowStoreBuilder(WindowBytesStoreSupplier, Serde, Serde)}.
         * If you want to create a {@link WindowStore} you should use
         * {@link #PersistentWindowStore(string, TimeSpan, TimeSpan, bool)} to create a store supplier instead.
         *
         * @param Name                  Name of the store (cannot be {@code null})
         * @param retentionPeriod      .Length of time to retain data in the store (cannot be negative)
         *                              (note that the retention period must be at least long enough to contain the
         *                              windowed data's entire life cycle, from window-start through window-end,
         *                              and for the entire grace period)
         * @param windowSize            size of the windows (cannot be negative)
         * @param retainDuplicates      whether or not to retain duplicates.
         * @return an instance of {@link WindowBytesStoreSupplier}
         * @throws ArgumentException if {@code retentionPeriod} or {@code windowSize} can't be represented as {@code long milliseconds}
         */
        public IWindowBytesStoreSupplier PersistentTimestampedWindowStore(
            string Name,
            TimeSpan retentionPeriod,
            TimeSpan windowSize,
            bool retainDuplicates)
            => this.PersistentWindowStore(
                Name,
                retentionPeriod,
                windowSize,
                retainDuplicates,
                timestampedStore: true);

        // TODO: Clean this up, back and forth from TimeSpan to MS isn't useful
        // but Java kept things as MS for more things.
        private IWindowBytesStoreSupplier PersistentWindowStore(
            string Name,
            TimeSpan retentionPeriod,
            TimeSpan windowSize,
            bool retainDuplicates,
            bool timestampedStore)
        {
            Name = Name ?? throw new ArgumentNullException(nameof(Name));

            var rpMsgPrefix = ApiUtils.PrepareMillisCheckFailMsgPrefix(retentionPeriod, "retentionPeriod");
            var retentionMs = ApiUtils.ValidateMillisecondDuration(retentionPeriod, rpMsgPrefix);
            var wsMsgPrefix = ApiUtils.PrepareMillisCheckFailMsgPrefix(windowSize, "windowSize");
            var windowSizeMs = ApiUtils.ValidateMillisecondDuration(windowSize, wsMsgPrefix);

            var defaultSegmentInterval = TimeSpan.FromMilliseconds(Math.Max(retentionMs.TotalMilliseconds / 2, 60_000L));

            return this.PersistentWindowStore(
                Name,
                retentionMs,
                windowSizeMs,
                retainDuplicates,
                defaultSegmentInterval,
                timestampedStore);
        }

        private IWindowBytesStoreSupplier PersistentWindowStore(
            string Name,
            TimeSpan retentionPeriod,
            TimeSpan windowSize,
            bool retainDuplicates,
            TimeSpan segmentInterval,
            bool timestampedStore)
        {
            Name = Name ?? throw new ArgumentNullException(nameof(Name));

            if (retentionPeriod.TotalMilliseconds < 0L)
            {
                throw new ArgumentException("retentionPeriod cannot be negative");
            }

            if (windowSize < TimeSpan.Zero)
            {
                throw new ArgumentException("windowSize cannot be negative");
            }

            if (segmentInterval.TotalMilliseconds < 1L)
            {
                throw new ArgumentException("segmentInterval cannot be zero or negative");
            }

            if (windowSize > retentionPeriod)
            {
                throw new ArgumentException(
                    $"The retention period of the window store " +
                    $"{Name} must be no smaller than its window size. " +
                    $"Got size=[{windowSize}], retention=[{retentionPeriod}]");
            }

            var windowBytesStoreSupplier = this.services.GetService<IWindowBytesStoreSupplier>()
                ?? this.services.GetRequiredService<NullStoreSupplier>();

            return windowBytesStoreSupplier;
            //new RocksDbWindowBytesStoreSupplier(
            //    Name,
            //    retentionPeriod,
            //    segmentInterval,
            //    windowSize,
            //    retainDuplicates,
            //    timestampedStore);
        }

        /**
         * Create an in-memory {@link WindowBytesStoreSupplier}.
         * <p>
         * This store supplier can be passed into a {@link #windowStoreBuilder(WindowBytesStoreSupplier, Serde, Serde)} or
         * {@link #timestampedWindowStoreBuilder(WindowBytesStoreSupplier, Serde, Serde)}.
         *
         * @param Name                  Name of the store (cannot be {@code null})
         * @param retentionPeriod      .Length of time to retain data in the store (cannot be negative)
         *                              Note that the retention period must be at least long enough to contain the
         *                              windowed data's entire life cycle, from window-start through window-end,
         *                              and for the entire grace period.
         * @param windowSize            size of the windows (cannot be negative)
         * @return an instance of {@link WindowBytesStoreSupplier}
         * @throws ArgumentException if {@code retentionPeriod} or {@code windowSize} can't be represented as {@code long milliseconds}
         */
        public IWindowBytesStoreSupplier InMemoryWindowStore(
            string Name,
            TimeSpan retentionPeriod,
            TimeSpan windowSize,
            bool retainDuplicates)
        {
            Name = Name ?? throw new ArgumentNullException(nameof(Name));

            string repartitionPeriodErrorMessagePrefix = ApiUtils.PrepareMillisCheckFailMsgPrefix(retentionPeriod, "retentionPeriod");
            var retentionMs = ApiUtils.ValidateMillisecondDuration(retentionPeriod, repartitionPeriodErrorMessagePrefix);
            if (retentionMs.TotalMilliseconds < 0L)
            {
                throw new ArgumentException("retentionPeriod cannot be negative");
            }

            string windowSizeErrorMessagePrefix = ApiUtils.PrepareMillisCheckFailMsgPrefix(windowSize, "windowSize");
            var windowSizeMs = ApiUtils.ValidateMillisecondDuration(windowSize, windowSizeErrorMessagePrefix);

            if (windowSizeMs.TotalMilliseconds < 0L)
            {
                throw new ArgumentException("windowSize cannot be negative");
            }

            if (windowSizeMs > retentionMs)
            {
                throw new ArgumentException("The retention period of the window store "
                    + Name + " must be no smaller than its window size. Got size=["
                    + windowSize + "], retention=[" + retentionPeriod + "]");
            }

            return new NullStoreSupplier();
            //new InMemoryWindowBytesStoreSupplier(Name, retentionMs, windowSizeMs, retainDuplicates);
        }

        /**
         * Create a Persistent {@link SessionBytesStoreSupplier}.
         *
         * @param Name              Name of the store (cannot be {@code null})
         * @param retentionPeriodMs.Length ot time to retain data in the store (cannot be negative)
         *                          (note that the retention period must be at least long enough to contain the
         *                          windowed data's entire life cycle, from window-start through window-end,
         *                          and for the entire grace period)
         * @return an instance of a {@link  SessionBytesStoreSupplier}
         * @deprecated since 2.1 Use {@link Stores#PersistentSessionStore(string, TimeSpan)} instead
         */
        [Obsolete] // continuing to support Windows#maintainMs/segmentInterval in fallback mode
        public ISessionBytesStoreSupplier PersistentSessionStore(
            string Name,
            long retentionPeriodMs)
        {
            Name = Name ?? throw new ArgumentNullException(nameof(Name));

            if (retentionPeriodMs < 0)
            {
                throw new ArgumentException("retentionPeriod cannot be negative");
            }

            var persistentSessionStore = this.services.GetService<ISessionBytesStoreSupplier>()
                ?? this.services.GetRequiredService<NullStoreSupplier>();

            return null;// new RocksDbSessionBytesStoreSupplier(Name, retentionPeriodMs);
        }

        /**
         * Create a Persistent {@link SessionBytesStoreSupplier}.
         *
         * @param Name              Name of the store (cannot be {@code null})
         * @param retentionPeriod  .Length ot time to retain data in the store (cannot be negative)
         *                          Note that the retention period must be at least long enough to contain the
         *                          windowed data's entire life cycle, from window-start through window-end,
         *                          and for the entire grace period.
         * @return an instance of a {@link  SessionBytesStoreSupplier}
         */
        // public ISessionBytesStoreSupplier PersistentSessionStore(
        //     string Name,
        //     TimeSpan retentionPeriod)
        // {
        //     var msgPrefix = ApiUtils.PrepareMillisCheckFailMsgPrefix(retentionPeriod, "retentionPeriod");
        // 
        //     return PersistentSessionStore(Name, ApiUtils.ValidateMillisecondDuration(retentionPeriod, msgPrefix));
        // }

        /**
         * Create an in-memory {@link SessionBytesStoreSupplier}.
         *
         * @param Name              Name of the store (cannot be {@code null})
         * @param retentionPeriod  .Length ot time to retain data in the store (cannot be negative)
         *                          Note that the retention period must be at least long enough to contain the
         *                          windowed data's entire life cycle, from window-start through window-end,
         *                          and for the entire grace period.
         * @return an instance of a {@link  SessionBytesStoreSupplier}
         */
        public ISessionBytesStoreSupplier InMemorySessionStore(string Name, TimeSpan retentionPeriod)
        {
            Name = Name ?? throw new ArgumentNullException(nameof(Name));

            var msgPrefix = ApiUtils.PrepareMillisCheckFailMsgPrefix(retentionPeriod, "retentionPeriod");
            retentionPeriod = ApiUtils.ValidateMillisecondDuration(retentionPeriod, msgPrefix);

            if (retentionPeriod < TimeSpan.Zero)
            {
                throw new ArgumentException("retentionPeriod cannot be negative");
            }

            return new InMemorySessionBytesStoreSupplier(Name, retentionPeriod);
        }

        /**
         * Creates a {@link StoreBuilder} that can be used to build a {@link KeyValueStore}.
         * <p>
         * The provided supplier should <strong>not</strong> be a supplier for
         * {@link TimestampedKeyValueStore TimestampedKeyValueStores}.
         *
         * @param supplier      a {@link KeyValueBytesStoreSupplier} (cannot be {@code null})
         * @param keySerde      the key serde to use
         * @param valueSerde    the value serde to use; if the serialized bytes is {@code null} for Put operations,
         *                      it is treated as delete
         * @param           key type
         * @param           value type
         * @return an instance of a {@link StoreBuilder} that can build a {@link KeyValueStore}
         */
        public IStoreBuilder<IKeyValueStore<K, V>> KeyValueStoreBuilder<K, V>(
            KafkaStreamsContext context,
            IKeyValueBytesStoreSupplier supplier,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
        {
            if (context is null)
            {
                throw new ArgumentNullException(nameof(context));
            }

            supplier = supplier ?? throw new ArgumentNullException(nameof(supplier));

            return new KeyValueStoreBuilder<K, V>(
                context,
                supplier,
                keySerde,
                valueSerde);
        }

        /**
         * Creates a {@link StoreBuilder} that can be used to build a {@link TimestampedKeyValueStore}.
         * <p>
         * The provided supplier should <strong>not</strong> be a supplier for
         * {@link KeyValueStore KeyValueStores}. For this case, passed in timestamps will be dropped and not stored in the
         * key-value-store. On read, no valid timestamp but a dummy timestamp will be returned.
         *
         * @param supplier      a {@link KeyValueBytesStoreSupplier} (cannot be {@code null})
         * @param keySerde      the key serde to use
         * @param valueSerde    the value serde to use; if the serialized bytes is {@code null} for Put operations,
         *                      it is treated as delete
         * @param           key type
         * @param           value type
         * @return an instance of a {@link StoreBuilder} that can build a {@link KeyValueStore}
         */
        public IStoreBuilder<ITimestampedKeyValueStore<K, V>> TimestampedKeyValueStoreBuilder<K, V>(
            KafkaStreamsContext context,
            IKeyValueBytesStoreSupplier supplier,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
        {
            supplier = supplier ?? throw new ArgumentNullException(nameof(supplier));

            return new TimestampedKeyValueStoreBuilder<K, V>(
                context,
                supplier,
                keySerde,
                valueSerde);
        }

        /**
         * Creates a {@link StoreBuilder} that can be used to build a {@link WindowStore}.
         * <p>
         * The provided supplier should <strong>not</strong> be a supplier for
         * {@link TimestampedWindowStore TimestampedWindowStores}.
         *
         * @param supplier      a {@link WindowBytesStoreSupplier} (cannot be {@code null})
         * @param keySerde      the key serde to use
         * @param valueSerde    the value serde to use; if the serialized bytes is {@code null} for Put operations,
         *                      it is treated as delete
         * @param           key type
         * @param           value type
         * @return an instance of {@link StoreBuilder} than can build a {@link WindowStore}
         */
        public IStoreBuilder<IWindowStore<K, V>> WindowStoreBuilder<K, V>(
            KafkaStreamsContext context,
            IWindowBytesStoreSupplier supplier,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
        {
            supplier = supplier ?? throw new ArgumentNullException(nameof(supplier));

            return new WindowStoreBuilder<K, V>(
                context,
                supplier,
                keySerde,
                valueSerde);
        }

        /**
         * Creates a {@link StoreBuilder} that can be used to build a {@link TimestampedWindowStore}.
         * <p>
         * The provided supplier should <strong>not</strong> be a supplier for
         * {@link WindowStore WindowStores}. For this case, passed in timestamps will be dropped and not stored in the
         * windows-store. On read, no valid timestamp but a dummy timestamp will be returned.
         *
         * @param supplier      a {@link WindowBytesStoreSupplier} (cannot be {@code null})
         * @param keySerde      the key serde to use
         * @param valueSerde    the value serde to use; if the serialized bytes is {@code null} for Put operations,
         *                      it is treated as delete
         * @param           key type
         * @param           value type
         * @return an instance of {@link StoreBuilder} that can build a {@link TimestampedWindowStore}
         */
        public IStoreBuilder<ITimestampedWindowStore<K, V>> TimestampedWindowStoreBuilder<K, V>(
            KafkaStreamsContext context,
            IWindowBytesStoreSupplier supplier,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
        {
            supplier = supplier ?? throw new ArgumentNullException(nameof(supplier));

            return new TimestampedWindowStoreBuilder<K, V>(
                context,
                supplier,
                keySerde,
                valueSerde);
        }

        /**
         * Creates a {@link StoreBuilder} that can be used to build a {@link ISessionStore}.
         *
         * @param supplier      a {@link SessionBytesStoreSupplier} (cannot be {@code null})
         * @param keySerde      the key serde to use
         * @param valueSerde    the value serde to use; if the serialized bytes is {@code null} for Put operations,
         *                      it is treated as delete
         * @param           key type
         * @param           value type
         * @return an instance of {@link StoreBuilder} than can build a {@link ISessionStore}
         */
        public IStoreBuilder<ISessionStore<K, V>> SessionStoreBuilder<K, V>(
            KafkaStreamsContext context,
            ISessionBytesStoreSupplier supplier,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
            where V : class
        {
            supplier = supplier ?? throw new ArgumentNullException(nameof(supplier));

            return new SessionStoreBuilder<K, V>(context, supplier, keySerde, valueSerde);
        }
    }
}
