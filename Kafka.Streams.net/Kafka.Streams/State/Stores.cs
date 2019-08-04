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
namespace Kafka.streams.state;

using Kafka.Common.annotation.InterfaceStability;
using Kafka.Common.serialization.Serde;
using Kafka.Common.Utils.Bytes;
using Kafka.Common.Utils.Time;
using Kafka.Streams.internals.ApiUtils;
using Kafka.Streams.State.internals.InMemoryKeyValueStore;
using Kafka.Streams.State.internals.InMemorySessionBytesStoreSupplier;
using Kafka.Streams.State.internals.InMemoryWindowBytesStoreSupplier;
using Kafka.Streams.State.internals.KeyValueStoreBuilder;
using Kafka.Streams.State.internals.MemoryNavigableLRUCache;
using Kafka.Streams.State.internals.RocksDbKeyValueBytesStoreSupplier;
using Kafka.Streams.State.internals.RocksDbSessionBytesStoreSupplier;
using Kafka.Streams.State.internals.RocksDbWindowBytesStoreSupplier;
using Kafka.Streams.State.internals.SessionStoreBuilder;
using Kafka.Streams.State.internals.TimestampedKeyValueStoreBuilder;
using Kafka.Streams.State.internals.TimestampedWindowStoreBuilder;
using Kafka.Streams.State.internals.WindowStoreBuilder;

import java.time.Duration;
import java.util.Objects;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;

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
 * KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore("queryable-store-name");
 * KTable<Long,string> table = builder.table(
 *   "topicName",
 *   Materialized.<Long,string>as(storeSupplier)
 *               .withKeySerde(Serdes.Long())
 *               .withValueSerde(Serdes.string())
 *               .withCachingDisabled());
 * }</pre>
 * When using the Processor API, i.e., {@link org.apache.kafka.streams.Topology Topology}, users create
 * {@link StoreBuilder}s that can be attached to {@link org.apache.kafka.streams.processor.Processor Processor}s.
 * For example, you can create a {@link org.apache.kafka.streams.kstream.Windowed windowed} RocksDB store with custom
 * changelog topic configuration like:
 * <pre>{@code
 * Topology topology = new Topology();
 * topology.addProcessor("processorName", ...);
 *
 * Dictionary<string,string> topicConfig = new HashMap<>();
 * StoreBuilder<WindowStore<Integer, Long>> storeBuilder = Stores
 *   .windowStoreBuilder(
 *     Stores.persistentWindowStore("queryable-store-name", ...),
 *     Serdes.Integer(),
 *     Serdes.Long())
 *   .withLoggingEnabled(topicConfig);
 *
 * topology.addStateStore(storeBuilder, "processorName");
 * }</pre>
 */
@InterfaceStability.Evolving
public class Stores
{

    /**
     * Create a persistent {@link KeyValueBytesStoreSupplier}.
     * <p>
     * This store supplier can be passed into a {@link #keyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}.
     * If you want to create a {@link TimestampedKeyValueStore} you should use
     * {@link #persistentTimestampedKeyValueStore(string)} to create a store supplier instead.
     *
     * @param name  name of the store (cannot be {@code null})
     * @return an instance of a {@link KeyValueBytesStoreSupplier} that can be used
     * to build a persistent key-value store
     */
    public static KeyValueBytesStoreSupplier persistentKeyValueStore(string name)
{
        Objects.requireNonNull(name, "name cannot be null");
        return new RocksDbKeyValueBytesStoreSupplier(name, false);
    }

    /**
     * Create a persistent {@link KeyValueBytesStoreSupplier}.
     * <p>
     * This store supplier can be passed into a
     * {@link #timestampedKeyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}.
     * If you want to create a {@link KeyValueStore} you should use
     * {@link #persistentKeyValueStore(string)} to create a store supplier instead.
     *
     * @param name  name of the store (cannot be {@code null})
     * @return an instance of a {@link KeyValueBytesStoreSupplier} that can be used
     * to build a persistent key-(timestamp/value) store
     */
    public static KeyValueBytesStoreSupplier persistentTimestampedKeyValueStore(string name)
{
        Objects.requireNonNull(name, "name cannot be null");
        return new RocksDbKeyValueBytesStoreSupplier(name, true);
    }

    /**
     * Create an in-memory {@link KeyValueBytesStoreSupplier}.
     * <p>
     * This store supplier can be passed into a {@link #keyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}
     * or {@link #timestampedKeyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}.
     *
     * @param name  name of the store (cannot be {@code null})
     * @return an instance of a {@link KeyValueBytesStoreSupplier} than can be used to
     * build an in-memory store
     */
    public static KeyValueBytesStoreSupplier inMemoryKeyValueStore(string name)
{
        Objects.requireNonNull(name, "name cannot be null");
        return new KeyValueBytesStoreSupplier()
{
            
            public string name()
{
                return name;
            }

            
            public IKeyValueStore<Bytes, byte[]> get()
{
                return new InMemoryKeyValueStore(name);
            }

            
            public string metricsScope()
{
                return "in-memory-state";
            }
        };
    }

    /**
     * Create a LRU Map {@link KeyValueBytesStoreSupplier}.
     * <p>
     * This store supplier can be passed into a {@link #keyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}
     * or {@link #timestampedKeyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}.
     *
     * @param name          name of the store (cannot be {@code null})
     * @param maxCacheSize  maximum number of items in the LRU (cannot be negative)
     * @return an instance of a {@link KeyValueBytesStoreSupplier} that can be used to build
     * an LRU Map based store
     */
    public static KeyValueBytesStoreSupplier lruMap(string name, int maxCacheSize)
{
        Objects.requireNonNull(name, "name cannot be null");
        if (maxCacheSize < 0)
{
            throw new ArgumentException("maxCacheSize cannot be negative");
        }
        return new KeyValueBytesStoreSupplier()
{
            
            public string name()
{
                return name;
            }

            
            public IKeyValueStore<Bytes, byte[]> get()
{
                return new MemoryNavigableLRUCache(name, maxCacheSize);
            }

            
            public string metricsScope()
{
                return "in-memory-lru-state";
            }
        };
    }

    /**
     * Create a persistent {@link WindowBytesStoreSupplier}.
     *
     * @param name                  name of the store (cannot be {@code null})
     * @param retentionPeriod       length of time to retain data in the store (cannot be negative)
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
     * @deprecated since 2.1 Use {@link Stores#persistentWindowStore(string, Duration, Duration, boolean)} instead
     */
    @Deprecated // continuing to support Windows#maintainMs/segmentInterval in fallback mode
    public static WindowBytesStoreSupplier persistentWindowStore(string name,
                                                                 long retentionPeriod,
                                                                 int numSegments,
                                                                 long windowSize,
                                                                 bool retainDuplicates)
{
        if (numSegments < 2)
{
            throw new ArgumentException("numSegments cannot be smaller than 2");
        }

        long legacySegmentInterval = Math.Max(retentionPeriod / (numSegments - 1), 60_000L);

        return persistentWindowStore(
            name,
            retentionPeriod,
            windowSize,
            retainDuplicates,
            legacySegmentInterval,
            false
        );
    }

    /**
     * Create a persistent {@link WindowBytesStoreSupplier}.
     * <p>
     * This store supplier can be passed into a {@link #windowStoreBuilder(WindowBytesStoreSupplier, Serde, Serde)}.
     * If you want to create a {@link TimestampedWindowStore} you should use
     * {@link #persistentTimestampedWindowStore(string, Duration, Duration, boolean)} to create a store supplier instead.
     *
     * @param name                  name of the store (cannot be {@code null})
     * @param retentionPeriod       length of time to retain data in the store (cannot be negative)
     *                              (note that the retention period must be at least long enough to contain the
     *                              windowed data's entire life cycle, from window-start through window-end,
     *                              and for the entire grace period)
     * @param windowSize            size of the windows (cannot be negative)
     * @param retainDuplicates      whether or not to retain duplicates.
     * @return an instance of {@link WindowBytesStoreSupplier}
     * @throws ArgumentException if {@code retentionPeriod} or {@code windowSize} can't be represented as {@code long milliseconds}
     */
    public static WindowBytesStoreSupplier persistentWindowStore(string name,
                                                                 Duration retentionPeriod,
                                                                 Duration windowSize,
                                                                 bool retainDuplicates) throws ArgumentException
{
        return persistentWindowStore(name, retentionPeriod, windowSize, retainDuplicates, false);
    }

    /**
     * Create a persistent {@link WindowBytesStoreSupplier}.
     * <p>
     * This store supplier can be passed into a
     * {@link #timestampedWindowStoreBuilder(WindowBytesStoreSupplier, Serde, Serde)}.
     * If you want to create a {@link WindowStore} you should use
     * {@link #persistentWindowStore(string, Duration, Duration, boolean)} to create a store supplier instead.
     *
     * @param name                  name of the store (cannot be {@code null})
     * @param retentionPeriod       length of time to retain data in the store (cannot be negative)
     *                              (note that the retention period must be at least long enough to contain the
     *                              windowed data's entire life cycle, from window-start through window-end,
     *                              and for the entire grace period)
     * @param windowSize            size of the windows (cannot be negative)
     * @param retainDuplicates      whether or not to retain duplicates.
     * @return an instance of {@link WindowBytesStoreSupplier}
     * @throws ArgumentException if {@code retentionPeriod} or {@code windowSize} can't be represented as {@code long milliseconds}
     */
    public static WindowBytesStoreSupplier persistentTimestampedWindowStore(string name,
                                                                            Duration retentionPeriod,
                                                                            Duration windowSize,
                                                                            bool retainDuplicates) throws ArgumentException
{
        return persistentWindowStore(name, retentionPeriod, windowSize, retainDuplicates, true);
    }

    private static WindowBytesStoreSupplier persistentWindowStore(string name,
                                                                  Duration retentionPeriod,
                                                                  Duration windowSize,
                                                                  bool retainDuplicates,
                                                                  bool timestampedStore)
{
        Objects.requireNonNull(name, "name cannot be null");
        string rpMsgPrefix = prepareMillisCheckFailMsgPrefix(retentionPeriod, "retentionPeriod");
        long retentionMs = ApiUtils.validateMillisecondDuration(retentionPeriod, rpMsgPrefix);
        string wsMsgPrefix = prepareMillisCheckFailMsgPrefix(windowSize, "windowSize");
        long windowSizeMs = ApiUtils.validateMillisecondDuration(windowSize, wsMsgPrefix);

        long defaultSegmentInterval = Math.Max(retentionMs / 2, 60_000L);

        return persistentWindowStore(name, retentionMs, windowSizeMs, retainDuplicates, defaultSegmentInterval, timestampedStore);
    }

    private static WindowBytesStoreSupplier persistentWindowStore(string name,
                                                                  long retentionPeriod,
                                                                  long windowSize,
                                                                  bool retainDuplicates,
                                                                  long segmentInterval,
                                                                  bool timestampedStore)
{
        Objects.requireNonNull(name, "name cannot be null");
        if (retentionPeriod < 0L)
{
            throw new ArgumentException("retentionPeriod cannot be negative");
        }
        if (windowSize < 0L)
{
            throw new ArgumentException("windowSize cannot be negative");
        }
        if (segmentInterval < 1L)
{
            throw new ArgumentException("segmentInterval cannot be zero or negative");
        }
        if (windowSize > retentionPeriod)
{
            throw new ArgumentException("The retention period of the window store "
                + name + " must be no smaller than its window size. Got size=["
                + windowSize + "], retention=[" + retentionPeriod + "]"];
        }

        return new RocksDbWindowBytesStoreSupplier(
            name,
            retentionPeriod,
            segmentInterval,
            windowSize,
            retainDuplicates,
            timestampedStore);
    }

    /**
     * Create an in-memory {@link WindowBytesStoreSupplier}.
     * <p>
     * This store supplier can be passed into a {@link #windowStoreBuilder(WindowBytesStoreSupplier, Serde, Serde)} or
     * {@link #timestampedWindowStoreBuilder(WindowBytesStoreSupplier, Serde, Serde)}.
     *
     * @param name                  name of the store (cannot be {@code null})
     * @param retentionPeriod       length of time to retain data in the store (cannot be negative)
     *                              Note that the retention period must be at least long enough to contain the
     *                              windowed data's entire life cycle, from window-start through window-end,
     *                              and for the entire grace period.
     * @param windowSize            size of the windows (cannot be negative)
     * @return an instance of {@link WindowBytesStoreSupplier}
     * @throws ArgumentException if {@code retentionPeriod} or {@code windowSize} can't be represented as {@code long milliseconds}
     */
    public static WindowBytesStoreSupplier inMemoryWindowStore(string name,
                                                               Duration retentionPeriod,
                                                               Duration windowSize,
                                                               bool retainDuplicates) throws ArgumentException
{
        Objects.requireNonNull(name, "name cannot be null");

        string repartitionPeriodErrorMessagePrefix = prepareMillisCheckFailMsgPrefix(retentionPeriod, "retentionPeriod");
        long retentionMs = ApiUtils.validateMillisecondDuration(retentionPeriod, repartitionPeriodErrorMessagePrefix);
        if (retentionMs < 0L)
{
            throw new ArgumentException("retentionPeriod cannot be negative");
        }

        string windowSizeErrorMessagePrefix = prepareMillisCheckFailMsgPrefix(windowSize, "windowSize");
        long windowSizeMs = ApiUtils.validateMillisecondDuration(windowSize, windowSizeErrorMessagePrefix);
        if (windowSizeMs < 0L)
{
            throw new ArgumentException("windowSize cannot be negative");
        }

        if (windowSizeMs > retentionMs)
{
            throw new ArgumentException("The retention period of the window store "
                + name + " must be no smaller than its window size. Got size=["
                + windowSize + "], retention=[" + retentionPeriod + "]"];
        }

        return new InMemoryWindowBytesStoreSupplier(name, retentionMs, windowSizeMs, retainDuplicates);
    }

    /**
     * Create a persistent {@link SessionBytesStoreSupplier}.
     *
     * @param name              name of the store (cannot be {@code null})
     * @param retentionPeriodMs length ot time to retain data in the store (cannot be negative)
     *                          (note that the retention period must be at least long enough to contain the
     *                          windowed data's entire life cycle, from window-start through window-end,
     *                          and for the entire grace period)
     * @return an instance of a {@link  SessionBytesStoreSupplier}
     * @deprecated since 2.1 Use {@link Stores#persistentSessionStore(string, Duration)} instead
     */
    @Deprecated // continuing to support Windows#maintainMs/segmentInterval in fallback mode
    public static SessionBytesStoreSupplier persistentSessionStore(string name,
                                                                   long retentionPeriodMs)
{
        Objects.requireNonNull(name, "name cannot be null");
        if (retentionPeriodMs < 0)
{
            throw new ArgumentException("retentionPeriod cannot be negative");
        }
        return new RocksDbSessionBytesStoreSupplier(name, retentionPeriodMs);
    }

    /**
     * Create a persistent {@link SessionBytesStoreSupplier}.
     *
     * @param name              name of the store (cannot be {@code null})
     * @param retentionPeriod   length ot time to retain data in the store (cannot be negative)
     *                          Note that the retention period must be at least long enough to contain the
     *                          windowed data's entire life cycle, from window-start through window-end,
     *                          and for the entire grace period.
     * @return an instance of a {@link  SessionBytesStoreSupplier}
     */
    @SuppressWarnings("deprecation") // removing #persistentSessionStore(string name, long retentionPeriodMs) will fix this
    public static SessionBytesStoreSupplier persistentSessionStore(string name,
                                                                   Duration retentionPeriod)
{
        string msgPrefix = prepareMillisCheckFailMsgPrefix(retentionPeriod, "retentionPeriod");
        return persistentSessionStore(name, ApiUtils.validateMillisecondDuration(retentionPeriod, msgPrefix));
    }

    /**
     * Create an in-memory {@link SessionBytesStoreSupplier}.
     *
     * @param name              name of the store (cannot be {@code null})
     * @param retentionPeriod   length ot time to retain data in the store (cannot be negative)
     *                          Note that the retention period must be at least long enough to contain the
     *                          windowed data's entire life cycle, from window-start through window-end,
     *                          and for the entire grace period.
     * @return an instance of a {@link  SessionBytesStoreSupplier}
     */
    public static SessionBytesStoreSupplier inMemorySessionStore(string name, Duration retentionPeriod)
{
        Objects.requireNonNull(name, "name cannot be null");

        string msgPrefix = prepareMillisCheckFailMsgPrefix(retentionPeriod, "retentionPeriod");
        long retentionPeriodMs = ApiUtils.validateMillisecondDuration(retentionPeriod, msgPrefix);
        if (retentionPeriodMs < 0)
{
            throw new ArgumentException("retentionPeriod cannot be negative");
        }
        return new InMemorySessionBytesStoreSupplier(name, retentionPeriodMs);
    }

    /**
     * Creates a {@link StoreBuilder} that can be used to build a {@link KeyValueStore}.
     * <p>
     * The provided supplier should <strong>not</strong> be a supplier for
     * {@link TimestampedKeyValueStore TimestampedKeyValueStores}.
     *
     * @param supplier      a {@link KeyValueBytesStoreSupplier} (cannot be {@code null})
     * @param keySerde      the key serde to use
     * @param valueSerde    the value serde to use; if the serialized bytes is {@code null} for put operations,
     *                      it is treated as delete
     * @param <K>           key type
     * @param <V>           value type
     * @return an instance of a {@link StoreBuilder} that can build a {@link KeyValueStore}
     */
    public static <K, V> StoreBuilder<IKeyValueStore<K, V>> keyValueStoreBuilder(KeyValueBytesStoreSupplier supplier,
                                                                                ISerde<K> keySerde,
                                                                                ISerde<V> valueSerde)
{
        Objects.requireNonNull(supplier, "supplier cannot be null");
        return new KeyValueStoreBuilder<>(supplier, keySerde, valueSerde, ITime.SYSTEM);
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
     * @param valueSerde    the value serde to use; if the serialized bytes is {@code null} for put operations,
     *                      it is treated as delete
     * @param <K>           key type
     * @param <V>           value type
     * @return an instance of a {@link StoreBuilder} that can build a {@link KeyValueStore}
     */
    public static <K, V> StoreBuilder<TimestampedKeyValueStore<K, V>> timestampedKeyValueStoreBuilder(KeyValueBytesStoreSupplier supplier,
                                                                                                      ISerde<K> keySerde,
                                                                                                      ISerde<V> valueSerde)
{
        Objects.requireNonNull(supplier, "supplier cannot be null");
        return new TimestampedKeyValueStoreBuilder<>(supplier, keySerde, valueSerde, ITime.SYSTEM);
    }

    /**
     * Creates a {@link StoreBuilder} that can be used to build a {@link WindowStore}.
     * <p>
     * The provided supplier should <strong>not</strong> be a supplier for
     * {@link TimestampedWindowStore TimestampedWindowStores}.
     *
     * @param supplier      a {@link WindowBytesStoreSupplier} (cannot be {@code null})
     * @param keySerde      the key serde to use
     * @param valueSerde    the value serde to use; if the serialized bytes is {@code null} for put operations,
     *                      it is treated as delete
     * @param <K>           key type
     * @param <V>           value type
     * @return an instance of {@link StoreBuilder} than can build a {@link WindowStore}
     */
    public static <K, V> StoreBuilder<WindowStore<K, V>> windowStoreBuilder(WindowBytesStoreSupplier supplier,
                                                                            ISerde<K> keySerde,
                                                                            ISerde<V> valueSerde)
{
        Objects.requireNonNull(supplier, "supplier cannot be null");
        return new WindowStoreBuilder<>(supplier, keySerde, valueSerde, ITime.SYSTEM);
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
     * @param valueSerde    the value serde to use; if the serialized bytes is {@code null} for put operations,
     *                      it is treated as delete
     * @param <K>           key type
     * @param <V>           value type
     * @return an instance of {@link StoreBuilder} that can build a {@link TimestampedWindowStore}
     */
    public static <K, V> StoreBuilder<TimestampedWindowStore<K, V>> timestampedWindowStoreBuilder(WindowBytesStoreSupplier supplier,
                                                                                                  ISerde<K> keySerde,
                                                                                                  ISerde<V> valueSerde)
{
        Objects.requireNonNull(supplier, "supplier cannot be null");
        return new TimestampedWindowStoreBuilder<>(supplier, keySerde, valueSerde, ITime.SYSTEM);
    }

    /**
     * Creates a {@link StoreBuilder} that can be used to build a {@link SessionStore}.
     *
     * @param supplier      a {@link SessionBytesStoreSupplier} (cannot be {@code null})
     * @param keySerde      the key serde to use
     * @param valueSerde    the value serde to use; if the serialized bytes is {@code null} for put operations,
     *                      it is treated as delete
     * @param <K>           key type
     * @param <V>           value type
     * @return an instance of {@link StoreBuilder} than can build a {@link SessionStore}
     */
    public static <K, V> StoreBuilder<SessionStore<K, V>> sessionStoreBuilder(SessionBytesStoreSupplier supplier,
                                                                              ISerde<K> keySerde,
                                                                              ISerde<V> valueSerde)
{
        Objects.requireNonNull(supplier, "supplier cannot be null");
        return new SessionStoreBuilder<>(supplier, keySerde, valueSerde, ITime.SYSTEM);
    }
}