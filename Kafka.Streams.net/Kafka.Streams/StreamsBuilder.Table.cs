using System;
using System.Runtime.CompilerServices;
using Kafka.Common;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams
{
    namespace Kafka.Streams
    {
        /// <summary>
        /// Provides the high-level Kafka Streams DSL to specify a Kafka Streams topology.
        /// </summary>
        public partial class StreamsBuilder
        {
            /**
             * Create a {@link KTable} for the specified topic.
             * The {@code "auto.offset.reset"} strategy, {@link ITimestampExtractor}, key and value deserializers
             * are defined by the options in {@link Consumed} are used.
             * Input {@link KeyValuePair records} with {@code null} key will be dropped.
             * <p>
             * Note that the specified input topic must be partitioned by key.
             * If this is not the case the returned {@link KTable} will be corrupted.
             * <p>
             * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} using the given
             * {@code Materialized} instance.
             * An internal changelog topic is created by default. Because the source topic can
             * be used for recovery, you can avoid creating the changelog topic by setting
             * the {@code "topology.optimization"} to {@code "All"} in the {@link StreamsConfig}.
             * <p>
             * You should only specify serdes in the {@link Consumed} instance as these will also be used to overwrite the
             * serdes in {@link Materialized}, i.e.,
             * <pre> {@code
             * streamBuilder.table(topic, Consumed.With(Serde.String(), Serde.String()), Materialized.<String, String, KeyValueStore<Bytes, byte[]>as(storeName))
             * }
             * </pre>
             * To query the local {@link KeyValueStore} it must be obtained via
             * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
             * <pre>{@code
             * KafkaStreams streams = ...
             * IReadOnlyKeyValueStore<String, long> localStore = streams.Store(queryableStoreName, QueryableStoreTypes.<String, long>KeyValueStore);
             * String key = "some-key";
             * long valueForKey = localStore.Get(key); // key must be local (application state is shared over All running Kafka Streams instances)
             * }</pre>
             * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
             * query the value of the key on a parallel running instance of your Kafka Streams application.
             *
             * @param topic              the topic Name; cannot be {@code null}
             * @param consumed           the instance of {@link Consumed} used to define optional parameters; cannot be {@code null}
             * @param materialized       the instance of {@link Materialized} used to materialize a state store; cannot be {@code null}
             * @return a {@link KTable} for the specified topic
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public IKTable<K, V> Table<K, V>(
                string topic,
                Consumed<K, V> consumed,
                Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
            {
                if (string.IsNullOrEmpty(topic))
                {
                    throw new ArgumentException("message", nameof(topic));
                }

                if (consumed is null)
                {
                    throw new ArgumentNullException(nameof(consumed));
                }

                if (materialized is null)
                {
                    throw new ArgumentNullException(nameof(materialized));
                }

                var consumedInternal = new ConsumedInternal<K, V>(consumed);
                materialized
                    .WithKeySerde(consumedInternal.KeySerde)
                    .WithValueSerde(consumedInternal.ValueSerde);

                var materializedInternal =
                     new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(materialized, this.Context.InternalStreamsBuilder, topic + "-");

                return this.Context.InternalStreamsBuilder.Table(topic, consumedInternal, materializedInternal);
            }

            /**
             * Create a {@link KTable} for the specified topic.
             * The default {@code "auto.offset.reset"} strategy and default key and value deserializers as specified in the
             * {@link StreamsConfig config} are used.
             * Input {@link KeyValuePair records} with {@code null} key will be dropped.
             * <p>
             * Note that the specified input topics must be partitioned by key.
             * If this is not the case the returned {@link KTable} will be corrupted.
             * <p>
             * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} with an internal
             * store Name. Note that store Name may not be queriable through Interactive Queries.
             * An internal changelog topic is created by default. Because the source topic can
             * be used for recovery, you can avoid creating the changelog topic by setting
             * the {@code "topology.optimization"} to {@code "All"} in the {@link StreamsConfig}.
             *
             * @param topic the topic Name; cannot be {@code null}
             * @return a {@link KTable} for the specified topic
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public IKTable<K, V> Table<K, V>(string topic)
            {
                return this.Table(topic, new ConsumedInternal<K, V>());
            }

            /**
             * Create a {@link KTable} for the specified topic.
             * The {@code "auto.offset.reset"} strategy, {@link ITimestampExtractor}, key and value deserializers
             * are defined by the options in {@link Consumed} are used.
             * Input {@link KeyValuePair records} with {@code null} key will be dropped.
             * <p>
             * Note that the specified input topics must be partitioned by key.
             * If this is not the case the returned {@link KTable} will be corrupted.
             * <p>
             * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} with an internal
             * store Name. Note that store Name may not be queriable through Interactive Queries.
             * An internal changelog topic is created by default. Because the source topic can
             * be used for recovery, you can avoid creating the changelog topic by setting
             * the {@code "topology.optimization"} to {@code "All"} in the {@link StreamsConfig}.
             *
             * @param topic     the topic Name; cannot be {@code null}
             * @param consumed  the instance of {@link Consumed} used to define optional parameters; cannot be {@code null}
             * @return a {@link KTable} for the specified topic
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public IKTable<K, V> Table<K, V>(
                string topic,
                Consumed<K, V> consumed)
            {
                if (string.IsNullOrEmpty(topic))
                {
                    throw new ArgumentException("message", nameof(topic));
                }

                if (consumed is null)
                {
                    throw new ArgumentNullException(nameof(consumed));
                }

                var consumedInternal = new ConsumedInternal<K, V>(consumed);

                var materializedInternal =
                     new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(
                             Materialized.With<K, V, IKeyValueStore<Bytes, byte[]>>(
                                 consumedInternal.KeySerde,
                                 consumedInternal.ValueSerde),
                             this.Context.InternalStreamsBuilder, topic + "-");

                return this.Context.InternalStreamsBuilder.Table(
                    topic,
                    consumedInternal,
                    materializedInternal);
            }

            /**
             * Create a {@link KTable} for the specified topic.
             * The default {@code "auto.offset.reset"} strategy as specified in the {@link StreamsConfig config} are used.
             * Key and value deserializers as defined by the options in {@link Materialized} are used.
             * Input {@link KeyValuePair records} with {@code null} key will be dropped.
             * <p>
             * Note that the specified input topics must be partitioned by key.
             * If this is not the case the returned {@link KTable} will be corrupted.
             * <p>
             * The resulting {@link KTable} will be materialized in a local {@link KeyValueStore} using the {@link Materialized} instance.
             * An internal changelog topic is created by default. Because the source topic can
             * be used for recovery, you can avoid creating the changelog topic by setting
             * the {@code "topology.optimization"} to {@code "All"} in the {@link StreamsConfig}.
             *
             * @param topic         the topic Name; cannot be {@code null}
             * @param materialized  the instance of {@link Materialized} used to materialize a state store; cannot be {@code null}
             * @return a {@link KTable} for the specified topic
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public IKTable<K, V> Table<K, V>(
                string topic,
                Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
            {
                if (string.IsNullOrEmpty(topic))
                {
                    throw new ArgumentException("message", nameof(topic));
                }

                if (materialized is null)
                {
                    throw new ArgumentNullException(nameof(materialized));
                }

                var materializedInternal = new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(
                    materialized,
                    this.Context.InternalStreamsBuilder,
                    topic + "-");

                var consumedInternal = new ConsumedInternal<K, V>(
                    Consumed.With<K, V>(
                        materializedInternal.KeySerde,
                        materializedInternal.ValueSerde));

                return this.Context.InternalStreamsBuilder.Table(
                    topic,
                    consumedInternal,
                    materializedInternal);
            }

            /**
             * Create a {@link GlobalKTable} for the specified topic.
             * Input {@link KeyValuePair records} with {@code null} key will be dropped.
             * <p>
             * The resulting {@link GlobalKTable} will be materialized in a local {@link KeyValueStore} with an internal
             * store Name. Note that store Name may not be queriable through Interactive Queries.
             * No internal changelog topic is created since the original input topic can be used for recovery (cf.
             * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
             * <p>
             * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
             * regardless of the specified value in {@link StreamsConfig} or {@link Consumed}.
             *
             * @param topic the topic Name; cannot be {@code null}
             * @param consumed  the instance of {@link Consumed} used to define optional parameters
             * @return a {@link GlobalKTable} for the specified topic
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public IGlobalKTable<K, V> GlobalTable<K, V>(
                string topic,
                Consumed<K, V> consumed)
            {
                if (string.IsNullOrEmpty(topic))
                {
                    throw new ArgumentException("message", nameof(topic));
                }

                if (consumed is null)
                {
                    throw new ArgumentNullException(nameof(consumed));
                }

                ConsumedInternal<K, V> consumedInternal = new ConsumedInternal<K, V>(consumed);

                var materializedInternal =
                     new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(
                         Materialized.With<K, V, IKeyValueStore<Bytes, byte[]>>(consumedInternal.KeySerde, consumedInternal.ValueSerde),
                         this.Context.InternalStreamsBuilder, topic + "-");

                return this.Context.InternalStreamsBuilder.GlobalTable(
                    topic,
                    consumedInternal,
                    materializedInternal);
            }

            /**
             * Create a {@link GlobalKTable} for the specified topic.
             * The default key and value deserializers as specified in the {@link StreamsConfig config} are used.
             * Input {@link KeyValuePair records} with {@code null} key will be dropped.
             * <p>
             * The resulting {@link GlobalKTable} will be materialized in a local {@link KeyValueStore} with an internal
             * store Name. Note that store Name may not be queriable through Interactive Queries.
             * No internal changelog topic is created since the original input topic can be used for recovery (cf.
             * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
             * <p>
             * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
             * regardless of the specified value in {@link StreamsConfig}.
             *
             * @param topic the topic Name; cannot be {@code null}
             * @return a {@link GlobalKTable} for the specified topic
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public IGlobalKTable<K, V> GlobalTable<K, V>(string topic)
            {
                return this.GlobalTable(
                    topic,
                    Consumed.With<K, V>(null, null));
            }

            /**
             * Create a {@link GlobalKTable} for the specified topic.
             *
             * Input {@link KeyValuePair} pairs with {@code null} key will be dropped.
             * <p>
             * The resulting {@link GlobalKTable} will be materialized in a local {@link KeyValueStore} configured with
             * the provided instance of {@link Materialized}.
             * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
             * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
             * <p>
             * You should only specify serdes in the {@link Consumed} instance as these will also be used to overwrite the
             * serdes in {@link Materialized}, i.e.,
             * <pre> {@code
             * streamBuilder.globalTable(topic, Consumed.With(Serde.String(), Serde.String()), Materialized.<String, String, KeyValueStore<Bytes, byte[]>as(storeName))
             * }
             * </pre>
             * To query the local {@link KeyValueStore} it must be obtained via
             * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
             * <pre>{@code
             * KafkaStreams streams = ...
             * IReadOnlyKeyValueStore<String, long> localStore = streams.Store(queryableStoreName, QueryableStoreTypes.<String, long>KeyValueStore);
             * String key = "some-key";
             * long valueForKey = localStore.Get(key);
             * }</pre>
             * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
             * regardless of the specified value in {@link StreamsConfig} or {@link Consumed}.
             *
             * @param topic         the topic Name; cannot be {@code null}
             * @param consumed      the instance of {@link Consumed} used to define optional parameters; can't be {@code null}
             * @param materialized   the instance of {@link Materialized} used to materialize a state store; cannot be {@code null}
             * @return a {@link GlobalKTable} for the specified topic
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public IGlobalKTable<K, V> GlobalTable<K, V>(
                string topic,
                Consumed<K, V> consumed,
                Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
            {
                if (string.IsNullOrEmpty(topic))
                {
                    throw new ArgumentException("message", nameof(topic));
                }

                if (consumed is null)
                {
                    throw new ArgumentNullException(nameof(consumed));
                }

                if (materialized is null)
                {
                    throw new ArgumentNullException(nameof(materialized));
                }

                ConsumedInternal<K, V> consumedInternal = new ConsumedInternal<K, V>(consumed);
                // always use the serdes from consumed
                materialized
                    .WithKeySerde(consumedInternal.KeySerde)
                    .WithValueSerde(consumedInternal.ValueSerde);

                var materializedInternal =
                     new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(
                         materialized,
                         this.Context.InternalStreamsBuilder,
                         topic + "-");

                return this.Context.InternalStreamsBuilder.GlobalTable(
                    topic,
                    consumedInternal,
                    materializedInternal);
            }

            /**
             * Create a {@link GlobalKTable} for the specified topic.
             *
             * Input {@link KeyValuePair} pairs with {@code null} key will be dropped.
             * <p>
             * The resulting {@link GlobalKTable} will be materialized in a local {@link KeyValueStore} configured with
             * the provided instance of {@link Materialized}.
             * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
             * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
             * <p>
             * To query the local {@link KeyValueStore} it must be obtained via
             * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
             * <pre>{@code
             * KafkaStreams streams = ...
             * IReadOnlyKeyValueStore<String, long> localStore = streams.Store(queryableStoreName, QueryableStoreTypes.<String, long>KeyValueStore);
             * String key = "some-key";
             * long valueForKey = localStore.Get(key);
             * }</pre>
             * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
             * regardless of the specified value in {@link StreamsConfig}.
             *
             * @param topic         the topic Name; cannot be {@code null}
             * @param materialized   the instance of {@link Materialized} used to materialize a state store; cannot be {@code null}
             * @return a {@link GlobalKTable} for the specified topic
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public IGlobalKTable<K, V> GlobalTable<K, V>(
                string topic,
                Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
            {
                if (string.IsNullOrWhiteSpace(topic))
                {
                    throw new ArgumentException("message", nameof(topic));
                }

                if (materialized is null)
                {
                    throw new ArgumentNullException(nameof(materialized));
                }

                var materializedInternal =
                     new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(
                         materialized,
                         this.Context.InternalStreamsBuilder,
                         topic + "-");

                return this.Context.InternalStreamsBuilder.GlobalTable(
                    topic,
                    new ConsumedInternal<K, V>(
                        Consumed.With(
                            materializedInternal.KeySerde,
                            materializedInternal.ValueSerde)),
                        materializedInternal);
            }
        }
    }
}
