using Kafka.Common;
using Kafka.Streams.Clients;
using Kafka.Streams.Clients.Consumers;
using Kafka.Streams.Configs;
using Kafka.Streams.Factories;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.Tasks;
using Kafka.Streams.Threads;
using Kafka.Streams.Threads.GlobalStream;
using Kafka.Streams.Threads.Stream;
using Kafka.Streams.Threads.KafkaStreams;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Confluent.Kafka;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Clients.Producers;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.Interfaces;

namespace Kafka.Streams
{
    namespace Kafka.Streams
    {
        /// <summary>
        /// Provides the high-level Kafka Streams DSL to specify a Kafka Streams topology.
        /// </summary>
        public class StreamsBuilder
        {
            public IConfiguration Configuration { get; }
            public IServiceCollection ServiceCollection { get; }
            public ServiceProvider Services { get; }

            /** The actual topology that is constructed by this StreamsBuilder. */
            public Topology Topology { get; }

            public InternalTopologyBuilder InternalTopologyBuilder
                => this.Topology?.internalTopologyBuilder ?? throw new InvalidOperationException($"{nameof(InternalTopologyBuilder)} accessed without initializing {nameof(StreamsBuilder)}");

            public InternalStreamsBuilder InternalStreamsBuilder { get; }

            public StreamsBuilder(
                IConfiguration? configuration = null,
                IServiceCollection? serviceCollection = null)
            {
                this.Configuration = configuration ?? new ConfigurationBuilder().Build();
                this.ServiceCollection = serviceCollection ?? new ServiceCollection();
                this.ServiceCollection = this.BuildDependencyTree(this.Configuration, this.ServiceCollection);

                // Add an empty configuration if none provided.
                var config = new StreamsConfig
                {
                    ApplicationId = "ApplicationId_NotSet",
                    BootstrapServers = "localhost:9092",
                };

                this.ServiceCollection.TryAddSingleton<StreamsConfig>(config);

                this.Services = this.ServiceCollection.BuildServiceProvider();

                this.Topology = this.Services.GetRequiredService<Topology>();
                this.InternalStreamsBuilder = this.Services.GetRequiredService<InternalStreamsBuilder>();
            }

            public StreamsBuilder(IServiceCollection serviceCollection)
                : this(new ConfigurationBuilder().Build(), serviceCollection)
            { }

            public StreamsBuilder(IConfiguration configuration)
                : this(configuration, new ServiceCollection())
            { }

            public StreamsBuilder()
                : this(new ConfigurationBuilder().Build(), new ServiceCollection())
            { }

            public virtual IKafkaStreamsThread BuildKafkaStreams()
            {
                var kafkaStreamsThread = this.Services.GetRequiredService<IKafkaStreamsThread>();
                kafkaStreamsThread.SetStateListener(kafkaStreamsThread.GetStateListener());

                return kafkaStreamsThread;
            }

            public virtual IServiceCollection BuildDependencyTree(IConfiguration configuration, IServiceCollection serviceCollection)
            {
                serviceCollection.AddLogging();

                serviceCollection.TryAddSingleton(configuration);
                serviceCollection.TryAddSingleton(serviceCollection);
                serviceCollection.TryAddSingleton<IClock, SystemClock>();

                this.AddNodes(serviceCollection);
                this.AddThreads(serviceCollection);
                this.AddClients(serviceCollection);
                this.AddFactories(serviceCollection);

                serviceCollection.TryAddSingleton<StateDirectory>();

                serviceCollection.TryAddSingleton<IStateRestoreListener, DelegatingStateRestoreListener>();
                serviceCollection.TryAddSingleton<IChangelogReader, StoreChangelogReader>();
                serviceCollection.TryAddSingleton<IStateListener, StreamStateListener>();
                serviceCollection.TryAddSingleton<StreamsMetadataState>();
                serviceCollection.TryAddSingleton<InternalTopologyBuilder>();
                serviceCollection.TryAddSingleton<InternalStreamsBuilder>();

                // TaskManager stuff
                serviceCollection.TryAddTransient<ThreadCache>();
                serviceCollection.TryAddSingleton<ITaskManager, TaskManager>();
                serviceCollection.TryAddSingleton<AbstractTaskCreator<StandbyTask>, StandbyTaskCreator>();
                serviceCollection.TryAddSingleton<AbstractTaskCreator<StreamTask>, TaskCreator>();
                serviceCollection.TryAddSingleton<AssignedStreamsTasks>();
                serviceCollection.TryAddSingleton<AssignedStandbyTasks>();

                serviceCollection.TryAddSingleton<Topology>();

                return serviceCollection;
            }

            protected virtual IServiceCollection AddFactories(IServiceCollection serviceCollection)
            {
                serviceCollection.TryAddSingleton(typeof(SourceNodeFactory<,>));
                serviceCollection.TryAddSingleton<DeserializerFactory>();
                serviceCollection.TryAddSingleton<SerializerFactory>();
                serviceCollection.TryAddTransient(typeof(SerdeFactory<>));

                serviceCollection.TryAddTransient<ISerializer<string>>(_ => Serializers.Utf8);
                serviceCollection.TryAddTransient<IDeserializer<string>>(_ => Deserializers.Utf8);

                serviceCollection.TryAddTransient<ISerializer<byte[]>>(_ => Serializers.ByteArray);
                serviceCollection.TryAddTransient<IDeserializer<byte[]>>(_ => Deserializers.ByteArray);

                serviceCollection.TryAddTransient(typeof(ISerde<>), typeof(Serde<>));
                serviceCollection.TryAddTransient<ISerde<byte[]>>(sp => Serdes.ByteArray());
                serviceCollection.TryAddTransient<Serde<byte[]>>(sp => Serdes.ByteArray() as Serde<byte[]>);
                serviceCollection.TryAddTransient<ISerializer<byte[]>>(sp => Serdes.ByteArray().Serializer);

                return serviceCollection;
            }

            protected virtual IServiceCollection AddNodes(IServiceCollection serviceCollection)
            {
                //serviceCollection.TryAddScoped(typeof(IDeserializer<>),
                //    sp =>
                //    {
                //        return null;
                //    });

                //serviceCollection.TryAddScoped(typeof(ISerializer<>),typeof(Serializer<T>))
                //    sp =>
                //    {
                //        return null;
                //    });

                return serviceCollection;
            }

            protected virtual IServiceCollection AddClients(IServiceCollection serviceCollection)
            {
                serviceCollection.TryAddSingleton<IKafkaClientSupplier, DefaultKafkaClientSupplier>();

                // Consumers
                serviceCollection.TryAddSingleton<GlobalConsumer>();
                serviceCollection.TryAddSingleton<StateConsumer>();
                serviceCollection.TryAddSingleton<RestoreConsumer>();

                serviceCollection.TryAddSingleton<RestoreConsumerConfig>();

                // Producers
                serviceCollection.TryAddSingleton<BaseProducer<byte[], byte[]>>();

                // Special clients, e.g., AdminClient
                serviceCollection.TryAddSingleton<IAdminClient>(sp =>
                {
                    var clientSupplier = sp.GetRequiredService<IKafkaClientSupplier>();
                    var config = sp.GetRequiredService<StreamsConfig>();

                    return clientSupplier.GetAdminClient(config);
                });

                return serviceCollection;
            }

            protected virtual IServiceCollection AddThreads(IServiceCollection serviceCollection)
            {
                serviceCollection.TryAddSingleton<IGlobalStreamThread, GlobalStreamThread>();
                serviceCollection.TryAddSingleton<IStateMachine<GlobalStreamThreadStates>, GlobalStreamThreadState>();

                serviceCollection.TryAddTransient<IStreamThread, StreamThread>();
                serviceCollection.TryAddTransient<IStateMachine<StreamThreadStates>, StreamThreadState>();

                serviceCollection.TryAddSingleton<IKafkaStreamsThread, KafkaStreamsThread>();
                serviceCollection.TryAddSingleton<IStateMachine<KafkaStreamsThreadStates>, KafkaStreamsThreadState>();

                return serviceCollection;
            }

            // Create a {@link KStream} from the specified topic.
            // The default {@code "auto.offset.reset"} strategy, default {@link ITimestampExtractor}, and default key and value
            // deserializers as specified in the {@link StreamsConfig config} are used.
            // <p>
            // If multiple topics are specified there is no ordering guarantee for records from different topics.
            // <p>
            // Note that the specified input topic must be partitioned by key.
            // If this is not the case it is the user's responsibility to repartition the data before any key based operation
            // (like aggregation or join) is applied to the returned {@link KStream}.
            // 
            // @param topic the topic name; cannot be {@code null}
            // @return a {@link KStream} for the specified topic
            [MethodImpl(MethodImplOptions.Synchronized)]
            public IKStream<K, V> Stream<K, V>(string topic)
            {
                return Stream<K, V>(new List<string>() { topic }.AsReadOnly());
            }

            /**
             * Create a {@link KStream} from the specified topic.
             * The {@code "auto.offset.reset"} strategy, {@link ITimestampExtractor}, key and value deserializers
             * are defined by the options in {@link Consumed} are used.
             * <p>
             * Note that the specified input topic must be partitioned by key.
             * If this is not the case it is the user's responsibility to repartition the data before any key based operation
             * (like aggregation or join) is applied to the returned {@link KStream}.
             *
             * @param topic the topic names; cannot be {@code null}
             * @param consumed      the instance of {@link Consumed} used to define optional parameters
             * @return a {@link KStream} for the specified topic
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public IKStream<K, V> Stream<K, V>(
                string topic,
                Consumed<K, V> consumed)
            {
                return this.Stream(new[] { topic }, consumed);
            }

            /**
             * Create a {@link KStream} from the specified topics.
             * The default {@code "auto.offset.reset"} strategy, default {@link ITimestampExtractor}, and default key and value
             * deserializers as specified in the {@link StreamsConfig config} are used.
             * <p>
             * If multiple topics are specified there is no ordering guarantee for records from different topics.
             * <p>
             * Note that the specified input topics must be partitioned by key.
             * If this is not the case it is the user's responsibility to repartition the data before any key based operation
             * (like aggregation or join) is applied to the returned {@link KStream}.
             *
             * @param topics the topic names; must contain at least one topic name
             * @return a {@link KStream} for the specified topics
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public IKStream<K, V> Stream<K, V>(IReadOnlyList<string> topics)
            {
                return Stream(
                    topics,
                    Consumed.With<K, V>(null, null, null, null));
            }

            /**
             * Create a {@link KStream} from the specified topics.
             * The {@code "auto.offset.reset"} strategy, {@link ITimestampExtractor}, key and value deserializers
             * are defined by the options in {@link Consumed} are used.
             * <p>
             * If multiple topics are specified there is no ordering guarantee for records from different topics.
             * <p>
             * Note that the specified input topics must be partitioned by key.
             * If this is not the case it is the user's responsibility to repartition the data before any key based operation
             * (like aggregation or join) is applied to the returned {@link KStream}.
             *
             * @param topics the topic names; must contain at least one topic name
             * @param consumed      the instance of {@link Consumed} used to define optional parameters
             * @return a {@link KStream} for the specified topics
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public IKStream<K, V> Stream<K, V>(
                IEnumerable<string> topics,
                Consumed<K, V> consumed)
            {
                topics = topics ?? throw new ArgumentNullException(nameof(topics));
                consumed = consumed ?? throw new ArgumentNullException(nameof(consumed));

                return this.InternalStreamsBuilder.Stream(topics, new ConsumedInternal<K, V>(consumed));
            }

            /**
             * Create a {@link KStream} from the specified topic pattern.
             * The default {@code "auto.offset.reset"} strategy, default {@link ITimestampExtractor}, and default key and value
             * deserializers as specified in the {@link StreamsConfig config} are used.
             * <p>
             * If multiple topics are matched by the specified pattern, the created {@link KStream} will read data from all of
             * them and there is no ordering guarantee between records from different topics.
             * <p>
             * Note that the specified input topics must be partitioned by key.
             * If this is not the case it is the user's responsibility to repartition the data before any key based operation
             * (like aggregation or join) is applied to the returned {@link KStream}.
             *
             * @param topicPattern the pattern to match for topic names
             * @return a {@link KStream} for topics matching the regex pattern.
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public IKStream<K, V> Stream<K, V>(Regex topicPattern)
            {
                return Stream(topicPattern, Consumed.With<K, V>(null, null));
            }

            /**
             * Create a {@link KStream} from the specified topic pattern.
             * The {@code "auto.offset.reset"} strategy, {@link ITimestampExtractor}, key and value deserializers
             * are defined by the options in {@link Consumed} are used.
             * <p>
             * If multiple topics are matched by the specified pattern, the created {@link KStream} will read data from all of
             * them and there is no ordering guarantee between records from different topics.
             * <p>
             * Note that the specified input topics must be partitioned by key.
             * If this is not the case it is the user's responsibility to repartition the data before any key based operation
             * (like aggregation or join) is applied to the returned {@link KStream}.
             *
             * @param topicPattern  the pattern to match for topic names
             * @param consumed      the instance of {@link Consumed} used to define optional parameters
             * @return a {@link KStream} for topics matching the regex pattern.
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public IKStream<K, V> Stream<K, V>(
                Regex topicPattern,
                Consumed<K, V> consumed)
            {
                topicPattern = topicPattern ?? throw new ArgumentNullException(nameof(topicPattern));
                consumed = consumed ?? throw new ArgumentNullException(nameof(consumed));

                return this.InternalStreamsBuilder.Stream(topicPattern, new ConsumedInternal<K, V>(consumed));
            }

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
             * the {@code "topology.optimization"} to {@code "all"} in the {@link StreamsConfig}.
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
             * IReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>KeyValueStore());
             * String key = "some-key";
             * Long valueForKey = localStore.Get(key); // key must be local (application state is shared over all running Kafka Streams instances)
             * }</pre>
             * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
             * query the value of the key on a parallel running instance of your Kafka Streams application.
             *
             * @param topic              the topic name; cannot be {@code null}
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
                    .WithKeySerde(consumedInternal.keySerde)
                    .WithValueSerde(consumedInternal.valueSerde);

                var materializedInternal =
                     new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(materialized, this.InternalStreamsBuilder, topic + "-");

                return this.InternalStreamsBuilder.Table(topic, consumedInternal, materializedInternal);
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
             * store name. Note that store name may not be queriable through Interactive Queries.
             * An internal changelog topic is created by default. Because the source topic can
             * be used for recovery, you can avoid creating the changelog topic by setting
             * the {@code "topology.optimization"} to {@code "all"} in the {@link StreamsConfig}.
             *
             * @param topic the topic name; cannot be {@code null}
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
             * store name. Note that store name may not be queriable through Interactive Queries.
             * An internal changelog topic is created by default. Because the source topic can
             * be used for recovery, you can avoid creating the changelog topic by setting
             * the {@code "topology.optimization"} to {@code "all"} in the {@link StreamsConfig}.
             *
             * @param topic     the topic name; cannot be {@code null}
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
                             Materialized.With<K, V, IKeyValueStore<Bytes, byte[]>>(consumedInternal.keySerde, consumedInternal.valueSerde),
                             this.InternalStreamsBuilder, topic + "-");

                return this.InternalStreamsBuilder.Table(topic, consumedInternal, materializedInternal);
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
             * the {@code "topology.optimization"} to {@code "all"} in the {@link StreamsConfig}.
             *
             * @param topic         the topic name; cannot be {@code null}
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

                var materializedInternal =
                     new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(materialized, this.InternalStreamsBuilder, topic + "-");

                var consumedInternal =
                        new ConsumedInternal<K, V>(Consumed.With<K, V>(materializedInternal.KeySerde, materializedInternal.ValueSerde));

                return this.InternalStreamsBuilder.Table(topic, consumedInternal, materializedInternal);
            }

            /**
             * Create a {@link GlobalKTable} for the specified topic.
             * Input {@link KeyValuePair records} with {@code null} key will be dropped.
             * <p>
             * The resulting {@link GlobalKTable} will be materialized in a local {@link KeyValueStore} with an internal
             * store name. Note that store name may not be queriable through Interactive Queries.
             * No internal changelog topic is created since the original input topic can be used for recovery (cf.
             * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
             * <p>
             * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
             * regardless of the specified value in {@link StreamsConfig} or {@link Consumed}.
             *
             * @param topic the topic name; cannot be {@code null}
             * @param consumed  the instance of {@link Consumed} used to define optional parameters
             * @return a {@link GlobalKTable} for the specified topic
             */
            //[MethodImpl(MethodImplOptions.Synchronized)]
            //public IGlobalKTable<K, V> globalTable<K, V>(
            //    string topic,
            //    Consumed<K, V> consumed)
            //{
            //    Objects.requireNonNull(topic, "topic can't be null");
            //    Objects.requireNonNull(consumed, "consumed can't be null");
            //    ConsumedInternal<K, V> consumedInternal = new ConsumedInternal<K, V>(consumed);

            //    var materializedInternal =
            //         new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(
            //             Materialized<K, V, IKeyValueStore<Bytes, byte[]>>.with(consumedInternal.keySerde, consumedInternal.valueSerde),
            //             internalStreamsBuilder, topic + "-");

            //    return internalStreamsBuilder.globalTable(topic, consumedInternal, materializedInternal);
            //}

            /**
             * Create a {@link GlobalKTable} for the specified topic.
             * The default key and value deserializers as specified in the {@link StreamsConfig config} are used.
             * Input {@link KeyValuePair records} with {@code null} key will be dropped.
             * <p>
             * The resulting {@link GlobalKTable} will be materialized in a local {@link KeyValueStore} with an internal
             * store name. Note that store name may not be queriable through Interactive Queries.
             * No internal changelog topic is created since the original input topic can be used for recovery (cf.
             * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
             * <p>
             * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
             * regardless of the specified value in {@link StreamsConfig}.
             *
             * @param topic the topic name; cannot be {@code null}
             * @return a {@link GlobalKTable} for the specified topic
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public IGlobalKTable<K, V> GlobalTable<K, V>(string topic)
            {
                return null; // GlobalTable(topic, Consumed.With<K, V>(null, null));
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
             * IReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>KeyValueStore());
             * String key = "some-key";
             * Long valueForKey = localStore.Get(key);
             * }</pre>
             * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
             * regardless of the specified value in {@link StreamsConfig} or {@link Consumed}.
             *
             * @param topic         the topic name; cannot be {@code null}
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
                    .WithKeySerde(consumedInternal.keySerde)
                    .WithValueSerde(consumedInternal.valueSerde);

                MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal =
                     new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(materialized, this.InternalStreamsBuilder, topic + "-");

                return this.InternalStreamsBuilder.GlobalTable(topic, consumedInternal, materializedInternal);
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
             * IReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>KeyValueStore());
             * String key = "some-key";
             * Long valueForKey = localStore.Get(key);
             * }</pre>
             * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
             * regardless of the specified value in {@link StreamsConfig}.
             *
             * @param topic         the topic name; cannot be {@code null}
             * @param materialized   the instance of {@link Materialized} used to materialize a state store; cannot be {@code null}
             * @return a {@link GlobalKTable} for the specified topic
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public IGlobalKTable<K, V> GlobalTable<K, V>(
                string topic,
                Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
            {
                Objects.requireNonNull(topic, "topic can't be null");
                Objects.requireNonNull(materialized, "materialized can't be null");
                MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal =
                     new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(
                         materialized, this.InternalStreamsBuilder, topic + "-");

                return this.InternalStreamsBuilder.GlobalTable(
                    topic,
                    new ConsumedInternal<K, V>(
                        Consumed.With<K, V>(materializedInternal.KeySerde, materializedInternal.ValueSerde)),
                        materializedInternal);
            }

            /**
             * Adds a state store to the underlying {@link Topology}.
             * <p>
             * It is required to connect state stores to {@link IProcessor Processors}, {@link Transformer Transformers},
             * or {@link ValueTransformer ValueTransformers} before they can be used.
             *
             * @param builder the builder used to obtain this state store {@link StateStore} instance
             * @return itself
             * @throws TopologyException if state store supplier is already added
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public StreamsBuilder AddStateStore<K, V, T>(IStoreBuilder<T> builder)
                where T : IStateStore
            {
                if (builder is null)
                {
                    throw new ArgumentNullException(nameof(builder));
                }

                this.InternalStreamsBuilder.AddStateStore<K, V, T>(builder);

                return this;
            }

            /**
             * @deprecated use {@link #addGlobalStore(StoreBuilder, String, Consumed, IProcessorSupplier)} instead
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public StreamsBuilder AddGlobalStore<K, V, T>(
                IStoreBuilder<T> storeBuilder,
                string topic,
                string sourceName,
                Consumed<K, V> consumed,
                string processorName,
                IProcessorSupplier<K, V> stateUpdateSupplier)
                where T : IStateStore
            {
                if (storeBuilder is null)
                {
                    throw new ArgumentNullException(nameof(storeBuilder));
                }

                if (string.IsNullOrEmpty(topic))
                {
                    throw new ArgumentException("message", nameof(topic));
                }

                if (string.IsNullOrEmpty(sourceName))
                {
                    throw new ArgumentException("message", nameof(sourceName));
                }

                if (consumed is null)
                {
                    throw new ArgumentNullException(nameof(consumed));
                }

                if (string.IsNullOrEmpty(processorName))
                {
                    throw new ArgumentException("message", nameof(processorName));
                }

                if (stateUpdateSupplier is null)
                {
                    throw new ArgumentNullException(nameof(stateUpdateSupplier));
                }

                this.InternalStreamsBuilder.AddGlobalStore(
                    storeBuilder,
                    sourceName,
                    topic,
                    new ConsumedInternal<K, V>(consumed),
                    processorName,
                    stateUpdateSupplier);

                return this;
            }

            /**
             * Adds a global {@link StateStore} to the topology.
             * The {@link StateStore} sources its data from all partitions of the provided input topic.
             * There will be exactly one instance of this {@link StateStore} per Kafka Streams instance.
             * <p>
             * A {@link SourceNode} with the provided sourceName will be added to consume the data arriving from the partitions
             * of the input topic.
             * <p>
             * The provided {@link IProcessorSupplier} will be used to create an {@link ProcessorNode} that will receive all
             * records forwarded from the {@link SourceNode}. NOTE: you should not use the {@code IProcessor} to insert transformed records into
             * the global state store. This store uses the source topic as changelog and during restore will insert records directly
             * from the source.
             * This {@link ProcessorNode} should be used to keep the {@link StateStore} up-to-date.
             * The default {@link ITimestampExtractor} as specified in the {@link StreamsConfig config} is used.
             * <p>
             * It is not required to connect a global store to {@link IProcessor Processors}, {@link Transformer Transformers},
             * or {@link ValueTransformer ValueTransformer}; those have read-only access to all global stores by default.
             *
             * @param storeBuilder          user defined {@link StoreBuilder}; can't be {@code null}
             * @param topic                 the topic to source the data from
             * @param consumed              the instance of {@link Consumed} used to define optional parameters; can't be {@code null}
             * @param stateUpdateSupplier   the instance of {@link IProcessorSupplier}
             * @return itself
             * @throws TopologyException if the processor of state is already registered
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public StreamsBuilder AddGlobalStore<K, V, T>(
                IStoreBuilder<T> storeBuilder,
                string topic,
                Consumed<K, V> consumed,
                IProcessorSupplier<K, V> stateUpdateSupplier)
                where T : IStateStore
            {
                storeBuilder = storeBuilder ?? throw new ArgumentNullException(nameof(storeBuilder));
                consumed = consumed ?? throw new ArgumentNullException(nameof(consumed));

                InternalStreamsBuilder.AddGlobalStore(
                    storeBuilder,
                    topic,
                    new ConsumedInternal<K, V>(consumed),
                    stateUpdateSupplier);

                return this;
            }

            /**
             * Returns the {@link Topology} that represents the specified processing logic.
             * Note that using this method means no optimizations are performed.
             *
             * @return the {@link Topology} that represents the specified processing logic
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public virtual Topology Build()
            {
                return Build(null);
            }

            /**
             * Returns the {@link Topology} that represents the specified processing logic and accepts
             * a {@link Properties} instance used to indicate whether to optimize topology or not.
             *
             * @param props the {@link Properties} used for building possibly optimized topology
             * @return the {@link Topology} that represents the specified processing logic
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public virtual Topology Build(StreamsConfig? config)
            {
                this.InternalStreamsBuilder.BuildAndOptimizeTopology(config);

                return Topology;
            }

            public static string GetTaskProducerClientId(string threadClientId, TaskId taskId)
                => $"{threadClientId}-{taskId}-producer";

            public static string GetThreadProducerClientId(string threadClientId)
                => $"{threadClientId}-producer";

            public static string GetConsumerClientId(string threadClientId)
                => $"{threadClientId}-consumer";

            public static string GetRestoreConsumerClientId(string threadClientId)
                => $"{threadClientId}-restore-consumer";

            // currently admin client is shared among all threads
            public static string GetSharedAdminClientId(string clientId)
                => $"{clientId}-admin";
        }
    }
}
