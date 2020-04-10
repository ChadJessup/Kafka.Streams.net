using System;
using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Clients;
using Kafka.Streams.Clients.Consumers;
using Kafka.Streams.Clients.Producers;
using Kafka.Streams.Configs;
using Kafka.Streams.Factories;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.NullModels;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Tasks;
using Kafka.Streams.Threads;
using Kafka.Streams.Threads.GlobalStream;
using Kafka.Streams.Threads.KafkaStreams;
using Kafka.Streams.Threads.Stream;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Kafka.Streams
{
    namespace Kafka.Streams
    {
        /// <summary>
        /// Provides the high-level Kafka Streams DSL to specify a Kafka Streams topology.
        /// </summary>
        public partial class StreamsBuilder
        {
            public KafkaStreamsContext Context { get; set; }

            public StreamsBuilder(
                IConfiguration? configuration = null,
                IServiceCollection? serviceCollection = null)
            {
                configuration ??= new ConfigurationBuilder().Build();
                serviceCollection ??= new ServiceCollection();
                serviceCollection = this.BuildDependencyTree(configuration, serviceCollection);

                // Add an empty configuration if none provided.
                var config = new StreamsConfig
                {
                    ApplicationId = "ApplicationId_NotSet",
                    BootstrapServers = "localhost:9092",
                };

                serviceCollection.TryAddSingleton<StreamsConfig>(config);

                var services = serviceCollection.BuildServiceProvider();
                this.Context = services.GetRequiredService<KafkaStreamsContext>();
                this.Context = this.AddCircularDependencies(this.Context, services);
            }

            private KafkaStreamsContext AddCircularDependencies(KafkaStreamsContext context, IServiceProvider services)
            {
                // Dependency Injection doesn't allow ctor injection with circular dependencies.
                // We'll manually inject these dependencies here...
                var internalStreamsBuilder = services.GetRequiredService<InternalStreamsBuilder>();
                context.InternalStreamsBuilder = internalStreamsBuilder;

                return context;
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
                var kafkaStreamsThread = this.Context.GetRequiredService<IKafkaStreamsThread>();
                kafkaStreamsThread.SetStateListener(kafkaStreamsThread.GetStateListener());

                return kafkaStreamsThread;
            }

            public IServiceCollection BuildDependencyTree(IConfiguration configuration, IServiceCollection serviceCollection)
            {
                serviceCollection.AddLogging();
                serviceCollection.AddOptions();

                serviceCollection.TryAddSingleton(configuration);
                serviceCollection.TryAddSingleton(serviceCollection);
                serviceCollection.TryAddSingleton<IClock, SystemClock>();

                this.AddNodes(serviceCollection);
                this.AddThreads(serviceCollection);
                this.AddClients(serviceCollection);
                this.AddFactories(serviceCollection);
                this.AddNullModels(serviceCollection);

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
                serviceCollection.TryAddSingleton<KafkaStreamsContext>();

                return serviceCollection;
            }

            /// <summary>
            /// Add Null dependencies to handle cases where no implementation
            /// has been provided (e.g., RockDb Persistent store not added).
            /// And we don't want to crash unexpectedly.
            /// </summary>
            /// <remarks>
            /// These will not be injected automatically via the associated
            /// interfaces, there will be fallback paths to provide these,
            /// if needed.
            /// </remarks>
            /// <param Name="serviceCollection"></param>
            private void AddNullModels(IServiceCollection serviceCollection)
            {
                serviceCollection.AddSingleton(new NullStoreSupplier());
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
                //serviceCollection.TryAddTransient<Serde<byte[]>>(sp => Serdes.ByteArray() as Serde<byte[]>);
                serviceCollection.TryAddTransient<ISerializer<byte[]>>(sp => Serdes.ByteArray().Serializer);

                serviceCollection.TryAddTransient<IStoresFactory, StoresFactory>();

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

                this.Context.InternalStreamsBuilder.AddStateStore<K, V, T>(builder);

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

                this.Context.InternalStreamsBuilder.AddGlobalStore(
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
             * The {@link StateStore} sources its data from All partitions of the provided input topic.
             * There will be exactly one instance of this {@link StateStore} per Kafka Streams instance.
             * <p>
             * A {@link SourceNode} with the provided sourceName will be added to consume the data arriving from the partitions
             * of the input topic.
             * <p>
             * The provided {@link IProcessorSupplier} will be used to create an {@link ProcessorNode} that will receive All
             * records forwarded from the {@link SourceNode}. NOTE: you should not use the {@code IProcessor} to insert transformed records into
             * the global state store. This store uses the source topic as changelog and during restore will insert records directly
             * from the source.
             * This {@link ProcessorNode} should be used to keep the {@link StateStore} up-to-date.
             * The default {@link ITimestampExtractor} as specified in the {@link StreamsConfig config} is used.
             * <p>
             * It is not required to connect a global store to {@link IProcessor Processors}, {@link Transformer Transformers},
             * or {@link ValueTransformer ValueTransformer}; those have read-only access to All global stores by default.
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

                this.Context.InternalStreamsBuilder.AddGlobalStore(
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
                return this.Build(null);
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
                this.Context.InternalStreamsBuilder.BuildAndOptimizeTopology(config);

                return this.Context.Topology;
            }

            public static string GetTaskProducerClientId(string threadClientId, TaskId taskId)
                => $"{threadClientId}-{taskId}-producer";

            public static string GetThreadProducerClientId(string threadClientId)
                => $"{threadClientId}-producer";

            public static string GetConsumerClientId(string threadClientId)
                => $"{threadClientId}-consumer";

            public static string GetRestoreConsumerClientId(string threadClientId)
                => $"{threadClientId}-restore-consumer";

            // currently admin client is shared among All threads
            public static string GetSharedAdminClientId(string clientId)
                => $"{clientId}-admin";
        }
    }
}
