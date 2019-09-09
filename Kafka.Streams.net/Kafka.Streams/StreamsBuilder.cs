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
using Kafka.Common;
using Kafka.Common.Utils;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processor;
using Kafka.Streams.Processor.Internals;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;

namespace Kafka.Streams
{
    namespace Kafka.Streams
    {
        /// <summary>
        /// Provides the high-level Kafka Streams DSL to specify a Kafka Streams topology.
        /// </summary>
        public class StreamsBuilder
        {
            private readonly IConfiguration configuration;
            private readonly IServiceCollection serviceCollection;
            private readonly ServiceProvider services;

            /** The actual topology that is constructed by this StreamsBuilder. */
            private Topology topology;

            private InternalTopologyBuilder internalTopologyBuilder => topology?.internalTopologyBuilder ?? throw new InvalidOperationException($"{nameof(internalTopologyBuilder)} accessed without initializing {nameof(StreamsBuilder)}");
            private InternalStreamsBuilder internalStreamsBuilder;

            public StreamsBuilder(
                IConfiguration configuration,
                IServiceCollection serviceCollection)
            {
                this.configuration = configuration ?? new ConfigurationBuilder().Build();
                this.serviceCollection = serviceCollection ?? new ServiceCollection();

                this.BuildDependencyTree(this.configuration, this.serviceCollection);

                this.services = this.serviceCollection.BuildServiceProvider();

                this.topology = this.services.GetRequiredService<Topology>();
                this.internalStreamsBuilder = this.services.GetRequiredService<InternalStreamsBuilder>();
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

            public KafkaStreams BuildKafkaStreams()
                => this.services.GetRequiredService<KafkaStreams>();

            private void BuildDependencyTree(IConfiguration configuration, IServiceCollection serviceCollection)
            {
                serviceCollection.TryAddSingleton<KafkaStreams>(sp =>
                {
                    var ks = ActivatorUtilities.CreateInstance<KafkaStreams>(sp,
                        sp.GetRequiredService<ILogger<KafkaStreams>>(),
                        sp.GetRequiredService<KafkaStreamsState>(),
                        this.build(),
                        sp.GetRequiredService<StreamsConfig>(),
                        sp);

                    return ks;
                });
                serviceCollection.TryAddSingleton<KafkaStreamsState>();

                serviceCollection.TryAddSingleton<GlobalStreamThread>();
                serviceCollection.TryAddSingleton<GlobalStreamThreadState>();

                serviceCollection.TryAddScoped<StreamThread>();
                serviceCollection.TryAddScoped<StreamThreadState>();

                serviceCollection.TryAddSingleton<StreamStateListener>();

                serviceCollection.TryAddSingleton<InternalTopologyBuilder>();
                serviceCollection.TryAddSingleton<InternalStreamsBuilder>();
                serviceCollection.TryAddSingleton<Topology>();
                serviceCollection.TryAddSingleton(configuration);
                serviceCollection.TryAddSingleton(serviceCollection);
            }

            /**
             * Create a {@link KStream} from the specified topic.
             * The default {@code "auto.offset.reset"} strategy, default {@link ITimestampExtractor}, and default key and value
             * deserializers as specified in the {@link StreamsConfig config} are used.
             * <p>
             * If multiple topics are specified there is no ordering guarantee for records from different topics.
             * <p>
             * Note that the specified input topic must be partitioned by key.
             * If this is not the case it is the user's responsibility to repartition the data before any key based operation
             * (like aggregation or join) is applied to the returned {@link KStream}.
             *
             * @param topic the topic name; cannot be {@code null}
             * @return a {@link KStream} for the specified topic
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public IKStream<K, V> stream<K, V>(string topic)
            {
                return stream<K, V>(new List<string>() { topic }.AsReadOnly());
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
            public IKStream<K, V> stream<K, V>(
                string topic,
                Consumed<K, V> consumed)
            {
                return stream(topic, consumed);
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
            public IKStream<K, V> stream<K, V>(IReadOnlyList<string> topics)
            {
                return stream(
                    topics,
                    Consumed<K, V>.with(null, null, null, null));
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
            public IKStream<K, V> stream<K, V>(
                IReadOnlyList<string> topics,
                Consumed<K, V> consumed)
            {
                topics = topics ?? throw new ArgumentNullException("topics can't be null", nameof(topics));
                consumed = consumed ?? throw new ArgumentNullException("consumed can't be null", nameof(consumed));

                return internalStreamsBuilder.Stream(topics, new ConsumedInternal<K, V>(consumed));
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
            public IKStream<K, V> stream<K, V>(Regex topicPattern)
            {
                return stream(topicPattern, Consumed<K, V>.with(null, null));
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
            public IKStream<K, V> stream<K, V>(
                Regex topicPattern,
                Consumed<K, V> consumed)
            {
                topicPattern = topicPattern ?? throw new ArgumentNullException("topic can't be null", nameof(topicPattern));
                consumed = consumed ?? throw new ArgumentNullException("consumed can't be null", nameof(consumed));

                return null;
                //internalStreamsBuilder.stream(topicPattern, new ConsumedInternal<K, V>(consumed));
            }

            /**
             * Create a {@link KTable} for the specified topic.
             * The {@code "auto.offset.reset"} strategy, {@link ITimestampExtractor}, key and value deserializers
             * are defined by the options in {@link Consumed} are used.
             * Input {@link KeyValue records} with {@code null} key will be dropped.
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
             * streamBuilder.table(topic, Consumed.with(Serde.String(), Serde.String()), Materialized.<String, String, KeyValueStore<Bytes, byte[]>as(storeName))
             * }
             * </pre>
             * To query the local {@link KeyValueStore} it must be obtained via
             * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
             * <pre>{@code
             * KafkaStreams streams = ...
             * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
             * String key = "some-key";
             * Long valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
             * }</pre>
             * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
             * query the value of the key on a parallel running instance of your Kafka Streams application.
             *
             * @param topic              the topic name; cannot be {@code null}
             * @param consumed           the instance of {@link Consumed} used to define optional parameters; cannot be {@code null}
             * @param materialized       the instance of {@link Materialized} used to materialize a state store; cannot be {@code null}
             * @return a {@link KTable} for the specified topic
             */
            //[MethodImpl(MethodImplOptions.Synchronized)]
            //public IKTable<K, V> table<K, V>(
            //    string topic,
            //    Consumed<K, V> consumed,
            //    Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
            //{
            //    Objects.requireNonNull(topic, "topic can't be null");
            //    Objects.requireNonNull(consumed, "consumed can't be null");
            //    Objects.requireNonNull(materialized, "materialized can't be null");

            //    ConsumedInternal<K, V> consumedInternal = new ConsumedInternal<K, V>(consumed);
            //    materialized.withKeySerde(consumedInternal.keySerde).withValueSerde(consumedInternal.valueSerde);

            //    MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal =
            //         new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(materialized, internalStreamsBuilder, topic + "-");

            //    return internalStreamsBuilder.table(topic, consumedInternal, materializedInternal);
            //}

            /**
             * Create a {@link KTable} for the specified topic.
             * The default {@code "auto.offset.reset"} strategy and default key and value deserializers as specified in the
             * {@link StreamsConfig config} are used.
             * Input {@link KeyValue records} with {@code null} key will be dropped.
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
            //[MethodImpl(MethodImplOptions.Synchronized)]
            //public IKTable<K, V> table<K, V>(string topic)
            //{
            //    return table(topic, new ConsumedInternal<K, V>());
            //}

            /**
             * Create a {@link KTable} for the specified topic.
             * The {@code "auto.offset.reset"} strategy, {@link ITimestampExtractor}, key and value deserializers
             * are defined by the options in {@link Consumed} are used.
             * Input {@link KeyValue records} with {@code null} key will be dropped.
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
            //[MethodImpl(MethodImplOptions.Synchronized)]
            //public IKTable<K, V> table<K, V>(
            //    string topic,
            //    Consumed<K, V> consumed)
            //{
            //    Objects.requireNonNull(topic, "topic can't be null");
            //    Objects.requireNonNull(consumed, "consumed can't be null");
            //    var consumedInternal = new ConsumedInternal<K, V>(consumed);

            //    var materializedInternal =
            //         new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(
            //                 Materialized<K, V, IKeyValueStore<Bytes, byte[]>>.with(consumedInternal.keySerde, consumedInternal.valueSerde),
            //                 internalStreamsBuilder, topic + "-");

            //    return internalStreamsBuilder.table(topic, consumedInternal, materializedInternal);
            //}

            /**
             * Create a {@link KTable} for the specified topic.
             * The default {@code "auto.offset.reset"} strategy as specified in the {@link StreamsConfig config} are used.
             * Key and value deserializers as defined by the options in {@link Materialized} are used.
             * Input {@link KeyValue records} with {@code null} key will be dropped.
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
            //[MethodImpl(MethodImplOptions.Synchronized)]
            //public IKTable<K, V> table<K, V>(
            //    string topic,
            //    Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
            //{
            //    Objects.requireNonNull(topic, "topic can't be null");
            //    Objects.requireNonNull(materialized, "materialized can't be null");

            //    var materializedInternal =
            //         new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(materialized, internalStreamsBuilder, topic + "-");

            //    var consumedInternal =
            //            new ConsumedInternal<K, V>(Consumed<K, V>.with(materializedInternal.keySerde, materializedInternal.valueSerde));

            //    return internalStreamsBuilder.table(topic, consumedInternal, materializedInternal);
            //}

            /**
             * Create a {@link GlobalKTable} for the specified topic.
             * Input {@link KeyValue records} with {@code null} key will be dropped.
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
             * Input {@link KeyValue records} with {@code null} key will be dropped.
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
            public IGlobalKTable<K, V> globalTable<K, V>(string topic)
            {
                return null; // globalTable<K, V>(topic, Consumed<K, V>.with(null, null));
            }

            /**
             * Create a {@link GlobalKTable} for the specified topic.
             *
             * Input {@link KeyValue} pairs with {@code null} key will be dropped.
             * <p>
             * The resulting {@link GlobalKTable} will be materialized in a local {@link KeyValueStore} configured with
             * the provided instance of {@link Materialized}.
             * However, no internal changelog topic is created since the original input topic can be used for recovery (cf.
             * methods of {@link KGroupedStream} and {@link KGroupedTable} that return a {@link KTable}).
             * <p>
             * You should only specify serdes in the {@link Consumed} instance as these will also be used to overwrite the
             * serdes in {@link Materialized}, i.e.,
             * <pre> {@code
             * streamBuilder.globalTable(topic, Consumed.with(Serde.String(), Serde.String()), Materialized.<String, String, KeyValueStore<Bytes, byte[]>as(storeName))
             * }
             * </pre>
             * To query the local {@link KeyValueStore} it must be obtained via
             * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
             * <pre>{@code
             * KafkaStreams streams = ...
             * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
             * String key = "some-key";
             * Long valueForKey = localStore.get(key);
             * }</pre>
             * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
             * regardless of the specified value in {@link StreamsConfig} or {@link Consumed}.
             *
             * @param topic         the topic name; cannot be {@code null}
             * @param consumed      the instance of {@link Consumed} used to define optional parameters; can't be {@code null}
             * @param materialized   the instance of {@link Materialized} used to materialize a state store; cannot be {@code null}
             * @return a {@link GlobalKTable} for the specified topic
             */
            //[MethodImpl(MethodImplOptions.Synchronized)]
            //public IGlobalKTable<K, V> globalTable<K, V>(
            //    string topic,
            //    Consumed<K, V> consumed,
            //    Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
            //{
            //    Objects.requireNonNull(topic, "topic can't be null");
            //    Objects.requireNonNull(consumed, "consumed can't be null");
            //    Objects.requireNonNull(materialized, "materialized can't be null");

            //    ConsumedInternal<K, V> consumedInternal = new ConsumedInternal<K, V>(consumed);
            //    // always use the serdes from consumed
            //    materialized.withKeySerde(consumedInternal.keySerde).withValueSerde(consumedInternal.valueSerde);

            //    MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal =
            //         new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(materialized, internalStreamsBuilder, topic + "-");

            //    return internalStreamsBuilder.globalTable(topic, consumedInternal, materializedInternal);
            //}

            /**
             * Create a {@link GlobalKTable} for the specified topic.
             *
             * Input {@link KeyValue} pairs with {@code null} key will be dropped.
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
             * ReadOnlyKeyValueStore<String, Long> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<String, Long>keyValueStore());
             * String key = "some-key";
             * Long valueForKey = localStore.get(key);
             * }</pre>
             * Note that {@link GlobalKTable} always applies {@code "auto.offset.reset"} strategy {@code "earliest"}
             * regardless of the specified value in {@link StreamsConfig}.
             *
             * @param topic         the topic name; cannot be {@code null}
             * @param materialized   the instance of {@link Materialized} used to materialize a state store; cannot be {@code null}
             * @return a {@link GlobalKTable} for the specified topic
             */
            //[MethodImpl(MethodImplOptions.Synchronized)]
            //public IGlobalKTable<K, V> globalTable<K, V>(
            //    string topic,
            //    Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
            //{
            //    Objects.requireNonNull(topic, "topic can't be null");
            //    Objects.requireNonNull(materialized, "materialized can't be null");
            //    MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal =
            //         new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(
            //             materialized, internalStreamsBuilder, topic + "-");

            //    return internalStreamsBuilder.globalTable(
            //        topic,
            //        new ConsumedInternal<K, V>(
            //            Consumed<K, V>.with(materializedInternal.keySerde, materializedInternal.valueSerde)),
            //            materializedInternal);
            //}


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
            public StreamsBuilder addStateStore(IStoreBuilder builder)
            {
                Objects.requireNonNull(builder, "builder can't be null");
                //internalStreamsBuilder.addStateStore(builder);
                return this;
            }

            /**
             * @deprecated use {@link #addGlobalStore(StoreBuilder, String, Consumed, IProcessorSupplier)} instead
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public StreamsBuilder addGlobalStore<K, V>(
                IStoreBuilder storeBuilder,
                string topic,
                string sourceName,
                Consumed<K, V> consumed,
                string processorName,
                IProcessorSupplier<K, V> stateUpdateSupplier)
            {
                Objects.requireNonNull(storeBuilder, "storeBuilder can't be null");
                Objects.requireNonNull(consumed, "consumed can't be null");
                //internalStreamsBuilder.addGlobalStore(
                //    storeBuilder,
                //    sourceName,
                //    topic,
                //    new ConsumedInternal<K, V>(consumed),
                //    processorName,
                //    stateUpdateSupplier);

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
            public StreamsBuilder addGlobalStore<K, V>(
                IStoreBuilder storeBuilder,
                string topic,
                Consumed<K, V> consumed,
                IProcessorSupplier<K, V> stateUpdateSupplier)
            {
                Objects.requireNonNull(storeBuilder, "storeBuilder can't be null");
                Objects.requireNonNull(consumed, "consumed can't be null");
                //internalStreamsBuilder.addGlobalStore(
                //    storeBuilder,
                //    topic,
                //    new ConsumedInternal<K, V>(consumed),
                //    stateUpdateSupplier);

                return this;
            }

            /**
             * Returns the {@link Topology} that represents the specified processing logic.
             * Note that using this method means no optimizations are performed.
             *
             * @return the {@link Topology} that represents the specified processing logic
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public Topology build()
            {
                return build(null);
            }

            /**
             * Returns the {@link Topology} that represents the specified processing logic and accepts
             * a {@link Properties} instance used to indicate whether to optimize topology or not.
             *
             * @param props the {@link Properties} used for building possibly optimized topology
             * @return the {@link Topology} that represents the specified processing logic
             */
            [MethodImpl(MethodImplOptions.Synchronized)]
            public Topology build(StreamsConfig config)
            {
                //InternalStreamsBuilder.buildAndOptimizeTopology(config);

                return topology;
            }
        }
    }
}