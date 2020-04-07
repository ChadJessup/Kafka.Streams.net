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
using Kafka.Streams.NullModels;

namespace Kafka.Streams
{
    namespace Kafka.Streams
    {
        /// <summary>
        /// Provides the high-level Kafka Streams DSL to specify a Kafka Streams topology.
        /// </summary>
        public partial class StreamsBuilder
        {
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

                return this.Context.InternalStreamsBuilder.Stream(topics, new ConsumedInternal<K, V>(consumed));
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

                return this.Context.InternalStreamsBuilder.Stream(topicPattern, new ConsumedInternal<K, V>(consumed));
            }
        }
    }
}
