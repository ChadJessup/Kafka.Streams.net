using Confluent.Kafka;
using Kafka.Common.Extensions;
using Kafka.Streams.Clients.Consumers;
using Kafka.Streams.Error;
using Kafka.Streams.Errors;
using Kafka.Streams.Errors.Interfaces;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Kafka.Streams.Configs
{
    /**
     * Configuration for a {@link KafkaStreams} instance.
     * Can also be used to configure the Kafka Streams internal {@link KafkaConsumer}, {@link KafkaProducer} and {@link Admin}.
     * To avoid consumer/producer/admin property conflicts, you should prefix those properties using
     * {@link #consumerPrefix(string)}, {@link #producerPrefix(string)} and {@link #adminClientPrefix(string)}, respectively.
     * <p>
     * Example:
     * <pre>{@code
     * // potentially wrong: sets "metadata.max.age.ms" to 1 minute for producer AND consumer
     * Properties streamsProperties = new Properties();
     * streamsProperties.Add(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 60000);
     * // or
     * streamsProperties.Add(ProducerConfig.METADATA_MAX_AGE_CONFIG, 60000);
     *
     * // suggested:
     * Properties streamsProperties = new Properties();
     * // sets "metadata.max.age.ms" to 1 minute for consumer only
     * streamsProperties.Add(StreamsConfig.consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), 60000);
     * // sets "metadata.max.age.ms" to 1 minute for producer only
     * streamsProperties.Add(StreamsConfig.producerPrefix(ProducerConfig.METADATA_MAX_AGE_CONFIG), 60000);
     *
     * StreamsConfig streamsConfig = new StreamsConfig(streamsProperties);
     * }</pre>
     *
     * This instance can also be used to pass in custom configurations to different modules (e.g. passing a special config in your customized serde).
     * The consumer/producer/admin prefix can also be used to distinguish these custom config values passed to different clients with the same config Name.
     * * Example:
     * <pre>{@code
     * Properties streamsProperties = new Properties();
     * // sets "my.custom.config" to "foo" for consumer only
     * streamsProperties.Add(StreamsConfig.consumerPrefix("my.custom.config"), "foo");
     * // sets "my.custom.config" to "bar" for producer only
     * streamsProperties.Add(StreamsConfig.producerPrefix("my.custom.config"), "bar");
     * // sets "my.custom.config2" to "boom" for All clients universally
     * streamsProperties.Add("my.custom.config2", "boom");
     *
     * // as a result, inside producer's serde configure(..) function,
     * // users can now read both key-value pairs "my.custom.config" => "foo"
     * // and "my.custom.config2" => "boom" from the config map
     * StreamsConfig streamsConfig = new StreamsConfig(streamsProperties);
     * }</pre>
     *
     *
     * When increasing {@link ProducerConfig#MAX_BLOCK_MS_CONFIG} to be more resilient to non-available brokers you should also
     * increase {@link ConsumerConfig#MAX_POLL_INTERVAL_MS_CONFIG} using the following guidance:
     * <pre>
     *     max.poll.interval.ms > max.block.ms
     * </pre>
     *
     *
     * Kafka Streams requires at least the following properties to be set:
     * <ul>
     *  <li>{@link #ApplicationIdConfig "application.id"}</li>
     *  <li>{@link #BootstrapServers "bootstrap.servers"}</li>
     * </ul>
     *
     * By default, Kafka Streams does not allow users to overwrite the following properties (Streams setting shown in parentheses):
     * <ul>
     *   <li>{@link ConsumerConfig#ENABLE_AUTO_COMMIT_CONFIG "enable.auto.commit"} (false) - Streams client will always disable/turn off auto committing</li>
     * </ul>
     *
     * If {@link #PROCESSING_GUARANTEE_CONFIG "processing.guarantee"} is set to {@link #StreamsConfig.ExactlyOnceConfig "exactly_once"}, Kafka Streams does not allow users to overwrite the following properties (Streams setting shown in parentheses):
     * <ul>
     *   <li>{@link ConsumerConfig#ISOLATION_LEVEL_CONFIG "isolation.level"} (read_committed) - Consumers will always read committed data only</li>
     *   <li>{@link ProducerConfig#ENABLE_IDEMPOTENCE_CONFIG "enable.idempotence"} (true) - Producer will always have idempotency enabled</li>
     *   <li>{@link ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION "max.in.flight.requests.per.connection"} (5) - Producer will always have one in-flight request per connection</li>
     * </ul>
     *
     *
     * @see KafkaStreams#KafkaStreams(org.apache.kafka.streams.Topology, Properties)
     * @see ConsumerConfig
     * @see ProducerConfig
     */
    public partial class StreamsConfig : Config
    {
        private static readonly string[] NON_CONFIGURABLE_CONSUMER_DEFAULT_CONFIGS = new string[] { StreamsConfig.ENABLE_AUTO_COMMIT_CONFIGConfig };
        private static readonly string[] NON_CONFIGURABLE_CONSUMER_EOS_CONFIGS = new string[] { StreamsConfig.ISOLATION_LEVEL_CONFIGConfig };
        private static readonly string[] NON_CONFIGURABLE_PRODUCER_EOS_CONFIGS = new string[] { StreamsConfig.ENABLE_IDEMPOTENCE_CONFIGConfig, StreamsConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTIONConfig };

        /// <summary>
        /// Initialize a new <see cref="StreamsConfig" /> instance.
        /// </summary>
        public StreamsConfig()
            : base()
        {
            this.SetDefaultConfiguration();
        }

        /// <summary>
        /// Initialize a new <see cref="StreamsConfig" /> instance based on
        /// an existing <see cref="Config" /> instance.
        /// </summary>
        public StreamsConfig(Config config)
            : base(config)
        {
            this.SetDefaultConfiguration();
            this.SetAll(config);
        }

        /**
         * Create a new {@code StreamsConfig} using the given properties.
         *
         * @param props properties that specify Kafka Streams and internal consumer/producer configuration
         */
        public StreamsConfig(IDictionary<string, string?> config)
            : this(new Config(config))
        {
            //   eosEnabled = StreamsConfig.ExactlyOnceConfig.Equals(getString(PROCESSING_GUARANTEE_CONFIG));
        }

        public void Put(string key, string value)
            => this.Set(key, value);

        public TopologyOptimization? TopologyOptimization
        {
            get => (TopologyOptimization?)this.GetEnum(typeof(TopologyOptimization), StreamsConfig.TOPOLOGY_OPTIMIZATIONConfig);
            set => this.SetObject(StreamsConfig.TOPOLOGY_OPTIMIZATIONConfig, value);
        }

        public string GroupId
        {
            get => this.Get(StreamsConfig.GroupIdConfig);
            set => this.SetObject(StreamsConfig.GroupIdConfig, value);
        }

        public string ApplicationId
        {
            get => this.Get(StreamsConfig.ApplicationIdConfig);
            set => this.SetObject(StreamsConfig.ApplicationIdConfig, value);
        }

        public string PartitionAssignmentStrategy
        {
            get => this.Get(StreamsConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIGConfig);
            set => this.SetObject(StreamsConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIGConfig, value);
        }

        public long StateCleanupDelayMs
        {
            get => this.GetLong(StreamsConfig.StateCleanupDelayMsConfig) ?? 10 * 60 * 1000L;
            set => this.SetObject(StreamsConfig.StateCleanupDelayMsConfig, value);
        }

        public TimeSpan MaxTaskIdleDuration
        {
            get => TimeSpan.FromMilliseconds(this.GetLong(StreamsConfig.MAX_TASK_IDLE_MS_CONFIGConfig) ?? 0L);
            set => this.SetObject(StreamsConfig.MAX_TASK_IDLE_MS_CONFIGConfig, value.TotalMilliseconds);
        }

        public string ApplicationServer
        {
            get => this.Get(StreamsConfig.ApplicationServerConfig) ?? "";
            set => this.SetObject(StreamsConfig.ApplicationServerConfig, value);
        }

        public string BootstrapServers
        {
            get => this.Get(StreamsConfig.BootstrapServersConfig);
            set => this.Set(StreamsConfig.BootstrapServersConfig, value);
        }

        public string ClientId
        {
            get => this.Get(StreamsConfig.ClientIdConfig);
            set => this.SetObject(StreamsConfig.ClientIdConfig, value);
        }

        public Type DefaultKeySerdeType
        {
            get => Type.GetType(this.Get(StreamsConfig.DefaultKeySerdeClassConfig));
            set => this.SetObject(StreamsConfig.DefaultKeySerdeClassConfig, value?.AssemblyQualifiedName);
        }


        public Type DefaultValueSerdeType
        {
            get => Type.GetType(this.Get(StreamsConfig.DefaultValueSerdeClassConfig));
            set => this.SetObject(StreamsConfig.DefaultValueSerdeClassConfig, value?.AssemblyQualifiedName);
        }

        public Type DefaultTimestampExtractorType
        {
            get => Type.GetType(this.Get(StreamsConfig.DefaultTimestampExtractorClassConfig));
            set => this.SetObject(StreamsConfig.DefaultTimestampExtractorClassConfig, value?.AssemblyQualifiedName);
        }

        public Type DefaultDeserializationExceptionHandler
        {
            get => Type.GetType(this.Get(StreamsConfig.DefaultDeserializationExceptionHandlerClassConfig));
            set => this.SetObject(StreamsConfig.DefaultDeserializationExceptionHandlerClassConfig, value?.AssemblyQualifiedName);
        }

        public Type DefaultProductionExceptionHandler
        {
            get => Type.GetType(this.Get(StreamsConfig.DefaultProductionExceptionHandlerClassConfig));
            set => this.Set(StreamsConfig.DefaultProductionExceptionHandlerClassConfig, value?.AssemblyQualifiedName);
        }

        public int NumberOfStreamThreads
        {
            get => this.GetInt(StreamsConfig.NumberOfStreamThreadsConfig) ?? 1;
            set => this.Set(StreamsConfig.NumberOfStreamThreadsConfig, value.ToString());
        }

        public int BufferedRecordsPerPartition
        {
            get => this.GetInt(StreamsConfig.BufferedRecordsPerPartitionConfig) ?? 1000;
            set => this.Set(StreamsConfig.BufferedRecordsPerPartitionConfig, value.ToString());
        }

        public long CacheMaxBytesBuffering
        {
            get => this.GetLong(StreamsConfig.CacheMaxBytesBufferingConfig) ?? 10485760L;
            set => this.Set(StreamsConfig.CacheMaxBytesBufferingConfig, value.ToString());
        }

        public bool EnableIdempotence
        {
            get => this.GetBool("enable.idempotence") ?? false;
            set => this.SetObject("enable.idempotence", value);
        }

        public int Retries
        {
            get => this.GetInt(StreamsConfig.RetriesConfig) ?? int.MaxValue;
            set => this.SetObject(StreamsConfig.RetriesConfig, value);
        }

        public long RetryBackoffMs
        {
            get => this.GetInt(StreamsConfig.RetryBackoffMsConfig) ?? 100L;
            set => this.SetObject(StreamsConfig.RetryBackoffMsConfig, value);
        }

        public DirectoryInfo StateStoreDirectory
        {
            get => new DirectoryInfo(this.GetString(StreamsConfig.StateDirPathConfig) ?? Path.Combine(Path.GetTempPath(), "kafka-streams"));
            set => this.Set(StreamsConfig.StateDirPathConfig, value.FullName);
        }

        public bool StateStoreIsPersistent
        {
            get => this.GetBool(StreamsConfig.StateDirHasPersistentStoresConfig) ?? false;
            set => this.Set(StreamsConfig.StateDirHasPersistentStoresConfig, value ? bool.TrueString : bool.FalseString);
        }

        /// <summary>
        ///     Automatically and periodically commit offsets in the background. Note: setting this to false does not prevent the consumer from fetching previously committed start offsets. To circumvent this behaviour set specific start offsets per partition in the call to assign().
        ///
        ///     default: true
        ///     importance: high
        /// </summary>
        public bool? EnableAutoCommit
        {
            get => this.GetBool(StreamsConfig.ENABLE_AUTO_COMMIT_CONFIGConfig);
            set => this.SetObject(StreamsConfig.ENABLE_AUTO_COMMIT_CONFIGConfig, value);
        }

        /// <summary>
        /// The frequency with which to save the position of the processor.
        /// (Note, if processing.guarantee is set to ExactlyOnce, the default value is
        /// EOS_DEFAULT_COMMIT_INTERVAL_MS otherwise the default value is DEFAULT_COMMIT_INTERVAL_MS.
        /// </summary>
        public long CommitIntervalMs
        {
            get => this.GetLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIGConfig) ?? 30000L;
            set => this.Set(StreamsConfig.COMMIT_INTERVAL_MS_CONFIGConfig, value.ToString());
        }

        /// <summary>
        ///     Action to take when there is no initial offset in offset store or the desired offset is out of range: 'smallest','earliest' - automatically reset the offset to the smallest offset, 'largest','latest' - automatically reset the offset to the largest offset, 'error' - trigger an error which is retrieved by consuming messages and checking 'message->err'.
        ///
        ///     default: largest
        ///     importance: high
        /// </summary>
        public AutoOffsetReset? AutoOffsetReset
        {
            get => (AutoOffsetReset?)this.GetEnum(typeof(AutoOffsetReset), "auto.offset.reset");
            set => this.SetObject("auto.offset.reset", value);
        }

        /// <summary>
        /// Gets or sets the amount of time in milliseconds to block waiting for input.
        /// </summary>
        public long PollMs
        {
            get => this.GetLong(StreamsConfig.PollMsConfig) ?? 100L;
            set => this.Set(StreamsConfig.PollMsConfig, value.ToString());
        }

        private void SetDefaultConfiguration()
        {
            this.EnableAutoCommit = false;
            this.DefaultTimestampExtractorType = typeof(FailOnInvalidTimestamp);

            this.DefaultDeserializationExceptionHandler = typeof(LogAndFailExceptionHandler);
            this.DefaultProductionExceptionHandler = typeof(DefaultProductionExceptionHandler);
            this.DefaultKeySerdeType = typeof(ISerde<byte[]>);
            this.DefaultValueSerdeType = typeof(ISerde<byte[]>);

            // TODO: make sure this and the producer configs that are required cannot be overridden:
            // NON_CONFIGURABLE_CONSUMER_DEFAULT_CONFIGS, etc
        }

        /**
         * Prefix a property with {@link #CONSUMER_PREFIX}. This is used to isolate {@link ConsumerConfig consumer configs}
         * from other client configs.
         *
         * @param consumerProp the consumer property to be masked
         * @return {@link #CONSUMER_PREFIX} + {@code consumerProp}
         */
        public static string ConsumerPrefix(string consumerProp)
        {
            return StreamsConfig.ConsumerPrefixConfig + consumerProp;
        }

        /**
         * Prefix a property with {@link #MAIN_CONSUMER_PREFIX}. This is used to isolate {@link ConsumerConfig main consumer configs}
         * from other client configs.
         *
         * @param consumerProp the consumer property to be masked
         * @return {@link #MAIN_CONSUMER_PREFIX} + {@code consumerProp}
         */
        public static string MainConsumerPrefix(string consumerProp)
        {
            return StreamsConfig.MainConsumerPrefixConfig + consumerProp;
        }

        /**
         * Prefix a property with {@link #RESTORE_CONSUMER_PREFIX}. This is used to isolate {@link ConsumerConfig restore consumer configs}
         * from other client configs.
         *
         * @param consumerProp the consumer property to be masked
         * @return {@link #RESTORE_CONSUMER_PREFIX} + {@code consumerProp}
         */
        public static string RestoreConsumerPrefix(string consumerProp)
        {
            return StreamsConfig.RestoreConsumerPrefixConfig + consumerProp;
        }

        /**
         * Prefix a property with {@link #GLOBAL_CONSUMER_PREFIX}. This is used to isolate {@link ConsumerConfig global consumer configs}
         * from other client configs.
         *
         * @param consumerProp the consumer property to be masked
         * @return {@link #GLOBAL_CONSUMER_PREFIX} + {@code consumerProp}
         */
        public static string GlobalConsumerPrefix(string consumerProp)
        {
            return StreamsConfig.GlobalConsumerPrefixConfig + consumerProp;
        }

        /**
         * Prefix a property with {@link #PRODUCER_PREFIX}. This is used to isolate {@link ProducerConfig producer configs}
         * from other client configs.
         *
         * @param producerProp the producer property to be masked
         * @return PRODUCER_PREFIX + {@code producerProp}
         */
        public static string ProducerPrefix(string producerProp)
        {
            return StreamsConfig.ProducerPrefixConfig + producerProp;
        }

        /**
         * Prefix a property with {@link #ADMIN_CLIENT_PREFIX}. This is used to isolate {@link AdminClientConfig admin configs}
         * from other client configs.
         *
         * @param adminClientProp the admin client property to be masked
         * @return ADMIN_CLIENT_PREFIX + {@code adminClientProp}
         */
        public static string AdminClientPrefix(string adminClientProp)
        {
            return StreamsConfig.AdminClientPrefixConfig + adminClientProp;
        }

        /**
         * Prefix a property with {@link #TOPIC_PREFIX}
         * used to provide default topic configs to be applied when creating internal topics.
         *
         * @param topicProp the topic property to be masked
         * @return TOPIC_PREFIX + {@code topicProp}
         */
        public static string TopicPrefix(string topicProp)
        {
            return StreamsConfig.TopicPrefixConfig + topicProp;
        }

        protected Dictionary<string, object> PostProcessParsedConfig(Dictionary<string, object> parsedValues)
        {
            var configUpdates = new Dictionary<string, object>();
            //   CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);

            //bool eosEnabled = StreamsConfig.ExactlyOnceConfig.Equals(parsedValues[PROCESSING_GUARANTEE_CONFIG]);
            //if (eosEnabled && !originals().ContainsKey(COMMIT_INTERVAL_MS_CONFIG))
            //{
            //    log.LogDebug("Using {} default value of {} as exactly once is enabled.",
            //            COMMIT_INTERVAL_MS_CONFIG, EOS_DEFAULT_COMMIT_INTERVAL_MS);
            //    configUpdates.Add(COMMIT_INTERVAL_MS_CONFIG, EOS_DEFAULT_COMMIT_INTERVAL_MS);
            //}

            return configUpdates;
        }

        private ConsumerConfig GetCommonConsumerConfigs()
        {
            var clientProvidedProps = this.GetClientPropsWithPrefix(StreamsConfig.ConsumerPrefixConfig, this);

            this.CheckIfUnexpectedUserSpecifiedConsumerConfig(clientProvidedProps, NON_CONFIGURABLE_CONSUMER_DEFAULT_CONFIGS);
            this.CheckIfUnexpectedUserSpecifiedConsumerConfig(clientProvidedProps, NON_CONFIGURABLE_CONSUMER_EOS_CONFIGS);

            var consumerProps = new Dictionary<string, string>(/*eosEnabled ? CONSUMER_EOS_OVERRIDES : CONSUMER_DEFAULT_OVERRIDES*/);
            consumerProps.PutAll(this.GetClientCustomProps());
            consumerProps.PutAll(clientProvidedProps);

            // bootstrap.servers should be from StreamsConfig
            if (consumerProps.ContainsKey(StreamsConfig.BootstrapServersConfig))
            {
                consumerProps[StreamsConfig.BootstrapServersConfig] = this.BootstrapServers;
            }
            else
            {
                consumerProps.Add(StreamsConfig.BootstrapServersConfig, this.properties[StreamsConfig.BootstrapServersConfig]);
            }

            if (!consumerProps.ContainsKey(StreamsConfig.GroupIdConfig))
            {
                consumerProps.Add(StreamsConfig.GroupIdConfig, this.properties[StreamsConfig.GroupIdConfig]);
            }

            return new ConsumerConfig(consumerProps);
        }

        private void CheckIfUnexpectedUserSpecifiedConsumerConfig(
            Dictionary<string, string> clientProvidedProps,
            string[] nonConfigurableConfigs)
        {
            if (clientProvidedProps is null)
            {
                throw new ArgumentNullException(nameof(clientProvidedProps));
            }

            if (nonConfigurableConfigs is null)
            {
                throw new ArgumentNullException(nameof(nonConfigurableConfigs));
            }

            // Streams does not allow users to configure certain consumer/producer configurations, for example,
            // enable.auto.commit. In cases where user tries to override such non-configurable
            // consumer/producer configurations, log a warning and Remove the user defined value from the Dictionary.
            // Thus the default values for these consumer/producer configurations that are suitable for
            // Streams will be used instead.

            //if (eosEnabled)
            //{
            //    object maxInFlightRequests = clientProvidedProps[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION];

            //    if (maxInFlightRequests != null)
            //    {
            //        int maxInFlightRequestsAsInteger;
            //        if (maxInFlightRequests is int)
            //        {
            //            maxInFlightRequestsAsInteger = (int)maxInFlightRequests;
            //        }
            //        else if (maxInFlightRequests is string)
            //        {
            //            try
            //            {
            //                maxInFlightRequestsAsInteger = int.Parse(((string)maxInFlightRequests).trim());
            //            }
            //            catch (Exception e)
            //            {
            //                throw new Exception(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequests, "string value could not be parsed as 32-bit integer");
            //            }
            //        }
            //        else
            //        {
            //            throw new Exception(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequests, "Expected value to be a 32-bit integer, but it was a " + maxInFlightRequests.GetType().getName());
            //        }

            //        if (maxInFlightRequestsAsInteger > 5)
            //        {
            //            throw new Exception(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequestsAsInteger, "Can't exceed 5 when exactly-once processing is enabled");
            //        }
            //    }
            //}

            //foreach (string config in nonConfigurableConfigs)
            //{
            //    if (clientProvidedProps.ContainsKey(config))
            //    {
            //        string eosMessage = PROCESSING_GUARANTEE_CONFIG + " is set to " + StreamsConfig.ExactlyOnceConfig + ". Hence, ";
            //        string nonConfigurableConfigMessage = "Unexpected user-specified %s config: %s found. %sUser setting (%s) will be ignored and the Streams default setting (%s) will be used ";

            //        if (CONSUMER_DEFAULT_OVERRIDES.ContainsKey(config))
            //        {
            //            if (!clientProvidedProps[config].Equals(CONSUMER_DEFAULT_OVERRIDES[config]))
            //            {
            //                log.LogWarning(string.Format(nonConfigurableConfigMessage, "consumer", config, "", clientProvidedProps[config], CONSUMER_DEFAULT_OVERRIDES[config]));
            //                clientProvidedProps.Remove(config);
            //            }
            //        }
            //        else if (eosEnabled)
            //        {
            //            if (CONSUMER_EOS_OVERRIDES.ContainsKey(config))
            //            {
            //                if (!clientProvidedProps[config].Equals(CONSUMER_EOS_OVERRIDES[config]))
            //                {
            //                    log.LogWarning(string.Format(nonConfigurableConfigMessage,
            //                            "consumer", config, eosMessage, clientProvidedProps[config], CONSUMER_EOS_OVERRIDES[config])];
            //                    clientProvidedProps.Remove(config);
            //                }
            //            }
            //            else if (PRODUCER_EOS_OVERRIDES.ContainsKey(config))
            //            {
            //                if (!clientProvidedProps[config].Equals(PRODUCER_EOS_OVERRIDES[config]))
            //                {
            //                    log.LogWarning(string.Format(nonConfigurableConfigMessage,
            //                            "producer", config, eosMessage, clientProvidedProps[config], PRODUCER_EOS_OVERRIDES[config])];
            //                    clientProvidedProps.Remove(config);
            //                }
            //            }
            //        }
            //    }
            //}
        }

        /**
         * Get the configs to the {@link KafkaConsumer main consumer}.
         * Properties using the prefix {@link #MAIN_CONSUMER_PREFIX} will be used in favor over
         * the properties prefixed with {@link #CONSUMER_PREFIX} and the non-prefixed versions
         * (read the override precedence ordering in {@link #MAIN_CONSUMER_PREFIX}
         * except in the case of {@link ConsumerConfig#BootstrapServers} where we always use the non-prefixed
         * version as we only support reading/writing from/to the same Kafka Cluster.
         * If not specified by {@link #MAIN_CONSUMER_PREFIX}, main consumer will share the general consumer configs
         * prefixed by {@link #CONSUMER_PREFIX}.
         *
         * @param groupId      consumer groupId
         * @param clientId     clientId
         * @param threadIdx    stream thread index
         * @return Dictionary of the consumer configuration.
         */
        public ConsumerConfig GetMainConsumerConfigs(string groupId, string clientId, int threadIdx)
        {
            var consumerProps = this.GetCommonConsumerConfigs();

            // Get main consumer override configs
            var mainConsumerProps = this.OriginalsWithPrefix(StreamsConfig.MainConsumerPrefixConfig);
            consumerProps.SetAll(mainConsumerProps);

            // this is a hack to work around StreamsConfig constructor inside StreamsPartitionAssignor to avoid casting
            // consumerProps.Set(StreamsConfig.ApplicationId, groupId);

            // add group id, client id with stream client id prefix, and group instance id
            consumerProps.GroupId = groupId;
            consumerProps.ClientId = clientId;

            var groupInstanceId = consumerProps.GroupId;

            // Suffix each thread consumer with thread.id to enforce uniqueness of group.instance.id.
            if (groupInstanceId != null)
            {
                consumerProps.GroupId = $"{groupInstanceId}-{threadIdx}";
            }

            // add configs required for stream partition assignor
            // consumerProps.Add(UPGRADE_FROM_CONFIG, getString(UPGRADE_FROM_CONFIG));
            // consumerProps.Add(StreamsConfig.REPLICATION_FACTOR_CONFIG, GetInt(StreamsConfig.REPLICATION_FACTOR_CONFIG));
            consumerProps.Set(StreamsConfig.ApplicationServerConfig, this.GetString(StreamsConfig.ApplicationServerConfig));
            consumerProps.Set(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIGConfig, this.GetString(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIGConfig));
            consumerProps.Set(StreamsConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIGConfig, this.GetString(typeof(StreamsPartitionAssignor).FullName));
            consumerProps.Set(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIGConfig, this.GetString(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIGConfig));

            // add admin retries configs for creating topics
            var adminClientDefaultConfig = new AdminClientConfig(this.GetClientPropsWithPrefix(StreamsConfig.AdminClientPrefixConfig, new AdminClientConfig()));
            // consumerProps.Set(adminClientPrefix(AdminClientConfig.RETRIES_CONFIG), adminClientDefaultConfig.GetInt(AdminClientConfig.RETRIES_CONFIG));
            // consumerProps.Add(adminClientPrefix(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG), adminClientDefaultConfig.GetLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG));

            // verify that producer batch config is no larger than segment size, then.Add topic configs required for creating topics
            Dictionary<string, string> topicProps = this.OriginalsWithPrefix(StreamsConfig.TopicPrefixConfig, stripPrefix: false);
            Dictionary<string, string> producerProps = this.GetClientPropsWithPrefix(StreamsConfig.ProducerPrefixConfig, new ProducerConfig());

            // if (topicProps.ContainsKey(topicPrefix(TopicConfig.SEGMENT_BYTES_CONFIG)) &&
            //     producerProps.ContainsKey(ProducerConfig.BATCH_SIZE_CONFIG))
            // {
            //     int segmentSize = int.Parse(topicProps[topicPrefix(TopicConfig.SEGMENT)_CONFIG)].ToString();
            //     int batchSize = int.Parse(producerProps[ProducerConfig.BATCH_SIZE_CONFIG].ToString());
            //
            //     if (segmentSize < batchSize)
            //     {
            //         throw new ArgumentException(
            //             $"Specified topic segment size {segmentSize} is is smaller than the" +
            //             $"configured producer batch size %d, this will cause produced batch not" +
            //             $"able to be appended to the topic",
            //                 batchSize.ToString());
            //     }
            // }

            consumerProps.SetAll(topicProps);

            return consumerProps;
        }

        public new int? GetInt(string key)
            => base.GetInt(key);

        public string? GetString(string key)
            => base.Get(key);

        /**
         * Get the configs for the {@link KafkaConsumer restore-consumer}.
         * Properties using the prefix {@link #RESTORE_CONSUMER_PREFIX} will be used in favor over
         * the properties prefixed with {@link #CONSUMER_PREFIX} and the non-prefixed versions
         * (read the override precedence ordering in {@link #RESTORE_CONSUMER_PREFIX}
         * except in the case of {@link ConsumerConfig#BootstrapServers} where we always use the non-prefixed
         * version as we only support reading/writing from/to the same Kafka Cluster.
         * If not specified by {@link #RESTORE_CONSUMER_PREFIX}, restore consumer will share the general consumer configs
         * prefixed by {@link #CONSUMER_PREFIX}.
         *
         * @param clientId clientId
         * @return Dictionary of the restore consumer configuration.
         */
        public RestoreConsumerConfig GetRestoreConsumerConfigs(string? clientId = null)
        {
            var baseConsumerProps = this.GetCommonConsumerConfigs();

            // Get restore consumer override configs
            var restoreConsumerProps = this.OriginalsWithPrefix(StreamsConfig.RestoreConsumerPrefixConfig);
            baseConsumerProps.SetAll(restoreConsumerProps);

            // add client id with stream client id prefix
            baseConsumerProps.ClientId = clientId;
            baseConsumerProps.AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest;

            if (string.IsNullOrWhiteSpace(baseConsumerProps.GroupId))
            {
                baseConsumerProps.GroupId = clientId;
            }

            return new RestoreConsumerConfig(baseConsumerProps);
        }

        /**
         * Get the configs for the {@link KafkaConsumer global consumer}.
         * Properties using the prefix {@link #GLOBAL_CONSUMER_PREFIX} will be used in favor over
         * the properties prefixed with {@link #CONSUMER_PREFIX} and the non-prefixed versions
         * (read the override precedence ordering in {@link #GLOBAL_CONSUMER_PREFIX}
         * except in the case of {@link ConsumerConfig#BootstrapServers} where we always use the non-prefixed
         * version as we only support reading/writing from/to the same Kafka Cluster.
         * If not specified by {@link #GLOBAL_CONSUMER_PREFIX}, global consumer will share the general consumer configs
         * prefixed by {@link #CONSUMER_PREFIX}.
         *
         * @param clientId clientId
         * @return Dictionary of the global consumer configuration.
         */
        public ConsumerConfig GetGlobalConsumerConfigs(string clientId)
        {
            var baseConsumerProps = this.GetCommonConsumerConfigs();

            // Get global consumer override configs
            var globalConsumerProps = this.OriginalsWithPrefix(StreamsConfig.GlobalConsumerPrefixConfig);

            baseConsumerProps.SetAll(globalConsumerProps);

            // no need to set group id for a global consumer
            // baseConsumerProps.Remove(StreamsConfig.GroupId);

            // add client id with stream client id prefix
            baseConsumerProps.ClientId = $"{clientId}-global-consumer";
            baseConsumerProps.AutoOffsetReset = null;

            return baseConsumerProps;
        }

        /**
         * Get the configs for the {@link KafkaProducer producer}.
         * Properties using the prefix {@link #PRODUCER_PREFIX} will be used in favor over their non-prefixed versions
         * except in the case of {@link ProducerConfig#BootstrapServers} where we always use the non-prefixed
         * version as we only support reading/writing from/to the same Kafka Cluster.
         *
         * @param clientId clientId
         * @return Dictionary of the producer configuration.
         */
        public ProducerConfig GetProducerConfigs(string clientId)
        {
            var producerConfig = new ProducerConfig();
            var clientProvidedProps = this.GetClientPropsWithPrefix(StreamsConfig.ProducerPrefixConfig, new ProducerConfig());

            this.CheckIfUnexpectedUserSpecifiedConsumerConfig(clientProvidedProps, NON_CONFIGURABLE_PRODUCER_EOS_CONFIGS);

            // generate producer configs from original properties and overridden maps
            producerConfig.SetAll(this.GetClientCustomProps());
            producerConfig.SetAll(clientProvidedProps);

            producerConfig.BootstrapServers = (string)this.properties[StreamsConfig.BootstrapServersConfig];

            // add client id with stream client id prefix
            producerConfig.ClientId = clientId;

            return producerConfig;
        }

        /**
         * Get the configs for the {@link Admin admin client}.
         * @param clientId clientId
         * @return Dictionary of the admin client configuration.
         */
        public AdminClientConfig GetAdminConfigs(string clientId)
        {
            var adminConfig = new AdminClientConfig(new Dictionary<string, string>());

            var clientProvidedProps = this.GetClientPropsWithPrefix(StreamsConfig.AdminClientPrefixConfig, this.properties);

            adminConfig.SetAll(this.GetClientCustomProps());
            adminConfig.SetAll(clientProvidedProps);

            // add client id with stream client id prefix
            adminConfig.ClientId = clientId;

            return adminConfig;
        }

        private Dictionary<string, string> GetClientPropsWithPrefix(string prefix, IEnumerable<KeyValuePair<string, string>> configKeyValuePairs)
            => this.GetClientPropsWithPrefix(prefix, new HashSet<string>(configKeyValuePairs.Select(kvp => kvp.Key)));

        private Dictionary<string, string> GetClientPropsWithPrefix(string prefix, HashSet<string> configNames)
        {
            var props = new Dictionary<string, string>();

            foreach (var original in this.OriginalsWithPrefix(prefix))
            {
                props.Add(original.Key, original.Value);
            }

            return props;
        }

        public long? GetLong(string key)
        {
            var keyAsLong = this.Get(key);

            if (long.TryParse(keyAsLong, out var value))
            {
                return value;
            }

            return null;
        }

        /**
         * Gets All original settings with the given prefix, stripping the prefix before adding it to the output.
         *
         * @param prefix the prefix to use as a filter
         * @return a Dictionary containing the settings with the prefix
         */
        private Dictionary<string, string> OriginalsWithPrefix(string prefix)
        {
            return this.OriginalsWithPrefix(prefix, stripPrefix: true);
        }

        /**
         * Gets All original settings with the given prefix.
         *
         * @param prefix the prefix to use as a filter
         * @param strip strip the prefix before adding to the output if set true
         * @return a Dictionary containing the settings with the prefix
         */
        private Dictionary<string, string> OriginalsWithPrefix(string prefix, bool stripPrefix)
        {
            var result = new Dictionary<string, string>();

            foreach (var entry in this.properties)
            {
                if (entry.Key.StartsWith(prefix) && entry.Key.Length > prefix.Length)
                {
                    if (stripPrefix)
                    {
                        result.Add(entry.Key.Substring(prefix.Length), entry.Value);
                    }
                    else
                    {
                        result.Add(entry.Key, entry.Value);
                    }
                }
            }

            return result;
        }

        /**
         * Get a map of custom configs by removing from the originals All the Streams, Consumer, Producer, and AdminClient configs.
         * Prefixed properties are also removed because they are already.Added by {@link #getClientPropsWithPrefix(string, Set)}.
         * This allows to set a custom property for a specific client alone if specified using a prefix, or for All
         * when no prefix is used.
         *
         * @return a map with the custom properties
         */
        private IDictionary<string, string> GetClientCustomProps()
        {
            //props.removeAll(CONFIG.names());

            return new Dictionary<string, string>(this.properties)
                .RemoveAll(new[]
                {
                    // TODO: make this automatic...
                    "processing.guarantee",
                    "buffered.records.per.partition",
                    "state.dir",
                    "application.id",
                    "default.timestamp.extractor",
                    "default.deserialization.exception.handler",
                    "default.production.exception.handler",
                    "default.key.serde",
                    "default.value.serde",
                    "num.stream.threads",
                    "cache.Max.bytes.buffering",
                    "commit.interval.ms",
                })
                .RemoveAll(this.OriginalsWithPrefix(StreamsConfig.ConsumerPrefixConfig, stripPrefix: false).Keys)
                .RemoveAll(this.OriginalsWithPrefix(StreamsConfig.ProducerPrefixConfig, stripPrefix: false).Keys)
                .RemoveAll(this.OriginalsWithPrefix(StreamsConfig.AdminClientPrefixConfig, stripPrefix: false).Keys);
        }

        /**
         * Return an {@link Serde#configure(Dictionary, bool) configured} instance of {@link #DefaultKeySerdeClassConfig key Serde }
         *
         * @return an configured instance of key Serde
         */
        public ISerde GetDefaultKeySerde(IServiceProvider services)
        {
            object keySerdeConfigSetting = this.Get(StreamsConfig.DefaultKeySerdeClassConfig);

            try
            {
                if (this.TryGetConfiguredInstance(typeof(ISerde<>), services, StreamsConfig.DefaultKeySerdeClassConfig, out var serde))
                {
                    (serde as ISerde).Configure(this.properties, true);
                }

                return serde as ISerde;
            }
            catch (Exception e)
            {
                throw new StreamsException($"Failed to configure key serde {keySerdeConfigSetting}", e);
            }
        }

        /**
         * Return an {@link Serde#configure(Dictionary, bool) configured} instance of {@link #DefaultValueSerdeClassConfig value
         * Serde}.
         *
         * @return an configured instance of value Serde
         */
        public ISerde GetDefaultValueSerde(IServiceProvider services)
        {
            object valueSerdeConfigSetting = this.Get(StreamsConfig.DefaultValueSerdeClassConfig);

            try
            {
                if (this.TryGetConfiguredInstance<ISerde>(services, StreamsConfig.DefaultValueSerdeClassConfig, out var serde))
                {
                    serde.Configure(this.properties, true);
                }

                return serde;
            }
            catch (Exception e)
            {
                throw new StreamsException($"Failed to configure value serde {valueSerdeConfigSetting}", e);
            }
        }

        public ITimestampExtractor GetDefaultTimestampExtractor(IServiceProvider services)
        {
            this.TryGetConfiguredInstance<ITimestampExtractor>(
                services,
                StreamsConfig.DefaultTimestampExtractorClassConfig,
                out var timestampExtractor);

            return timestampExtractor;
        }

        public IDeserializationExceptionHandler GetDefaultDeserializationExceptionHandler(IServiceProvider services)
        {
            this.TryGetConfiguredInstance<IDeserializationExceptionHandler>(
                services,
                StreamsConfig.DefaultDeserializationExceptionHandlerClassConfig,
                out var handler);

            return handler;
        }

        public bool TryGetConfiguredInstance<T>(IServiceProvider services, string key, out T instance)
        {
            var result = this.TryGetConfiguredInstance(typeof(T), services, key, out var innerInstance);

            instance = (T)innerInstance;

            return result;
        }

        public bool TryGetConfiguredInstance(Type instanceType, IServiceProvider services, string key, out object instance)
        {
            instance = default;

            if (services is null || string.IsNullOrEmpty(key))
            {
                return false;
            }

            string value = this.Get(key) ?? "";

            if (string.IsNullOrWhiteSpace(value))
            {
                return false;
            }

            try
            {
                Type type = Type.GetType(value);

                if (type == null)
                {

                }

                instance = ActivatorUtilities.GetServiceOrCreateInstance(services, type);
            }
            catch (TypeAccessException)
            {
                return false;
            }

            return true;
        }

        public IProductionExceptionHandler GetDefaultProductionExceptionHandler(IServiceProvider services)
        {
            this.TryGetConfiguredInstance<IProductionExceptionHandler>(
                services,
                StreamsConfig.DefaultProductionExceptionHandlerClassConfig,
                out var handler);

            return handler;
        }

        public void SetAll(StreamsConfig additional)
        {
            if (additional is null)
            {
                throw new ArgumentNullException(nameof(additional));
            }

            this.SetAll(additional.properties);
        }
    }
}
