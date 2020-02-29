using Confluent.Kafka;
using Kafka.Common.Extensions;
using Kafka.Streams.Errors;
using Kafka.Streams.Errors.Interfaces;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors.Internals;
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
     * The consumer/producer/admin prefix can also be used to distinguish these custom config values passed to different clients with the same config name.
     * * Example:
     * <pre>{@code
     * Properties streamsProperties = new Properties();
     * // sets "my.custom.config" to "foo" for consumer only
     * streamsProperties.Add(StreamsConfig.consumerPrefix("my.custom.config"), "foo");
     * // sets "my.custom.config" to "bar" for producer only
     * streamsProperties.Add(StreamsConfig.producerPrefix("my.custom.config"), "bar");
     * // sets "my.custom.config2" to "boom" for all clients universally
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
     *  <li>{@link #APPLICATION_ID_CONFIG "application.id"}</li>
     *  <li>{@link #BootstrapServers "bootstrap.servers"}</li>
     * </ul>
     *
     * By default, Kafka Streams does not allow users to overwrite the following properties (Streams setting shown in parentheses):
     * <ul>
     *   <li>{@link ConsumerConfig#ENABLE_AUTO_COMMIT_CONFIG "enable.auto.commit"} (false) - Streams client will always disable/turn off auto committing</li>
     * </ul>
     *
     * If {@link #PROCESSING_GUARANTEE_CONFIG "processing.guarantee"} is set to {@link #EXACTLY_ONCE "exactly_once"}, Kafka Streams does not allow users to overwrite the following properties (Streams setting shown in parentheses):
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
    public class StreamsConfig : Config
    {
        private static readonly string[] NON_CONFIGURABLE_CONSUMER_DEFAULT_CONFIGS = new string[] { StreamsConfigPropertyNames.ENABLE_AUTO_COMMIT_CONFIG };
        private static readonly string[] NON_CONFIGURABLE_CONSUMER_EOS_CONFIGS = new string[] { StreamsConfigPropertyNames.ISOLATION_LEVEL_CONFIG };
        private static readonly string[] NON_CONFIGURABLE_PRODUCER_EOS_CONFIGS = new string[] { StreamsConfigPropertyNames.ENABLE_IDEMPOTENCE_CONFIG, StreamsConfigPropertyNames.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION };

        /// <summary>
        /// Initialize a new <see cref="StreamsConfig" /> instance.
        /// </summary>
        public StreamsConfig()
            : base()
        {
        }

        /// <summary>
        /// Initialize a new <see cref="StreamsConfig" /> instance based on
        /// an existing <see cref="Config" /> instance.
        /// </summary>
        public StreamsConfig(Config config)
            : base(config)
        {
            this.SetAll(config);
            this.SetDefaultConfiguration();
        }

        public string ApplicationId
        {
            get => this.Get(StreamsConfigPropertyNames.ApplicationId);
            set => this.SetObject(StreamsConfigPropertyNames.ApplicationId, value);
        }

        public string PartitionAssignmentStrategy
        {
            get => this.Get(StreamsConfigPropertyNames.PARTITION_ASSIGNMENT_STRATEGY_CONFIG);
            set => this.SetObject(StreamsConfigPropertyNames.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, value);
        }

        public long StateCleanupDelayMs
        {
            get => this.getLong(StreamsConfigPropertyNames.StateCleanupDelayMs) ?? 10 * 60 * 1000L;
            set => this.SetObject(StreamsConfigPropertyNames.StateCleanupDelayMs, value);
        }

        public string ApplicationServer
        {
            get => this.Get(StreamsConfigPropertyNames.ApplicationServer) ?? "";
            set => this.SetObject(StreamsConfigPropertyNames.ApplicationServer, value);
        }

        public string BootstrapServers
        {
            get => this.Get(StreamsConfigPropertyNames.BootstrapServers);
            set => this.Set(StreamsConfigPropertyNames.BootstrapServers, value);
        }

        public string ClientId
        {
            get => this.Get(StreamsConfigPropertyNames.ClientId);
            set => this.SetObject(StreamsConfigPropertyNames.ClientId, value);
        }

        public Type DefaultKeySerde
        {
            get => Type.GetType(this.Get(StreamsConfigPropertyNames.DefaultKeySerdeClass));
            set => this.SetObject(StreamsConfigPropertyNames.DefaultKeySerdeClass, value);
        }

        public Type DefaultValueSerde
        {
            get => Type.GetType(this.Get(StreamsConfigPropertyNames.DefaultValueSerdeClass));
            set => this.SetObject(StreamsConfigPropertyNames.DefaultValueSerdeClass, value);
        }

        public int NumberOfStreamThreads
        {
            get => this.GetInt(StreamsConfigPropertyNames.NumberOfStreamThreads) ?? 1;
            set => this.Set(StreamsConfigPropertyNames.NumberOfStreamThreads, value.ToString());
        }

        public long CacheMaxBytesBuffering
        {
            get => this.getLong(StreamsConfigPropertyNames.CacheMaxBytesBuffering) ?? 10485760L;
            set => this.Set(StreamsConfigPropertyNames.CacheMaxBytesBuffering, value.ToString());
        }

        public bool EnableIdempotence
        {
            get => this.GetBool("enable.idempotence") ?? false;
            set => this.SetObject("enable.idempotence", value);
        }

        public int Retries
        {
            get => this.GetInt(StreamsConfigPropertyNames.Retries) ?? int.MaxValue;
            set => this.SetObject(StreamsConfigPropertyNames.Retries, value);
        }

        public long RetryBackoffMs
        {
            get => this.GetInt(StreamsConfigPropertyNames.RetryBackoffMs) ?? 100L;
            set => this.SetObject(StreamsConfigPropertyNames.RetryBackoffMs, value);
        }

        public string StateStoreDirectory
        {
            get => this.getString(StreamsConfigPropertyNames.STATE_DIR_CONFIG) ?? Path.Combine(Path.GetTempPath(), "kafka-streams");
            set => this.Set(StreamsConfigPropertyNames.STATE_DIR_CONFIG, value);
        }

        /// <summary>
        ///     Automatically and periodically commit offsets in the background. Note: setting this to false does not prevent the consumer from fetching previously committed start offsets. To circumvent this behaviour set specific start offsets per partition in the call to assign().
        ///
        ///     default: true
        ///     importance: high
        /// </summary>
        public bool? EnableAutoCommit
        {
            get => this.GetBool(StreamsConfigPropertyNames.ENABLE_AUTO_COMMIT_CONFIG);
            set => this.SetObject(StreamsConfigPropertyNames.ENABLE_AUTO_COMMIT_CONFIG, value);
        }

        /// <summary>
        /// The frequency with which to save the position of the processor.
        /// (Note, if processing.guarantee is set to ExactlyOnce, the default value is
        /// EOS_DEFAULT_COMMIT_INTERVAL_MS otherwise the default value is DEFAULT_COMMIT_INTERVAL_MS.
        /// </summary>
        public long CommitIntervalMs
        {
            get => this.getLong(StreamsConfigPropertyNames.COMMIT_INTERVAL_MS_CONFIG) ?? 30000L;
            set => this.Set(StreamsConfigPropertyNames.COMMIT_INTERVAL_MS_CONFIG, value.ToString());
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
            get => this.getLong(StreamsConfigPropertyNames.PollMs) ?? 100L;
            set => this.Set(StreamsConfigPropertyNames.PollMs, value.ToString());
        }

        private void SetDefaultConfiguration()
        {
            this.EnableAutoCommit = false;

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
            return StreamsConfigPropertyNames.ConsumerPrefix + consumerProp;
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
            return StreamsConfigPropertyNames.MainConsumerPrefix + consumerProp;
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
            return StreamsConfigPropertyNames.RestoreConsumerPrefix + consumerProp;
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
            return StreamsConfigPropertyNames.GlobalConsumerPrefix + consumerProp;
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
            return StreamsConfigPropertyNames.ProducerPrefix + producerProp;
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
            return StreamsConfigPropertyNames.AdminClientPrefix + adminClientProp;
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
            return StreamsConfigPropertyNames.TopicPrefix + topicProp;
        }

        /**
         * Create a new {@code StreamsConfig} using the given properties.
         *
         * @param props properties that specify Kafka Streams and internal consumer/producer configuration
         */
        public StreamsConfig(IDictionary<string, string> config)
            : base(config)
        {
            //   eosEnabled = EXACTLY_ONCE.Equals(getString(PROCESSING_GUARANTEE_CONFIG));
        }

        protected Dictionary<string, object> PostProcessParsedConfig(Dictionary<string, object> parsedValues)
        {
            Dictionary<string, object> configUpdates = new Dictionary<string, object>();
            //   CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);

            //bool eosEnabled = EXACTLY_ONCE.Equals(parsedValues[PROCESSING_GUARANTEE_CONFIG]);
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
            var clientProvidedProps = GetClientPropsWithPrefix(StreamsConfigPropertyNames.ConsumerPrefix, this);

            CheckIfUnexpectedUserSpecifiedConsumerConfig(clientProvidedProps, NON_CONFIGURABLE_CONSUMER_DEFAULT_CONFIGS);
            CheckIfUnexpectedUserSpecifiedConsumerConfig(clientProvidedProps, NON_CONFIGURABLE_CONSUMER_EOS_CONFIGS);

            var consumerProps = new Dictionary<string, string>(/*eosEnabled ? CONSUMER_EOS_OVERRIDES : CONSUMER_DEFAULT_OVERRIDES*/);
            consumerProps.PutAll(GetClientCustomProps());
            consumerProps.PutAll(clientProvidedProps);

            // bootstrap.servers should be from StreamsConfig
            if (consumerProps.ContainsKey(StreamsConfigPropertyNames.BootstrapServers))
            {
                consumerProps[StreamsConfigPropertyNames.BootstrapServers] = this.BootstrapServers;
            }
            else
            {
                consumerProps.Add(StreamsConfigPropertyNames.BootstrapServers, this.properties[StreamsConfigPropertyNames.BootstrapServers]);
            }

            return new ConsumerConfig(consumerProps);
        }

        private void CheckIfUnexpectedUserSpecifiedConsumerConfig(
            Dictionary<string, string> clientProvidedProps,
            string[] nonConfigurableConfigs)
        {
            // Streams does not allow users to configure certain consumer/producer configurations, for example,
            // enable.auto.commit. In cases where user tries to override such non-configurable
            // consumer/producer configurations, log a warning and Remove the user defined value from the Map.
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
            //        string eosMessage = PROCESSING_GUARANTEE_CONFIG + " is set to " + EXACTLY_ONCE + ". Hence, ";
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
         * @return Map of the consumer configuration.
         */
        public ConsumerConfig GetMainConsumerConfigs(string groupId, string clientId, int threadIdx)
        {
            var consumerProps = GetCommonConsumerConfigs();

            // Get main consumer override configs
            var mainConsumerProps = originalsWithPrefix(StreamsConfigPropertyNames.MainConsumerPrefix);
            consumerProps.SetAll(mainConsumerProps);

            // this is a hack to work around StreamsConfig constructor inside StreamsPartitionAssignor to avoid casting
            // consumerProps.Set(StreamsConfigPropertyNames.ApplicationId, groupId);

            // add group id, client id with stream client id prefix, and group instance id
            consumerProps.GroupId = groupId;
            consumerProps.ClientId = clientId;

            string groupInstanceId = consumerProps.GroupId;

            // Suffix each thread consumer with thread.id to enforce uniqueness of group.instance.id.
            if (groupInstanceId != null)
            {
                consumerProps.GroupId = $"{groupInstanceId}-{threadIdx}";
            }

            // add configs required for stream partition assignor
            // consumerProps.Add(UPGRADE_FROM_CONFIG, getString(UPGRADE_FROM_CONFIG));
            // consumerProps.Add(StreamsConfigPropertyNames.REPLICATION_FACTOR_CONFIG, GetInt(StreamsConfigPropertyNames.REPLICATION_FACTOR_CONFIG));
            consumerProps.Set(StreamsConfigPropertyNames.ApplicationServer, getString(StreamsConfigPropertyNames.ApplicationServer));
            consumerProps.Set(StreamsConfigPropertyNames.NUM_STANDBY_REPLICAS_CONFIG, getString(StreamsConfigPropertyNames.NUM_STANDBY_REPLICAS_CONFIG));
            consumerProps.Set(StreamsConfigPropertyNames.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, getString(typeof(StreamsPartitionAssignor).FullName));
            consumerProps.Set(StreamsConfigPropertyNames.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, getString(StreamsConfigPropertyNames.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG));

            // add admin retries configs for creating topics
            AdminClientConfig adminClientDefaultConfig = new AdminClientConfig(GetClientPropsWithPrefix(StreamsConfigPropertyNames.AdminClientPrefix, new AdminClientConfig()));
            // consumerProps.Set(adminClientPrefix(AdminClientConfig.RETRIES_CONFIG), adminClientDefaultConfig.getInt(AdminClientConfig.RETRIES_CONFIG));
            // consumerProps.Add(adminClientPrefix(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG), adminClientDefaultConfig.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG));

            // verify that producer batch config is no larger than segment size, then.Add topic configs required for creating topics
            Dictionary<string, string> topicProps = originalsWithPrefix(StreamsConfigPropertyNames.TopicPrefix, strip: false);
            Dictionary<string, string> producerProps = GetClientPropsWithPrefix(StreamsConfigPropertyNames.ProducerPrefix, new ProducerConfig());

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

        public string? getString(string key)
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
         * @return Map of the restore consumer configuration.
         */
        public ConsumerConfig GetRestoreConsumerConfigs(string clientId)
        {
            var baseConsumerProps = GetCommonConsumerConfigs();

            // Get restore consumer override configs
            var restoreConsumerProps = originalsWithPrefix(StreamsConfigPropertyNames.RestoreConsumerPrefix);
            baseConsumerProps.SetAll(restoreConsumerProps);

            // no need to set group id for a restore consumer
            // C# library throws if GroupId isn't set...
            // if (!string.IsNullOrWhiteSpace(this.GroupId) && baseConsumerProps.ContainsKey(this.GroupId))
            // {
            //     baseConsumerProps.Remove(this.GroupId);
            // }

            // add client id with stream client id prefix
            baseConsumerProps.ClientId = clientId;
            baseConsumerProps.AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest;
            baseConsumerProps.GroupId = clientId;

            return baseConsumerProps;
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
         * @return Map of the global consumer configuration.
         */
        public ConsumerConfig GetGlobalConsumerConfigs(string clientId)
        {
            var baseConsumerProps = GetCommonConsumerConfigs();

            // Get global consumer override configs
            var globalConsumerProps = originalsWithPrefix(StreamsConfigPropertyNames.GlobalConsumerPrefix);

            baseConsumerProps.SetAll(globalConsumerProps);

            // no need to set group id for a global consumer
            // baseConsumerProps.Remove(StreamsConfigPropertyNames.GroupId);

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
         * @return Map of the producer configuration.
         */
        public ProducerConfig GetProducerConfigs(string clientId)
        {
            var producerConfig = new ProducerConfig();
            var clientProvidedProps = GetClientPropsWithPrefix(StreamsConfigPropertyNames.ProducerPrefix, new ProducerConfig());

            CheckIfUnexpectedUserSpecifiedConsumerConfig(clientProvidedProps, NON_CONFIGURABLE_PRODUCER_EOS_CONFIGS);

            // generate producer configs from original properties and overridden maps
            producerConfig.SetAll(GetClientCustomProps());
            producerConfig.SetAll(clientProvidedProps);

            producerConfig.BootstrapServers = (string)this.properties[StreamsConfigPropertyNames.BootstrapServers];

            // add client id with stream client id prefix
            producerConfig.ClientId = clientId;

            return producerConfig;
        }

        /**
         * Get the configs for the {@link Admin admin client}.
         * @param clientId clientId
         * @return Map of the admin client configuration.
         */
        public AdminClientConfig GetAdminConfigs(string clientId)
        {
            var adminConfig = new AdminClientConfig(new Dictionary<string, string>());

            var clientProvidedProps = GetClientPropsWithPrefix(StreamsConfigPropertyNames.AdminClientPrefix, this.properties);

            adminConfig.SetAll(GetClientCustomProps());
            adminConfig.SetAll(clientProvidedProps);

            // add client id with stream client id prefix
            adminConfig.ClientId = clientId;

            return adminConfig;
        }

        private Dictionary<string, string> GetClientPropsWithPrefix(string prefix, IEnumerable<KeyValuePair<string, string>> configKeyValuePairs)
            => GetClientPropsWithPrefix(prefix, new HashSet<string>(configKeyValuePairs.Select(kvp => kvp.Key)));

        private Dictionary<string, string> GetClientPropsWithPrefix(string prefix, HashSet<string> configNames)
        {
            var props = new Dictionary<string, string>();

            foreach (var original in originalsWithPrefix(prefix))
            {
                props.Add(original.Key, original.Value);
            }

            return props;
        }

        public long? getLong(string key)
        {
            var keyAsLong = this.Get(key);

            if (long.TryParse(keyAsLong, out var value))
            {
                return value;
            }

            return null;
        }

        /**
         * Gets all original settings with the given prefix, stripping the prefix before adding it to the output.
         *
         * @param prefix the prefix to use as a filter
         * @return a Map containing the settings with the prefix
         */
        private Dictionary<string, string> originalsWithPrefix(string prefix)
        {
            return originalsWithPrefix(prefix, strip: true);
        }

        /**
         * Gets all original settings with the given prefix.
         *
         * @param prefix the prefix to use as a filter
         * @param strip strip the prefix before adding to the output if set true
         * @return a Map containing the settings with the prefix
         */
        private Dictionary<string, string> originalsWithPrefix(string prefix, bool strip)
        {
            var result = new Dictionary<string, string>();

            foreach (var entry in this.properties)
            {
                if (entry.Key.StartsWith(prefix) && entry.Key.Length > prefix.Length)
                {
                    if (strip)
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
         * Get a map of custom configs by removing from the originals all the Streams, Consumer, Producer, and AdminClient configs.
         * Prefixed properties are also removed because they are already.Added by {@link #getClientPropsWithPrefix(string, Set)}.
         * This allows to set a custom property for a specific client alone if specified using a prefix, or for all
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
                    "state.dir",
                    "application.id",
                    "default.key.serde",
                    "default.value.serde",
                    "num.stream.threads",
                    "cache.max.bytes.buffering",
                })
                .RemoveAll(originalsWithPrefix(StreamsConfigPropertyNames.ConsumerPrefix, false).Keys)
                .RemoveAll(originalsWithPrefix(StreamsConfigPropertyNames.ProducerPrefix, false).Keys)
                .RemoveAll(originalsWithPrefix(StreamsConfigPropertyNames.AdminClientPrefix, false).Keys);
        }

        /**
         * Return an {@link Serde#configure(Map, bool) configured} instance of {@link #DEFAULT_KEY_SERDE_CLASS_CONFIG key Serde }
         *
         * @return an configured instance of key Serde
         */
        public ISerde<object> defaultKeySerde()
        {
            object keySerdeConfigSetting = this.Get(StreamsConfigPropertyNames.DefaultKeySerdeClass);

            try
            {
                ISerde<object> serde = GetConfiguredInstance<ISerde<object>>(StreamsConfigPropertyNames.DefaultKeySerdeClass);
                serde.Configure(this.properties, true);

                return serde;
            }
            catch (Exception e)
            {
                throw new StreamsException($"Failed to configure key serde {keySerdeConfigSetting}", e);
            }
        }

        /**
         * Return an {@link Serde#configure(Map, bool) configured} instance of {@link #DEFAULT_VALUE_SERDE_CLASS_CONFIG value
         * Serde}.
         *
         * @return an configured instance of value Serde
         */
        public ISerde<object> defaultValueSerde()
        {
            object valueSerdeConfigSetting = this.Get(StreamsConfigPropertyNames.DefaultValueSerdeClass);

            try
            {
                ISerde<object> serde = GetConfiguredInstance<ISerde<object>>(StreamsConfigPropertyNames.DefaultValueSerdeClass);
                serde.Configure(this.properties, true);

                return serde;
            }
            catch (Exception e)
            {
                throw new StreamsException($"Failed to configure value serde {valueSerdeConfigSetting}", e);
            }
        }

        public ITimestampExtractor DefaultTimestampExtractor()
        {
            return GetConfiguredInstance<ITimestampExtractor>(
                StreamsConfigPropertyNames.DefaultTimestampExtractorClass);
        }

        public IDeserializationExceptionHandler DefaultDeserializationExceptionHandler()
        {
            return GetConfiguredInstance<IDeserializationExceptionHandler>(
                StreamsConfigPropertyNames.DefaultDeserializationExceptionHandlerClass);
        }

        public T GetConfiguredInstance<T>(string key)
        {
            return default;
        }

        public IProductionExceptionHandler DefaultProductionExceptionHandler()
        {
            return GetConfiguredInstance<IProductionExceptionHandler>(
                StreamsConfigPropertyNames.DefaultProductionExceptionHandlerClass);
        }

        /**
         * Override any client properties in the original configs with overrides
         *
         * @param configNames The given set of configuration names.
         * @param originals   The original configs to be filtered.
         * @return client config with any overrides
         */
        private Dictionary<string, string> ClientProps(
            HashSet<string> configNames,
            IDictionary<string, string> originals)
        {
            // iterate all client config names, filter out non-client configs from the original
            // property map and use the overridden values when they are not specified by users
            var parsed = new Dictionary<string, string>();
            foreach (string configName in configNames)
            {
                if (originals.ContainsKey(configName))
                {
                    parsed.Add(configName, originals[configName]);
                }
            }

            return parsed;
        }
    }
}
