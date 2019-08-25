using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.Errors.Interfaces;
using Kafka.Streams.Interfaces;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams
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
     *     max.poll.interval.ms &gt; max.block.ms
     * </pre>
     *
     *
     * Kafka Streams requires at least the following properties to be set:
     * <ul>
     *  <li>{@link #APPLICATION_ID_CONFIG "application.id"}</li>
     *  <li>{@link #BOOTSTRAP_SERVERS_CONFIG "bootstrap.servers"}</li>
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
    public class StreamsConfig : ConsumerConfig
    {
        private static ILogger log = new LoggerFactory().CreateLogger<StreamsConfig>();

        /// <summary>
        /// Initialize a new empty <see cref="StreamsConfig" /> instance.
        /// </summary>
        public StreamsConfig() { }

        /// <summary>
        /// Initialize a new <see cref="StreamsConfig" /> instance based on
        /// an existing <see cref="ClientConfig" /> instance.
        /// </summary>
        public StreamsConfig(ClientConfig config)
        {
            this.properties = new Dictionary<string, string>(config.ToDictionary(a => a.Key, a => a.Value));
        }

        /// <summary>
        /// Initialize a new <see cref="StreamsConfig" /> instance based on
        /// an existing key/value pair collection.
        /// </summary>
        public StreamsConfig(IEnumerable<KeyValuePair<string, string>> config)
        {
            this.properties = new Dictionary<string, string>(config.ToDictionary(a => a.Key, a => a.Value));
        }

        public string ApplicationId
        {
            get => Get(StreamsConfigPropertyNames.ApplicationId);
            set => this.SetObject(StreamsConfigPropertyNames.ApplicationId, value);
        }

        public new string ClientId
        {
            get => Get(StreamsConfigPropertyNames.ClientId);
            set => this.SetObject(StreamsConfigPropertyNames.ClientId, value);
        }

        public Type DefaultKeySerde
        {
            get => Type.GetType(Get(StreamsConfigPropertyNames.DefaultKeySerdeClass));
            set => this.SetObject(StreamsConfigPropertyNames.DefaultKeySerdeClass, value);
        }

        public Type DefaultValueSerde
        {
            get => Type.GetType(Get(StreamsConfigPropertyNames.DefaultValueSerdeClass));
            set => this.SetObject(StreamsConfigPropertyNames.DefaultValueSerdeClass, value);
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
        public StreamsConfig(Dictionary<string, object> props)
            : this(props, true)
        {
        }

        protected StreamsConfig(Dictionary<string, object> props, bool doLog)
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

        private Dictionary<string, object> GetCommonConsumerConfigs()
        {
            //Dictionary<string, object> clientProvidedProps = getClientPropsWithPrefix(CONSUMER_PREFIX, ConsumerConfig.configNames());

            //checkIfUnexpectedUserSpecifiedConsumerConfig(clientProvidedProps, NON_CONFIGURABLE_CONSUMER_DEFAULT_CONFIGS);
            //checkIfUnexpectedUserSpecifiedConsumerConfig(clientProvidedProps, NON_CONFIGURABLE_CONSUMER_EOS_CONFIGS);

            Dictionary<string, object> consumerProps = new Dictionary<string, object>(/*eosEnabled ? CONSUMER_EOS_OVERRIDES : CONSUMER_DEFAULT_OVERRIDES*/);
            //consumerProps.putAll(getClientCustomProps());
            //consumerProps.putAll(clientProvidedProps);

            //// bootstrap.servers should be from StreamsConfig
            //consumerProps.Add(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, originals()[BOOTSTRAP_SERVERS_CONFIG];

            return consumerProps;
        }

        private void CheckIfUnexpectedUserSpecifiedConsumerConfig(Dictionary<string, object> clientProvidedProps, string[] nonConfigurableConfigs)
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
         * except in the case of {@link ConsumerConfig#BOOTSTRAP_SERVERS_CONFIG} where we always use the non-prefixed
         * version as we only support reading/writing from/to the same Kafka Cluster.
         * If not specified by {@link #MAIN_CONSUMER_PREFIX}, main consumer will share the general consumer configs
         * prefixed by {@link #CONSUMER_PREFIX}.
         *
         * @param groupId      consumer groupId
         * @param clientId     clientId
         * @param threadIdx    stream thread index
         * @return Map of the consumer configuration.
         */
        public Dictionary<string, object> GetMainConsumerConfigs(string groupId, string clientId, int threadIdx)
        {
            Dictionary<string, object> consumerProps = GetCommonConsumerConfigs();

            //// Get main consumer override configs
            //Dictionary<string, object> mainConsumerProps = originalsWithPrefix(MAIN_CONSUMER_PREFIX);
            //foreach (KeyValuePair<string, object> entry in mainConsumerProps)
            //{
            //    consumerProps.Add(entry.Key, entry.Value);
            //}

            //// this is a hack to work around StreamsConfig constructor inside StreamsPartitionAssignor to avoid casting
            //consumerProps.Add(APPLICATION_ID_CONFIG, groupId);

            ////.Add group id, client id with stream client id prefix, and group instance id
            //consumerProps.Add(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            //consumerProps.Add(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
            //string groupInstanceId = (string)consumerProps[ConsumerConfig.GROUP_INSTANCE_ID_CONFIG];
            //// Suffix each thread consumer with thread.id to enforce uniqueness of group.instance.id.
            //if (groupInstanceId != null)
            //{
            //    consumerProps.Add(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId + "-" + threadIdx);
            //}

            ////.Add configs required for stream partition assignor
            //consumerProps.Add(UPGRADE_FROM_CONFIG, getString(UPGRADE_FROM_CONFIG));
            //consumerProps.Add(REPLICATION_FACTOR_CONFIG, GetInt(REPLICATION_FACTOR_CONFIG));
            //consumerProps.Add(APPLICATION_SERVER_CONFIG, getString(APPLICATION_SERVER_CONFIG));
            //consumerProps.Add(NUM_STANDBY_REPLICAS_CONFIG, GetInt(NUM_STANDBY_REPLICAS_CONFIG));
            //consumerProps.Add(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StreamsPartitionAssignor.getName());
            //consumerProps.Add(WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, getLong(WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG));

            ////.Add admin retries configs for creating topics
            //AdminClientConfig adminClientDefaultConfig = new AdminClientConfig(getClientPropsWithPrefix(ADMIN_CLIENT_PREFIX, AdminClientConfig.configNames()));
            //consumerProps.Add(adminClientPrefix(AdminClientConfig.RETRIES_CONFIG), adminClientDefaultConfig.getInt(AdminClientConfig.RETRIES_CONFIG));
            //consumerProps.Add(adminClientPrefix(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG), adminClientDefaultConfig.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG));

            //// verify that producer batch config is no larger than segment size, then.Add topic configs required for creating topics
            //Dictionary<string, object> topicProps = originalsWithPrefix(TOPIC_PREFIX, false);
            //Dictionary<string, object> producerProps = getClientPropsWithPrefix(PRODUCER_PREFIX, ProducerConfig.configNames());

            //if (topicProps.ContainsKey(topicPrefix(TopicConfig.SEGMENT_BYTES_CONFIG)) &&
            //    producerProps.ContainsKey(ProducerConfig.BATCH_SIZE_CONFIG))
            //{
            //    int segmentSize = int.Parse(topicProps[topicPrefix(TopicConfig.SEGMENT)_CONFIG)].ToString();
            //    int batchSize = int.Parse(producerProps[ProducerConfig.BATCH_SIZE_CONFIG].ToString());

            //    if (segmentSize < batchSize)
            //    {
            //        throw new System.ArgumentException(string.Format("Specified topic segment size %d is is smaller than the configured producer batch size %d, this will cause produced batch not able to be appended to the topic",
            //                segmentSize,
            //                batchSize));
            //    }
            //}

            //consumerProps.putAll(topicProps);

            return consumerProps;
        }

        public new int? GetInt(string key)
            => base.GetInt(key);

        /**
         * Get the configs for the {@link KafkaConsumer restore-consumer}.
         * Properties using the prefix {@link #RESTORE_CONSUMER_PREFIX} will be used in favor over
         * the properties prefixed with {@link #CONSUMER_PREFIX} and the non-prefixed versions
         * (read the override precedence ordering in {@link #RESTORE_CONSUMER_PREFIX}
         * except in the case of {@link ConsumerConfig#BOOTSTRAP_SERVERS_CONFIG} where we always use the non-prefixed
         * version as we only support reading/writing from/to the same Kafka Cluster.
         * If not specified by {@link #RESTORE_CONSUMER_PREFIX}, restore consumer will share the general consumer configs
         * prefixed by {@link #CONSUMER_PREFIX}.
         *
         * @param clientId clientId
         * @return Map of the restore consumer configuration.
         */
        public Dictionary<string, object> GetRestoreConsumerConfigs(string clientId)
        {
            Dictionary<string, object> baseConsumerProps = GetCommonConsumerConfigs();

            //// Get restore consumer override configs
            //Dictionary<string, object> restoreConsumerProps = originalsWithPrefix(RESTORE_CONSUMER_PREFIX);
            //foreach (KeyValuePair<string, object> entry in restoreConsumerProps)
            //{
            //    baseConsumerProps.Add(entry.Key, entry.Value);
            //}

            //// no need to set group id for a restore consumer
            //baseConsumerProps.Remove(ConsumerConfig.GROUP_ID_CONFIG);
            ////.Add client id with stream client id prefix
            //baseConsumerProps.Add(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
            //baseConsumerProps.Add(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

            return baseConsumerProps;
        }

        /**
         * Get the configs for the {@link KafkaConsumer global consumer}.
         * Properties using the prefix {@link #GLOBAL_CONSUMER_PREFIX} will be used in favor over
         * the properties prefixed with {@link #CONSUMER_PREFIX} and the non-prefixed versions
         * (read the override precedence ordering in {@link #GLOBAL_CONSUMER_PREFIX}
         * except in the case of {@link ConsumerConfig#BOOTSTRAP_SERVERS_CONFIG} where we always use the non-prefixed
         * version as we only support reading/writing from/to the same Kafka Cluster.
         * If not specified by {@link #GLOBAL_CONSUMER_PREFIX}, global consumer will share the general consumer configs
         * prefixed by {@link #CONSUMER_PREFIX}.
         *
         * @param clientId clientId
         * @return Map of the global consumer configuration.
         */
        public Dictionary<string, object> GetGlobalConsumerConfigs(string clientId)
        {
            Dictionary<string, object> baseConsumerProps = GetCommonConsumerConfigs();

            //// Get global consumer override configs
            //Dictionary<string, object> globalConsumerProps = originalsWithPrefix(GLOBAL_CONSUMER_PREFIX);
            //foreach (KeyValuePair<string, object> entry in globalConsumerProps)
            //{
            //    baseConsumerProps.Add(entry.Key, entry.Value);
            //}

            //// no need to set group id for a global consumer
            //baseConsumerProps.Remove(ConsumerConfig.GROUP_ID_CONFIG);
            ////.Add client id with stream client id prefix
            //baseConsumerProps.Add(CommonClientConfigs.CLIENT_ID_CONFIG, clientId + "-global-consumer");
            //baseConsumerProps.Add(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

            return baseConsumerProps;
        }

        /**
         * Get the configs for the {@link KafkaProducer producer}.
         * Properties using the prefix {@link #PRODUCER_PREFIX} will be used in favor over their non-prefixed versions
         * except in the case of {@link ProducerConfig#BOOTSTRAP_SERVERS_CONFIG} where we always use the non-prefixed
         * version as we only support reading/writing from/to the same Kafka Cluster.
         *
         * @param clientId clientId
         * @return Map of the producer configuration.
         */
        public Dictionary<string, object> getProducerConfigs(string clientId)
        {
            Dictionary<string, object> clientProvidedProps = null; // getClientPropsWithPrefix(PRODUCER_PREFIX, ProducerConfig.configNames());

            //            checkIfUnexpectedUserSpecifiedConsumerConfig(clientProvidedProps, NON_CONFIGURABLE_PRODUCER_EOS_CONFIGS);

            // generate producer configs from original properties and overridden maps
            Dictionary<string, object> props = new Dictionary<string, object>(/*eosEnabled ? PRODUCER_EOS_OVERRIDES : PRODUCER_DEFAULT_OVERRIDES*/);
            //props.putAll(getClientCustomProps());
            //props.putAll(clientProvidedProps);

            //props.Add(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, originals()[BOOTSTRAP_SERVERS_CONFIG]);
            ////.Add client id with stream client id prefix
            //props.Add(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);

            return props;
        }

        /**
         * Get the configs for the {@link Admin admin client}.
         * @param clientId clientId
         * @return Map of the admin client configuration.
         */
        public Dictionary<string, object> getAdminConfigs(string clientId)
        {
            //Dictionary<string, object> clientProvidedProps = getClientPropsWithPrefix(StreamConfigPropertyNames.AdminClientPrefix, new AdminClientConfig());

            Dictionary<string, object> props = new Dictionary<string, object>();
            //props.putAll(getClientCustomProps());
            //props.putAll(clientProvidedProps);

            // add client id with stream client id prefix
            //props.Add(CLIENT_ID_CONFIG, clientId);

            return props;
        }

        private Dictionary<string, object> getClientPropsWithPrefix(string prefix)
        {
            //Dictionary<string, object> props = clientProps(configNames, originals());
            //props.putAll(originalsWithPrefix(prefix));
            //return props;

            return new Dictionary<string, object>();
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
         * Get a map of custom configs by removing from the originals all the Streams, Consumer, Producer, and AdminClient configs.
         * Prefixed properties are also removed because they are already.Added by {@link #getClientPropsWithPrefix(string, Set)}.
         * This allows to set a custom property for a specific client alone if specified using a prefix, or for all
         * when no prefix is used.
         *
         * @return a map with the custom properties
         */
        private Dictionary<string, object> getClientCustomProps()
        {
            //Dictionary<string, object> props = originals();
            //props.Keys.removeAll(CONFIG.names());
            //props.Keys.removeAll(ConsumerConfig.configNames());
            //props.Keys.removeAll(ProducerConfig.configNames());
            //props.Keys.removeAll(AdminClientConfig.configNames());
            //props.Keys.removeAll(originalsWithPrefix(CONSUMER_PREFIX, false).Keys);
            //props.Keys.removeAll(originalsWithPrefix(PRODUCER_PREFIX, false).Keys);
            //props.Keys.removeAll(originalsWithPrefix(ADMIN_CLIENT_PREFIX, false).Keys);

            //return props;

            return new Dictionary<string, object>();
        }

        /**
         * Return an {@link Serde#configure(Map, bool) configured} instance of {@link #DEFAULT_KEY_SERDE_CLASS_CONFIG key Serde
         *}.
         *
         * @return an configured instance of key Serde
         */

        public ISerde<object> defaultKeySerde()
        {
            object keySerdeConfigSetting = Get(StreamsConfigPropertyNames.DefaultKeySerdeClass);

            try
            {
                ISerde<object> serde = null; // getConfiguredInstance(DEFAULT_KEY_SERDE_CLASS_CONFIG);
                                             //serde.Configure(originals(), true);

                return serde;
            }
            catch (Exception e)
            {
                throw new StreamsException(
                    string.Format("Failed to configure key serde %s", keySerdeConfigSetting), e);
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
            object valueSerdeConfigSetting = Get(
                StreamsConfigPropertyNames.DefaultValueSerdeClass);

            try
            {
                ISerde<object> serde = null; // getConfiguredInstance(StreamConfigPropertyNames.DEFAULT_VALUE_SERDE_CLASS_CONFIG/*, Serde*/);
                                             //serde.Configure(originals(), false);

                return serde;
            }
            catch (Exception e)
            {
                throw new StreamsException(
                    string.Format("Failed to configure value serde %s", valueSerdeConfigSetting), e);
            }
        }

        public ITimestampExtractor defaultTimestampExtractor()
        {
            return getConfiguredInstance<ITimestampExtractor>(
                StreamsConfigPropertyNames.DefaultTimestampExtractorClass);
        }

        public IDeserializationExceptionHandler defaultDeserializationExceptionHandler()
        {
            return getConfiguredInstance<IDeserializationExceptionHandler>(
                StreamsConfigPropertyNames.DefaultDeserializationExceptionHandlerClass);
        }

        public T getConfiguredInstance<T>(string key)
        {

            return default;
        }

        public IProductionExceptionHandler defaultProductionExceptionHandler()
        {
            return getConfiguredInstance<IProductionExceptionHandler>(
                StreamsConfigPropertyNames.DefaultProductionExceptionHandlerClass);
        }

        /**
         * Override any client properties in the original configs with overrides
         *
         * @param configNames The given set of configuration names.
         * @param originals   The original configs to be filtered.
         * @return client config with any overrides
         */
        private Dictionary<string, object> clientProps(
            HashSet<string> configNames,
            Dictionary<string, object> originals)
        {
            // iterate all client config names, filter out non-client configs from the original
            // property map and use the overridden values when they are not specified by users
            var parsed = new Dictionary<string, object>();
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