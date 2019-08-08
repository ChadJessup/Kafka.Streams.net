using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.Errors.Interfaces;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

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
     * // users can now read both key-value pairs "my.custom.config" -> "foo"
     * // and "my.custom.config2" -> "boom" from the config map
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
    public class StreamsConfig : ClientConfig //AbstractConfig
    {
        private static ILogger log = new LoggerFactory().CreateLogger<StreamsConfig>();

        //private static ConfigDef CONFIG;

        //private bool eosEnabled;
        //private static long DEFAULT_COMMIT_INTERVAL_MS = 30000L;
        //private static long EOS_DEFAULT_COMMIT_INTERVAL_MS = 100L;

        //public static int DUMMY_THREAD_INDEX = 1;

        /**
         * Prefix used to provide default topic configs to be applied when creating internal topics.
         * These should be valid properties from {@link org.apache.kafka.common.config.TopicConfig TopicConfig}.
         * It is recommended to use {@link #topicPrefix(string)}.
         */
        // TODO: currently we cannot get the full topic configurations and hence cannot allow topic configs without the prefix,
        //       this can be lifted once kafka.log.LogConfig is completely deprecated by org.apache.kafka.common.config.TopicConfig

        public static string TOPIC_PREFIX = "topic.";

        public string getString(string key)
        {
            throw new NotImplementedException();
        }

        public long getLong(string key)
        {
            throw new NotImplementedException();
        }

        /**
         * Prefix used to isolate {@link KafkaConsumer consumer} configs from other client configs.
         * It is recommended to use {@link #consumerPrefix(string)} to.Add this prefix to {@link ConsumerConfig consumer
         * properties}.
         */

        public static string CONSUMER_PREFIX = "consumer.";

        /**
         * Prefix used to override {@link KafkaConsumer consumer} configs for the main consumer client from
         * the general consumer client configs. The override precedence is the following (from highest to lowest precedence):
         * 1. main.consumer.[config-name)
         * 2. consumer.[config-name)
         * 3. [config-name)
         */

        public static string MAIN_CONSUMER_PREFIX = "main.consumer.";

        /**
         * Prefix used to override {@link KafkaConsumer consumer} configs for the restore consumer client from
         * the general consumer client configs. The override precedence is the following (from highest to lowest precedence):
         * 1. restore.consumer.[config-name)
         * 2. consumer.[config-name)
         * 3. [config-name)
         */

        public static string RESTORE_CONSUMER_PREFIX = "restore.consumer.";

        /**
         * Prefix used to override {@link KafkaConsumer consumer} configs for the global consumer client from
         * the general consumer client configs. The override precedence is the following (from highest to lowest precedence):
         * 1. global.consumer.[config-name)
         * 2. consumer.[config-name)
         * 3. [config-name)
         */

        public static string GLOBAL_CONSUMER_PREFIX = "global.consumer.";

        /**
         * Prefix used to isolate {@link KafkaProducer producer} configs from other client configs.
         * It is recommended to use {@link #producerPrefix(string)} to.Add this prefix to {@link ProducerConfig producer
         * properties}.
         */

        public static string PRODUCER_PREFIX = "producer.";

        /**
         * Prefix used to isolate {@link Admin admin} configs from other client configs.
         * It is recommended to use {@link #adminClientPrefix(string)} to.Add this prefix to {@link ProducerConfig producer
         * properties}.
         */

        public static string ADMIN_CLIENT_PREFIX = "admin.";

        /**
         * Config value for parameter (@link #TOPOLOGY_OPTIMIZATION "topology.optimization" for disabling topology optimization
         */
        public static string NO_OPTIMIZATION = "none";

        /**
         * Config value for parameter (@link #TOPOLOGY_OPTIMIZATION "topology.optimization" for enabling topology optimization
         */
        public static string OPTIMIZE = "all";

        /**
         * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.10.0.x}.
         */

        public static string UPGRADE_FROM_0100 = "0.10.0";

        /**
         * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.10.1.x}.
         */

        public static string UPGRADE_FROM_0101 = "0.10.1";

        /**
         * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.10.2.x}.
         */

        public static string UPGRADE_FROM_0102 = "0.10.2";

        /**
         * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.11.0.x}.
         */

        public static string UPGRADE_FROM_0110 = "0.11.0";

        /**
         * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 1.0.x}.
         */

        public static string UPGRADE_FROM_10 = "1.0";

        /**
         * Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 1.1.x}.
         */

        public static string UPGRADE_FROM_11 = "1.1";

        /**
         * Config value for parameter {@link #PROCESSING_GUARANTEE_CONFIG "processing.guarantee"} for at-least-once processing guarantees.
         */

        public static string AT_LEAST_ONCE = "at_least_once";

        /**
         * Config value for parameter {@link #PROCESSING_GUARANTEE_CONFIG "processing.guarantee"} for exactly-once processing guarantees.
         */

        public static string EXACTLY_ONCE = "exactly_once";

        /** {@code application.id} */

        public static string APPLICATION_ID_CONFIG = "application.id";
        private static string APPLICATION_ID_DOC = "An identifier for the stream processing application. Must be unique within the Kafka cluster. It is used as 1) the default client-id prefix, 2) the group-id for membership management, 3) the changelog topic prefix.";

        /**{@code user.endpoint} */

        public static string APPLICATION_SERVER_CONFIG = "application.server";
        private static string APPLICATION_SERVER_DOC = "A host:port pair pointing to an embedded user defined endpoint that can be used for discovering the locations of state stores within a single KafkaStreams application";

        /** {@code bootstrap.servers} */

        public static string BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

        /** {@code buffered.records.per.partition} */

        public static string BUFFERED_RECORDS_PER_PARTITION_CONFIG = "buffered.records.per.partition";
        private static string BUFFERED_RECORDS_PER_PARTITION_DOC = "Maximum number of records to buffer per partition.";

        /** {@code cache.max.bytes.buffering} */

        public static string CACHE_MAX_BYTES_BUFFERING_CONFIG = "cache.max.bytes.buffering";
        private static string CACHE_MAX_BYTES_BUFFERING_DOC = "Maximum number of memory bytes to be used for buffering across all threads";

        /** {@code client.id} */

        public static string CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;
        private static string CLIENT_ID_DOC = "An ID prefix string used for the client IDs of internal consumer, producer and restore-consumer," +
            " with pattern '<client.id>-StreamThread-<threadSequenceNumber>-<consumer|producer|restore-consumer>'.";

        /** {@code commit.interval.ms} */

        public static string COMMIT_INTERVAL_MS_CONFIG = "commit.interval.ms";
        private static string COMMIT_INTERVAL_MS_DOC = "The frequency with which to save the position of the processor." +
            " (Note, if <code>processing.guarantee</code> is set to <code>" + EXACTLY_ONCE + "</code>, the default value is <code>" + EOS_DEFAULT_COMMIT_INTERVAL_MS + "</code>," +
            " otherwise the default value is <code>" + DEFAULT_COMMIT_INTERVAL_MS + "</code>.";

        /** {@code max.task.idle.ms} */
        public static string MAX_TASK_IDLE_MS_CONFIG = "max.task.idle.ms";
        private static string MAX_TASK_IDLE_MS_DOC = "Maximum amount of time a stream task will stay idle when not all of its partition buffers contain records," +
            " to avoid potential out-of-order record processing across multiple input streams.";

        /** {@code connections.max.idle.ms} */

        public static string CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

        /**
         * {@code default.deserialization.exception.handler}
         */

        public static string DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG = "default.deserialization.exception.handler";
        private static string DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_DOC = "Exception handling that : the <code>org.apache.kafka.streams.errors.DeserializationExceptionHandler</code> interface.";

        /**
         * {@code default.production.exception.handler}
         */

        public static string DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG = "default.production.exception.handler";
        private static string DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_DOC = "Exception handling that : the <code>org.apache.kafka.streams.errors.ProductionExceptionHandler</code> interface.";

        /**
         * {@code default.windowed.key.serde.inner}
         */

        public static string DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS = "default.windowed.key.serde.inner";

        /**
         * {@code default.windowed.value.serde.inner}
         */

        public static string DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS = "default.windowed.value.serde.inner";

        /** {@code default key.serde} */

        public static string DEFAULT_KEY_SERDE_CLASS_CONFIG = "default.key.serde";
        private static string DEFAULT_KEY_SERDE_CLASS_DOC = " Default serializer / deserializer for key that : the <code>org.apache.kafka.common.serialization.ISerde</code> interface. "
                + "Note when windowed serde is used, one needs to set the inner serde that : the <code>org.apache.kafka.common.serialization.ISerde</code> interface via '"
                + DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS + "' or '" + DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS + "' as well";

        /** {@code default value.serde} */

        public static string DEFAULT_VALUE_SERDE_CLASS_CONFIG = "default.value.serde";
        private static string DEFAULT_VALUE_SERDE_CLASS_DOC = "Default serializer / deserializer for value that : the <code>org.apache.kafka.common.serialization.ISerde</code> interface. "
                + "Note when windowed serde is used, one needs to set the inner serde that : the <code>org.apache.kafka.common.serialization.ISerde</code> interface via '"
                + DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS + "' or '" + DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS + "' as well";

        /** {@code default.timestamp.extractor} */

        public static string DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG = "default.timestamp.extractor";
        private static string DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_DOC = "Default timestamp extractor that : the <code>org.apache.kafka.streams.processor.TimestampExtractor</code> interface.";

        /** {@code metadata.max.age.ms} */

        public static string METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;

        /** {@code metrics.num.samples} */

        public static string METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;

        /** {@code metrics.record.level} */

        public static string METRICS_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;

        /** {@code metric.reporters} */

        public static string METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

        /** {@code metrics.sample.window.ms} */

        public static string METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;

        /** {@code num.standby.replicas} */

        public static string NUM_STANDBY_REPLICAS_CONFIG = "num.standby.replicas";
        private static string NUM_STANDBY_REPLICAS_DOC = "The number of standby replicas for each task.";

        /** {@code num.stream.threads} */

        public static string NUM_STREAM_THREADS_CONFIG = "num.stream.threads";
        private static string NUM_STREAM_THREADS_DOC = "The number of threads to execute stream processing.";

        /** {@code partition.grouper} */

        public static string PARTITION_GROUPER_CLASS_CONFIG = "partition.grouper";
        private static string PARTITION_GROUPER_CLASS_DOC = "Partition grouper that : the <code>org.apache.kafka.streams.processor.PartitionGrouper</code> interface.";

        /** {@code poll.ms} */

        public static string POLL_MS_CONFIG = "poll.ms";
        private static string POLL_MS_DOC = "The amount of time in milliseconds to block waiting for input.";

        /** {@code processing.guarantee} */

        public static string PROCESSING_GUARANTEE_CONFIG = "processing.guarantee";
        private static string PROCESSING_GUARANTEE_DOC = "The processing guarantee that should be used. Possible values are <code>" + AT_LEAST_ONCE + "</code> (default) and <code>" + EXACTLY_ONCE + "</code>. " +
            "Note that exactly-once processing requires a cluster of at least three brokers by default what is the recommended setting for production; for development you can change this, by adjusting broker setting " +
            "<code>transaction.state.log.replication.factor</code> and <code>transaction.state.log.min.isr</code>.";

        /** {@code receive.buffer.bytes} */

        public static string RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;

        /** {@code reconnect.backoff.ms} */

        public static string RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;

        /** {@code reconnect.backoff.max} */

        public static string RECONNECT_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG;

        /** {@code replication.factor} */

        public static string REPLICATION_FACTOR_CONFIG = "replication.factor";
        private static string REPLICATION_FACTOR_DOC = "The replication factor for change log topics and repartition topics created by the stream processing application.";

        /** {@code request.timeout.ms} */

        public static string REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;

        /** {@code retries} */

        public static string RETRIES_CONFIG = CommonClientConfigs.RETRIES_CONFIG;

        /** {@code retry.backoff.ms} */

        public static string RETRY_BACKOFF_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;

        public new int GetInt(string key) => this.GetInt(key);

        /** {@code rocksdb.config.setter} */

        public static string ROCKSDB_CONFIG_SETTER_CLASS_CONFIG = "rocksdb.config.setter";
        private static string ROCKSDB_CONFIG_SETTER_CLASS_DOC = "A Rocks DB config setter or name that : the <code>org.apache.kafka.streams.state.RocksDBConfigSetter</code> interface";

        /** {@code security.protocol} */

        public static string SECURITY_PROTOCOL_CONFIG = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;

        /** {@code send.buffer.bytes} */

        public static string SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;

        /** {@code state.cleanup.delay} */

        public static string STATE_CLEANUP_DELAY_MS_CONFIG = "state.cleanup.delay.ms";
        private static string STATE_CLEANUP_DELAY_MS_DOC = "The amount of time in milliseconds to wait before deleting state when a partition has migrated. Only state directories that have not been modified for at least <code>state.cleanup.delay.ms</code> will be removed";

        /** {@code state.dir} */

        public static string STATE_DIR_CONFIG = "state.dir";
        private static string STATE_DIR_DOC = "Directory location for state store.";

        /** {@code topology.optimization} */
        public static string TOPOLOGY_OPTIMIZATION = "topology.optimization";
        private static string TOPOLOGY_OPTIMIZATION_DOC = "A configuration telling Kafka Streams if it should optimize the topology, disabled by default";

        /** {@code upgrade.from} */

        public static string UPGRADE_FROM_CONFIG = "upgrade.from";
        private static string UPGRADE_FROM_DOC = "Allows upgrading from versions 0.10.0/0.10.1/0.10.2/0.11.0/1.0/1.1 to version 1.2 (or newer) in a backward compatible way. " +
            "When upgrading from 1.2 to a newer version it is not required to specify this config." +
            "Default is null. Accepted values are \"" + UPGRADE_FROM_0100 + "\", \"" + UPGRADE_FROM_0101 + "\", \"" + UPGRADE_FROM_0102 + "\", \"" + UPGRADE_FROM_0110 + "\", \"" + UPGRADE_FROM_10 + "\", \"" + UPGRADE_FROM_11 + "\" (for upgrading from the corresponding old version).";

        /** {@code windowstore.changelog.Additional.retention.ms} */

        public static string WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG = "windowstore.changelog.Additional.retention.ms";
        private static string WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_DOC = "Added to a windows maintainMs to ensure data is not deleted from the log prematurely. Allows for clock drift. Default is 1 day";

        private static string[] NON_CONFIGURABLE_CONSUMER_DEFAULT_CONFIGS = new string[] { ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG };
        private static string[] NON_CONFIGURABLE_CONSUMER_EOS_CONFIGS = new string[] { ConsumerConfig.ISOLATION_LEVEL_CONFIG };
        private static string[] NON_CONFIGURABLE_PRODUCER_EOS_CONFIGS = new string[] {ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                                                                                        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION};

        //    static
        //{

        //        CONFIG = new ConfigDef()

        // HIGH

        //            .define(APPLICATION_ID_CONFIG, // required with no default value
        //                    Type.STRING,
        //                    Importance.HIGH,
        //                    APPLICATION_ID_DOC)
        //            .define(BOOTSTRAP_SERVERS_CONFIG, // required with no default value
        //                    Type.LIST,
        //                    Importance.HIGH,
        //                    CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
        //            .define(REPLICATION_FACTOR_CONFIG,
        //                    Type.INT,
        //                    1,
        //                    Importance.HIGH,
        //                    REPLICATION_FACTOR_DOC)
        //            .define(STATE_DIR_CONFIG,
        //                    Type.STRING,
        //                    "/tmp/kafka-streams",
        //                    Importance.HIGH,
        //                    STATE_DIR_DOC)

        //            // MEDIUM

        //            .define(CACHE_MAX_BYTES_BUFFERING_CONFIG,
        //                    Type.LONG,
        //                    10 * 1024 * 1024L,
        //                    atLeast(0),
        //                    Importance.MEDIUM,
        //                    CACHE_MAX_BYTES_BUFFERING_DOC)
        //            .define(CLIENT_ID_CONFIG,
        //                    Type.STRING,
        //                    "",
        //                    Importance.MEDIUM,
        //                    CLIENT_ID_DOC)
        //            .define(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        //                    Type.CLASS,
        //                    LogAndFailExceptionHandler.getName(),
        //                    Importance.MEDIUM,
        //                    DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_DOC)
        //            .define(DEFAULT_KEY_SERDE_CLASS_CONFIG,
        //                    Type.CLASS,
        //                    Serdes.ByteArraySerde.getName(),
        //                    Importance.MEDIUM,
        //                    DEFAULT_KEY_SERDE_CLASS_DOC)
        //            .define(DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
        //                    Type.CLASS,
        //                    DefaultProductionExceptionHandler.getName(),
        //                    Importance.MEDIUM,
        //                    DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_DOC)
        //            .define(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        //                    Type.CLASS,
        //                    FailOnInvalidTimestamp.getName(),
        //                    Importance.MEDIUM,
        //                    DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_DOC)
        //            .define(DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        //                    Type.CLASS,
        //                    Serdes.ByteArraySerde.getName(),
        //                    Importance.MEDIUM,
        //                    DEFAULT_VALUE_SERDE_CLASS_DOC)
        //            .define(NUM_STANDBY_REPLICAS_CONFIG,
        //                    Type.INT,
        //                    0,
        //                    Importance.MEDIUM,
        //                    NUM_STANDBY_REPLICAS_DOC)
        //            .define(NUM_STREAM_THREADS_CONFIG,
        //                    Type.INT,
        //                    1,
        //                    Importance.MEDIUM,
        //                    NUM_STREAM_THREADS_DOC)
        //            .define(MAX_TASK_IDLE_MS_CONFIG,
        //                    Type.LONG,
        //                    0L,
        //                    Importance.MEDIUM,
        //                    MAX_TASK_IDLE_MS_DOC)
        //            .define(PROCESSING_GUARANTEE_CONFIG,
        //                    Type.STRING,
        //                    AT_LEAST_ONCE,
        //                    in (AT_LEAST_ONCE, EXACTLY_ONCE),
        //                    Importance.MEDIUM,
        //                    PROCESSING_GUARANTEE_DOC)
        //            .define(SECURITY_PROTOCOL_CONFIG,
        //                    Type.STRING,
        //                    CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
        //                    Importance.MEDIUM,
        //                    CommonClientConfigs.SECURITY_PROTOCOL_DOC)
        //            .define(TOPOLOGY_OPTIMIZATION,
        //                    Type.STRING,
        //                    NO_OPTIMIZATION,
        //                    in (NO_OPTIMIZATION, OPTIMIZE),
        //                    Importance.MEDIUM,
        //                    TOPOLOGY_OPTIMIZATION_DOC)

        //            // LOW

        //            .define(APPLICATION_SERVER_CONFIG,
        //                    Type.STRING,
        //                    "",
        //                    Importance.LOW,
        //                    APPLICATION_SERVER_DOC)
        //            .define(BUFFERED_RECORDS_PER_PARTITION_CONFIG,
        //                    Type.INT,
        //                    1000,
        //                    Importance.LOW,
        //                    BUFFERED_RECORDS_PER_PARTITION_DOC)
        //            .define(COMMIT_INTERVAL_MS_CONFIG,
        //                    Type.LONG,
        //                    DEFAULT_COMMIT_INTERVAL_MS,
        //                    atLeast(0),
        //                    Importance.LOW,
        //                    COMMIT_INTERVAL_MS_DOC)
        //            .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
        //                    ConfigDef.Type.LONG,
        //                    9 * 60 * 1000L,
        //                    ConfigDef.Importance.LOW,
        //                    CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
        //            .define(METADATA_MAX_AGE_CONFIG,
        //                    ConfigDef.Type.LONG,
        //                    5 * 60 * 1000L,
        //                    atLeast(0),
        //                    ConfigDef.Importance.LOW,
        //                    CommonClientConfigs.METADATA_MAX_AGE_DOC)
        //            .define(METRICS_NUM_SAMPLES_CONFIG,
        //                    Type.INT,
        //                    2,
        //                    atLeast(1),
        //                    Importance.LOW,
        //                    CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
        //            .define(METRIC_REPORTER_CLASSES_CONFIG,
        //                    Type.LIST,
        //                    "",
        //                    Importance.LOW,
        //                    CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
        //            .define(METRICS_RECORDING_LEVEL_CONFIG,
        //                    Type.STRING,
        //                    RecordingLevel.INFO.ToString(),
        //                    in (RecordingLevel.INFO.ToString(), RecordingLevel.DEBUG.ToString()),
        //                    Importance.LOW,
        //                    CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC)
        //            .define(METRICS_SAMPLE_WINDOW_MS_CONFIG,
        //                    Type.LONG,
        //                    30000L,
        //                    atLeast(0),
        //                    Importance.LOW,
        //                    CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
        //            .define(PARTITION_GROUPER_CLASS_CONFIG,
        //                    Type.CLASS,
        //                    DefaultPartitionGrouper.getName(),
        //                    Importance.LOW,
        //                    PARTITION_GROUPER_CLASS_DOC)
        //            .define(POLL_MS_CONFIG,
        //                    Type.LONG,
        //                    100L,
        //                    Importance.LOW,
        //                    POLL_MS_DOC)
        //            .define(RECEIVE_BUFFER_CONFIG,
        //                    Type.INT,
        //                    32 * 1024,
        //                    atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND),
        //                    Importance.LOW,
        //                    CommonClientConfigs.RECEIVE_BUFFER_DOC)
        //            .define(RECONNECT_BACKOFF_MS_CONFIG,
        //                    Type.LONG,
        //                    50L,
        //                    atLeast(0L),
        //                    Importance.LOW,
        //                    CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
        //            .define(RECONNECT_BACKOFF_MAX_MS_CONFIG,
        //                    Type.LONG,
        //                    1000L,
        //                    atLeast(0L),
        //                    ConfigDef.Importance.LOW,
        //                    CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
        //            .define(RETRIES_CONFIG,
        //                    Type.INT,
        //                    0,
        //                    between(0, int.MaxValue),
        //                    ConfigDef.Importance.LOW,
        //                    CommonClientConfigs.RETRIES_DOC)
        //            .define(RETRY_BACKOFF_MS_CONFIG,
        //                    Type.LONG,
        //                    100L,
        //                    atLeast(0L),
        //                    ConfigDef.Importance.LOW,
        //                    CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
        //            .define(REQUEST_TIMEOUT_MS_CONFIG,
        //                    Type.INT,
        //                    40 * 1000,
        //                    atLeast(0),
        //                    ConfigDef.Importance.LOW,
        //                    CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC)
        //            .define(ROCKSDB_CONFIG_SETTER_CLASS_CONFIG,
        //                    Type.CLASS,
        //                    null,
        //                    Importance.LOW,
        //                    ROCKSDB_CONFIG_SETTER_CLASS_DOC)
        //            .define(SEND_BUFFER_CONFIG,
        //                    Type.INT,
        //                    128 * 1024,
        //                    atLeast(CommonClientConfigs.SEND_BUFFER_LOWER_BOUND),
        //                    Importance.LOW,
        //                    CommonClientConfigs.SEND_BUFFER_DOC)
        //            .define(STATE_CLEANUP_DELAY_MS_CONFIG,
        //                    Type.LONG,
        //                    10 * 60 * 1000L,
        //                    Importance.LOW,
        //                    STATE_CLEANUP_DELAY_MS_DOC)
        //            .define(UPGRADE_FROM_CONFIG,
        //                    ConfigDef.Type.STRING,
        //                    null,
        //                    in (null, UPGRADE_FROM_0100, UPGRADE_FROM_0101, UPGRADE_FROM_0102, UPGRADE_FROM_0110, UPGRADE_FROM_10, UPGRADE_FROM_11),
        //                    Importance.LOW,
        //                    UPGRADE_FROM_DOC)
        //            .define(WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG,
        //                    Type.LONG,
        //                    24 * 60 * 60 * 1000L,
        //                    Importance.LOW,
        //                    WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_DOC);
        //}

        // this is the list of configs for underlying clients
        // that streams prefer different default values
        //    private static  Dictionary<string, object> PRODUCER_DEFAULT_OVERRIDES;
        //    static
        //{

        //         Dictionary<string, object> tempProducerDefaultOverrides = new HashMap<>();
        //        tempProducerDefaultOverrides.Add(ProducerConfig.LINGER_MS_CONFIG, "100");
        //        PRODUCER_DEFAULT_OVERRIDES = Collections.unmodifiableMap(tempProducerDefaultOverrides);
        //    }

        //    private static  Dictionary<string, object> PRODUCER_EOS_OVERRIDES;
        //    static
        //{

        //         Dictionary<string, object> tempProducerDefaultOverrides = new HashMap<>(PRODUCER_DEFAULT_OVERRIDES);
        //        tempProducerDefaultOverrides.Add(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, int.MaxValue);
        //        tempProducerDefaultOverrides.Add(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        //        PRODUCER_EOS_OVERRIDES = Collections.unmodifiableMap(tempProducerDefaultOverrides);
        //    }

        //    private static  Dictionary<string, object> CONSUMER_DEFAULT_OVERRIDES;
        //    static
        //{

        //         Dictionary<string, object> tempConsumerDefaultOverrides = new HashMap<>();
        //        tempConsumerDefaultOverrides.Add(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        //        tempConsumerDefaultOverrides.Add(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //        tempConsumerDefaultOverrides.Add(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //        tempConsumerDefaultOverrides.Add("internal.leave.group.on.close", false);
        //        CONSUMER_DEFAULT_OVERRIDES = Collections.unmodifiableMap(tempConsumerDefaultOverrides);
        //    }

        //    private static  Dictionary<string, object> CONSUMER_EOS_OVERRIDES;
        //    static
        //{

        //         Dictionary<string, object> tempConsumerDefaultOverrides = new HashMap<>(CONSUMER_DEFAULT_OVERRIDES);
        //        tempConsumerDefaultOverrides.Add(ConsumerConfig.ISOLATION_LEVEL_CONFIG, READ_COMMITTED.name().toLowerCase(Locale.ROOT));
        //        CONSUMER_EOS_OVERRIDES = Collections.unmodifiableMap(tempConsumerDefaultOverrides);
        //    }

        public static class InternalConfig
        {
            public static string TASK_MANAGER_FOR_PARTITION_ASSIGNOR = "__task.manager.instance__";
            public static string ASSIGNMENT_ERROR_CODE = "__assignment.error.code__";
        }

        /**
         * Prefix a property with {@link #CONSUMER_PREFIX}. This is used to isolate {@link ConsumerConfig consumer configs}
         * from other client configs.
         *
         * @param consumerProp the consumer property to be masked
         * @return {@link #CONSUMER_PREFIX} + {@code consumerProp}
         */
        public static string consumerPrefix(string consumerProp)
        {
            return CONSUMER_PREFIX + consumerProp;
        }

        /**
         * Prefix a property with {@link #MAIN_CONSUMER_PREFIX}. This is used to isolate {@link ConsumerConfig main consumer configs}
         * from other client configs.
         *
         * @param consumerProp the consumer property to be masked
         * @return {@link #MAIN_CONSUMER_PREFIX} + {@code consumerProp}
         */
        public static string mainConsumerPrefix(string consumerProp)
        {
            return MAIN_CONSUMER_PREFIX + consumerProp;
        }

        /**
         * Prefix a property with {@link #RESTORE_CONSUMER_PREFIX}. This is used to isolate {@link ConsumerConfig restore consumer configs}
         * from other client configs.
         *
         * @param consumerProp the consumer property to be masked
         * @return {@link #RESTORE_CONSUMER_PREFIX} + {@code consumerProp}
         */
        public static string restoreConsumerPrefix(string consumerProp)
        {
            return RESTORE_CONSUMER_PREFIX + consumerProp;
        }

        /**
         * Prefix a property with {@link #GLOBAL_CONSUMER_PREFIX}. This is used to isolate {@link ConsumerConfig global consumer configs}
         * from other client configs.
         *
         * @param consumerProp the consumer property to be masked
         * @return {@link #GLOBAL_CONSUMER_PREFIX} + {@code consumerProp}
         */
        public static string globalConsumerPrefix(string consumerProp)
        {
            return GLOBAL_CONSUMER_PREFIX + consumerProp;
        }

        /**
         * Prefix a property with {@link #PRODUCER_PREFIX}. This is used to isolate {@link ProducerConfig producer configs}
         * from other client configs.
         *
         * @param producerProp the producer property to be masked
         * @return PRODUCER_PREFIX + {@code producerProp}
         */
        public static string producerPrefix(string producerProp)
        {
            return PRODUCER_PREFIX + producerProp;
        }

        /**
         * Prefix a property with {@link #ADMIN_CLIENT_PREFIX}. This is used to isolate {@link AdminClientConfig admin configs}
         * from other client configs.
         *
         * @param adminClientProp the admin client property to be masked
         * @return ADMIN_CLIENT_PREFIX + {@code adminClientProp}
         */
        public static string adminClientPrefix(string adminClientProp)
        {
            return ADMIN_CLIENT_PREFIX + adminClientProp;
        }

        /**
         * Prefix a property with {@link #TOPIC_PREFIX}
         * used to provide default topic configs to be applied when creating internal topics.
         *
         * @param topicProp the topic property to be masked
         * @return TOPIC_PREFIX + {@code topicProp}
         */

        public static string topicPrefix(string topicProp)
        {
            return TOPIC_PREFIX + topicProp;
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

        protected StreamsConfig(
            Dictionary<string, object> props,
            bool doLog)
            : base(CONFIG, props, doLog)
        {
            eosEnabled = EXACTLY_ONCE.Equals(getString(PROCESSING_GUARANTEE_CONFIG));
        }


        protected Dictionary<string, object> postProcessParsedConfig(Dictionary<string, object> parsedValues)
        {
            Dictionary<string, object> configUpdates =
               CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);

            bool eosEnabled = EXACTLY_ONCE.Equals(parsedValues[PROCESSING_GUARANTEE_CONFIG]);
            if (eosEnabled && !originals().ContainsKey(COMMIT_INTERVAL_MS_CONFIG))
            {
                log.LogDebug("Using {} default value of {} as exactly once is enabled.",
                        COMMIT_INTERVAL_MS_CONFIG, EOS_DEFAULT_COMMIT_INTERVAL_MS);
                configUpdates.Add(COMMIT_INTERVAL_MS_CONFIG, EOS_DEFAULT_COMMIT_INTERVAL_MS);
            }

            return configUpdates;
        }

        private Dictionary<string, object> getCommonConsumerConfigs()
        {
            Dictionary<string, object> clientProvidedProps = getClientPropsWithPrefix(CONSUMER_PREFIX, ConsumerConfig.configNames());

            checkIfUnexpectedUserSpecifiedConsumerConfig(clientProvidedProps, NON_CONFIGURABLE_CONSUMER_DEFAULT_CONFIGS);
            checkIfUnexpectedUserSpecifiedConsumerConfig(clientProvidedProps, NON_CONFIGURABLE_CONSUMER_EOS_CONFIGS);

            Dictionary<string, object> consumerProps = new HashMap<>(eosEnabled ? CONSUMER_EOS_OVERRIDES : CONSUMER_DEFAULT_OVERRIDES);
            consumerProps.putAll(getClientCustomProps());
            consumerProps.putAll(clientProvidedProps);

            // bootstrap.servers should be from StreamsConfig
            consumerProps.Add(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, originals()[BOOTSTRAP_SERVERS_CONFIG];

            return consumerProps;
        }

        private void checkIfUnexpectedUserSpecifiedConsumerConfig(Dictionary<string, object> clientProvidedProps, string[] nonConfigurableConfigs)
        {
            // Streams does not allow users to configure certain consumer/producer configurations, for example,
            // enable.auto.commit. In cases where user tries to override such non-configurable
            // consumer/producer configurations, log a warning and Remove the user defined value from the Map.
            // Thus the default values for these consumer/producer configurations that are suitable for
            // Streams will be used instead.

            if (eosEnabled)
            {
                object maxInFlightRequests = clientProvidedProps[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION];

                if (maxInFlightRequests != null)
                {
                    int maxInFlightRequestsAsInteger;
                    if (maxInFlightRequests is int)
                    {
                        maxInFlightRequestsAsInteger = (int)maxInFlightRequests;
                    }
                    else if (maxInFlightRequests is string)
                    {
                        try
                        {
                            maxInFlightRequestsAsInteger = int.Parse(((string)maxInFlightRequests).trim());
                        }
                        catch (Exception e)
                        {
                            throw new Exception(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequests, "string value could not be parsed as 32-bit integer");
                        }
                    }
                    else
                    {

                        throw new Exception(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequests, "Expected value to be a 32-bit integer, but it was a " + maxInFlightRequests.GetType().getName());
                    }

                    if (maxInFlightRequestsAsInteger > 5)
                    {
                        throw new Exception(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequestsAsInteger, "Can't exceed 5 when exactly-once processing is enabled");
                    }
                }
            }

            foreach (string config in nonConfigurableConfigs)
            {
                if (clientProvidedProps.ContainsKey(config))
                {
                    string eosMessage = PROCESSING_GUARANTEE_CONFIG + " is set to " + EXACTLY_ONCE + ". Hence, ";
                    string nonConfigurableConfigMessage = "Unexpected user-specified %s config: %s found. %sUser setting (%s) will be ignored and the Streams default setting (%s) will be used ";

                    if (CONSUMER_DEFAULT_OVERRIDES.ContainsKey(config))
                    {
                        if (!clientProvidedProps[config].Equals(CONSUMER_DEFAULT_OVERRIDES[config]))
                        {
                            log.LogWarning(string.Format(nonConfigurableConfigMessage, "consumer", config, "", clientProvidedProps[config], CONSUMER_DEFAULT_OVERRIDES[config]));
                            clientProvidedProps.Remove(config);
                        }
                    }
                    else if (eosEnabled)
                    {
                        if (CONSUMER_EOS_OVERRIDES.ContainsKey(config))
                        {
                            if (!clientProvidedProps[config].Equals(CONSUMER_EOS_OVERRIDES[config]))
                            {
                                log.LogWarning(string.Format(nonConfigurableConfigMessage,
                                        "consumer", config, eosMessage, clientProvidedProps[config], CONSUMER_EOS_OVERRIDES[config])];
                                clientProvidedProps.Remove(config);
                            }
                        }
                        else if (PRODUCER_EOS_OVERRIDES.ContainsKey(config))
                        {
                            if (!clientProvidedProps[config].Equals(PRODUCER_EOS_OVERRIDES[config]))
                            {
                                log.LogWarning(string.Format(nonConfigurableConfigMessage,
                                        "producer", config, eosMessage, clientProvidedProps[config], PRODUCER_EOS_OVERRIDES[config])];
                                clientProvidedProps.Remove(config);
                            }
                        }
                    }
                }
            }
        }

        /**
         * Get the configs to the {@link KafkaConsumer consumer}.
         * Properties using the prefix {@link #CONSUMER_PREFIX} will be used in favor over their non-prefixed versions
         * except in the case of {@link ConsumerConfig#BOOTSTRAP_SERVERS_CONFIG} where we always use the non-prefixed
         * version as we only support reading/writing from/to the same Kafka Cluster.
         *
         * @param groupId      consumer groupId
         * @param clientId     clientId
         * @return Map of the consumer configuration.
         * @deprecated use {@link StreamsConfig#getMainConsumerConfigs(string, string, int)}
         */

        [System.Obsolete]
        public Dictionary<string, object> getConsumerConfigs(string groupId, string clientId)
        {
            return getMainConsumerConfigs(groupId, clientId, DUMMY_THREAD_INDEX);
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

        public Dictionary<string, object> getMainConsumerConfigs(string groupId, string clientId, int threadIdx)
        {
            Dictionary<string, object> consumerProps = getCommonConsumerConfigs();

            // Get main consumer override configs
            Dictionary<string, object> mainConsumerProps = originalsWithPrefix(MAIN_CONSUMER_PREFIX);
            foreach (KeyValuePair<string, object> entry in mainConsumerProps)
            {
                consumerProps.Add(entry.Key, entry.Value);
            }

            // this is a hack to work around StreamsConfig constructor inside StreamsPartitionAssignor to avoid casting
            consumerProps.Add(APPLICATION_ID_CONFIG, groupId);

            //.Add group id, client id with stream client id prefix, and group instance id
            consumerProps.Add(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            consumerProps.Add(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
            string groupInstanceId = (string)consumerProps[ConsumerConfig.GROUP_INSTANCE_ID_CONFIG];
            // Suffix each thread consumer with thread.id to enforce uniqueness of group.instance.id.
            if (groupInstanceId != null)
            {
                consumerProps.Add(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId + "-" + threadIdx);
            }

            //.Add configs required for stream partition assignor
            consumerProps.Add(UPGRADE_FROM_CONFIG, getString(UPGRADE_FROM_CONFIG));
            consumerProps.Add(REPLICATION_FACTOR_CONFIG, getInt(REPLICATION_FACTOR_CONFIG));
            consumerProps.Add(APPLICATION_SERVER_CONFIG, getString(APPLICATION_SERVER_CONFIG));
            consumerProps.Add(NUM_STANDBY_REPLICAS_CONFIG, getInt(NUM_STANDBY_REPLICAS_CONFIG));
            consumerProps.Add(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StreamsPartitionAssignor.getName());
            consumerProps.Add(WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, getLong(WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG));

            //.Add admin retries configs for creating topics
            AdminClientConfig adminClientDefaultConfig = new AdminClientConfig(getClientPropsWithPrefix(ADMIN_CLIENT_PREFIX, AdminClientConfig.configNames()));
            consumerProps.Add(adminClientPrefix(AdminClientConfig.RETRIES_CONFIG), adminClientDefaultConfig.getInt(AdminClientConfig.RETRIES_CONFIG));
            consumerProps.Add(adminClientPrefix(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG), adminClientDefaultConfig.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG));

            // verify that producer batch config is no larger than segment size, then.Add topic configs required for creating topics
            Dictionary<string, object> topicProps = originalsWithPrefix(TOPIC_PREFIX, false);
            Dictionary<string, object> producerProps = getClientPropsWithPrefix(PRODUCER_PREFIX, ProducerConfig.configNames());

            if (topicProps.ContainsKey(topicPrefix(TopicConfig.SEGMENT_BYTES_CONFIG)) &&
                producerProps.ContainsKey(ProducerConfig.BATCH_SIZE_CONFIG))
            {
                int segmentSize = int.Parse(topicProps[topicPrefix(TopicConfig.SEGMENT_BYTES_CONFIG)].ToString());
                int batchSize = int.Parse(producerProps[ProducerConfig.BATCH_SIZE_CONFIG].ToString());

                if (segmentSize < batchSize)
                {
                    throw new System.ArgumentException(string.Format("Specified topic segment size %d is is smaller than the configured producer batch size %d, this will cause produced batch not able to be appended to the topic",
                            segmentSize,
                            batchSize));
                }
            }

            consumerProps.putAll(topicProps);

            return consumerProps;
        }

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
        public Dictionary<string, object> getRestoreConsumerConfigs(string clientId)
        {
            Dictionary<string, object> baseConsumerProps = getCommonConsumerConfigs();

            // Get restore consumer override configs
            Dictionary<string, object> restoreConsumerProps = originalsWithPrefix(RESTORE_CONSUMER_PREFIX);
            foreach (KeyValuePair<string, object> entry in restoreConsumerProps)
            {
                baseConsumerProps.Add(entry.Key, entry.Value);
            }

            // no need to set group id for a restore consumer
            baseConsumerProps.Remove(ConsumerConfig.GROUP_ID_CONFIG);
            //.Add client id with stream client id prefix
            baseConsumerProps.Add(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
            baseConsumerProps.Add(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

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
        public Dictionary<string, object> getGlobalConsumerConfigs(string clientId)
        {
            Dictionary<string, object> baseConsumerProps = getCommonConsumerConfigs();

            // Get global consumer override configs
            Dictionary<string, object> globalConsumerProps = originalsWithPrefix(GLOBAL_CONSUMER_PREFIX);
            foreach (KeyValuePair<string, object> entry in globalConsumerProps)
            {
                baseConsumerProps.Add(entry.Key, entry.Value);
            }

            // no need to set group id for a global consumer
            baseConsumerProps.Remove(ConsumerConfig.GROUP_ID_CONFIG);
            //.Add client id with stream client id prefix
            baseConsumerProps.Add(CommonClientConfigs.CLIENT_ID_CONFIG, clientId + "-global-consumer");
            baseConsumerProps.Add(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

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
            Dictionary<string, object> clientProvidedProps = getClientPropsWithPrefix(PRODUCER_PREFIX, ProducerConfig.configNames());

            checkIfUnexpectedUserSpecifiedConsumerConfig(clientProvidedProps, NON_CONFIGURABLE_PRODUCER_EOS_CONFIGS);

            // generate producer configs from original properties and overridden maps
            Dictionary<string, object> props = new Dictionary<string, object>(eosEnabled ? PRODUCER_EOS_OVERRIDES : PRODUCER_DEFAULT_OVERRIDES);
            props.putAll(getClientCustomProps());
            props.putAll(clientProvidedProps);

            props.Add(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, originals()[BOOTSTRAP_SERVERS_CONFIG]);
            //.Add client id with stream client id prefix
            props.Add(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);

            return props;
        }

        /**
         * Get the configs for the {@link Admin admin client}.
         * @param clientId clientId
         * @return Map of the admin client configuration.
         */

        public Dictionary<string, object> getAdminConfigs(string clientId)
        {
            Dictionary<string, object> clientProvidedProps = getClientPropsWithPrefix(ADMIN_CLIENT_PREFIX, AdminClientConfig.configNames());

            Dictionary<string, object> props = new Dictionary<string, object>();
            props.putAll(getClientCustomProps());
            props.putAll(clientProvidedProps);

            //.Add client id with stream client id prefix
            props.Add(CLIENT_ID_CONFIG, clientId);

            return props;
        }

        private Dictionary<string, object> getClientPropsWithPrefix(string prefix,
        {
            Dictionary<string, object> props = clientProps(configNames, originals());
            props.putAll(originalsWithPrefix(prefix));
            return props;
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
            Dictionary<string, object> props = originals();
            props.Keys.removeAll(CONFIG.names());
            props.Keys.removeAll(ConsumerConfig.configNames());
            props.Keys.removeAll(ProducerConfig.configNames());
            props.Keys.removeAll(AdminClientConfig.configNames());
            props.Keys.removeAll(originalsWithPrefix(CONSUMER_PREFIX, false).Keys);
            props.Keys.removeAll(originalsWithPrefix(PRODUCER_PREFIX, false).Keys);
            props.Keys.removeAll(originalsWithPrefix(ADMIN_CLIENT_PREFIX, false).Keys);

            return props;
        }

        /**
         * Return an {@link Serde#configure(Map, bool) configured} instance of {@link #DEFAULT_KEY_SERDE_CLASS_CONFIG key Serde
         *}.
         *
         * @return an configured instance of key Serde
         */

        public Serde<object> defaultKeySerde()
        {
            object keySerdeConfigSetting = Get(DEFAULT_KEY_SERDE_CLASS_CONFIG);

            try
            {

                ISerde<object> serde = getConfiguredInstance(DEFAULT_KEY_SERDE_CLASS_CONFIG, ISerde);
                serde.Configure(originals(), true);

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

        public Serde<object> defaultValueSerde()
        {
            object valueSerdeConfigSetting = Get(DEFAULT_VALUE_SERDE_CLASS_CONFIG);

            try
            {
                ISerde<object> serde = getConfiguredInstance(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serde);
                serde.Configure(originals(), false);

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
            return getConfiguredInstance<ITimestampExtractor>(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG);
        }

        public IDeserializationExceptionHandler defaultDeserializationExceptionHandler()
        {
            return getConfiguredInstance<IDeserializationExceptionHandler>(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG);
        }

        private T getConfiguredInstance<T>(string key)
        {
            return default;
        }

        public IProductionExceptionHandler defaultProductionExceptionHandler()
        {
            return getConfiguredInstance<IProductionExceptionHandler>(DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG);
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