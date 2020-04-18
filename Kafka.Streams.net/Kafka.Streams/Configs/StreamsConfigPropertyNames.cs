namespace Kafka.Streams.Configs
{
    public partial class StreamsConfig
    {
        /// <summary>
        /// Prefix used to provide default topic configs to be applied when creating internal topics.
        /// 
        ///     default: topic.
        /// </summary>
        public static readonly string TopicPrefixConfig = "topic.";

        /// <summary>
        /// Prefix used to isolate {@link KafkaConsumer consumer} configs from other client configs.
        /// It is recommended to use {@link #consumerPrefix(string)} to add this prefix to {@link ConsumerConfig consumer
        /// properties}.
        /// </summary>
        public static readonly string ConsumerPrefixConfig = "consumer.";

        /// <summary>
        /// Prefix used to override {@link KafkaConsumer consumer} configs for the main consumer client from
        /// the general consumer client configs. The override precedence is the following (from highest to lowest precedence):
        /// 1. main.consumer.[config-Name)
        /// 2. consumer.[config-Name)
        /// 3. [config-Name)
        /// </summary>
        public static readonly string MainConsumerPrefixConfig = "main.consumer.";

        /// <summary>
        /// Prefix used to override {@link KafkaConsumer consumer} configs for the restore consumer client from
        /// the general consumer client configs. The override precedence is the following (from highest to lowest precedence):
        /// 1. restore.consumer.[config-Name)
        /// 2. consumer.[config-Name)
        /// 3. [config-Name)
        /// </summary>
        public static readonly string RestoreConsumerPrefixConfig = "restore.consumer.";

        /// <summary>
        /// Prefix used to override {@link KafkaConsumer consumer} configs for the global consumer client from
        /// the general consumer client configs. The override precedence is the following (from highest to lowest precedence):
        /// 1. global.consumer.[config-Name)
        /// 2. consumer.[config-Name)
        /// 3. [config-Name)
        /// </summary>
        public static readonly string GlobalConsumerPrefixConfig = "global.consumer.";

        /// <summary>
        /// Prefix used to isolate {@link KafkaProducer producer} configs from other client configs.
        /// It is recommended to use {@link #producerPrefix(string)} to add this prefix to {@link ProducerConfig producer
        /// properties}.
        /// </summary>
        public static readonly string ProducerPrefixConfig = "producer.";

        /// <summary>
        /// Prefix used to isolate {@link Admin admin} configs from other client configs.
        /// It is recommended to use {@link #adminClientPrefix(string)} to add this prefix to {@link ProducerConfig producer
        /// properties}.
        /// </summary>
        public static readonly string AdminClientPrefixConfig = "admin.";

        /// <summary>
        /// Config value for parameter (@link #TOPOLOGY_OPTIMIZATION "topology.optimization" for disabling topology optimization
        /// </summary>
        public static readonly string NoOptimizationConfig = "none";

        /// <summary>
        /// Config value for parameter (@link #TOPOLOGY_OPTIMIZATION "topology.optimization" for enabling topology optimization
        /// </summary>
        public static readonly string OPTIMIZEConfig = "All";

        /// <summary>
        /// When set to 'true', the producer will ensure that exactly one copy of each message is written in the stream. If 'false', producer
        /// retries due to broker failures, etc., may write duplicates of the retried message in the stream.
        /// Note that enabling idempotence requires <code>" + MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + "</code> to be less than or equal to 5,
        /// <code>" + RETRIES_CONFIG + "</code> to be greater than 0 and <code>" + ACKS_CONFIG + "</code> must be 'All'. If these values
        /// are not explicitly set by the user, suitable values will be chosen. If incompatible values are set,
        /// a <code>ConfigException</code> will be thrown.
        /// </summary>
        public static readonly string ENABLE_IDEMPOTENCE_CONFIGConfig = "enable.idempotence";

        /// <summary>
        /// <p>Controls how to read messages written transactionally. If set to <code>read_committed</code>, consumer.poll() will only return" +
        /// transactional messages which have been committed. If set to <code>read_uncommitted</code>' (the default), consumer.poll() will return All messages, even transactional messages" +
        /// which have been aborted. Non-transactional messages will be returned unconditionally in either mode.</p> <p>Messages will always be returned in offset order. Hence, in " +
        /// <code>read_committed</code> mode, consumer.poll() will only return messages up to the last stable offset (LSO), which is the one less than the offset of the first open transaction." +
        /// In particular any messages appearing after messages belonging to ongoing transactions will be withheld until the relevant transaction has been completed. As a result, <code>read_committed</code>" +
        /// consumers will not be able to read up to the high watermark when there are in flight transactions.</p><p> Further, when in <code>read_committed</code> the seekToEnd method will" +
        /// return the LSO
        /// </summary>
        public static readonly string ISOLATION_LEVEL_CONFIGConfig = "isolation.level";

        /// <summary>
        /// If true the consumer's offset will be periodically committed in the background.
        /// </summary>
        public static readonly string ENABLE_AUTO_COMMIT_CONFIGConfig = "enable.auto.commit";

        /// <summary>
        /// Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.10.0.x}.
        /// </summary>
        public static readonly string UpgradeFrom0100Config = "0.10.0";

        /// <summary>
        /// Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.10.1.x}.
        /// </summary>
        public static readonly string UpgradeFrom0101Config = "0.10.1";

        /// <summary>
        /// Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.10.2.x}.
        /// </summary>
        public static readonly string UpgradeFrom0102Config = "0.10.2";

        /// <summary>
        /// Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.11.0.x}.
        /// </summary>
        public static readonly string UpgradeFrom0110Config = "0.11.0";

        /// <summary>
        /// Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 1.0.x}.
        /// </summary>
        public static readonly string UpgradeFrom10Config = "1.0";

        /// <summary>
        /// Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 1.1.x}.
        /// </summary>
        public static readonly string UpgradeFrom11Config = "1.1";

        /// <summary>
        /// Config value for parameter {@link #PROCESSING_GUARANTEE_CONFIG "processing.guarantee"} for at-least-once processing guarantees.
        /// </summary>
        public static readonly string AtLeastOnceConfig = "at_least_once";

        /// <summary>
        /// Config value for parameter {@link #PROCESSING_GUARANTEE_CONFIG "processing.guarantee"} for exactly-once processing guarantees.
        /// </summary>
        public static readonly string ExactlyOnceConfig = "exactly_once";

        /// <summary>
        /// An identifier for the stream processing application. Must be unique within the Kafka cluster. It is used as 1) the default client-id prefix, 2) the group-id for membership management, 3) the changelog topic prefix.
        /// </summary>
        public static readonly string ApplicationIdConfig = "application.id";

        /// <summary>
        /// client.id
        /// </summary>
        public static readonly string ClientIdConfig = "client.id";

        /// <summary>
        /// group.id
        /// </summary>
        public static readonly string GroupIdConfig = "group.id";

        /// <summary>
        /// A host:port pair pointing to an embedded user defined endpoint that can be used for discovering the locations of state stores within a single KafkaStreams application.
        /// </summary>
        public static readonly string ApplicationServerConfig = "application.server";
        
        /// <summary>
        /// {@code buffered.records.per.partition}
        /// </summary>
        public static readonly string BUFFERED_RECORDS_PER_PARTITION_CONFIGConfig = "buffered.records.per.partition";
        // private static string BUFFERED_RECORDS_PER_PARTITION_DOCConfig = "Maximum number of records to buffer per partition.";

        /// <summary>
        /// {@code cache.Max.bytes.buffering}
        /// </summary>
        public static readonly string CacheMaxBytesBufferingConfig = "cache.Max.bytes.buffering";
        // private static string CACHE_MAX_BYTES_BUFFERING_DOCConfig = "Maximum number of memory bytes to be used for buffering across All threads";

        /// <summary>
        /// {@code commit.interval.ms}
        /// </summary>
        public static readonly string COMMIT_INTERVAL_MS_CONFIGConfig = "commit.interval.ms";
        //private static string COMMIT_INTERVAL_MS_DOCConfig = "The frequency with which to save the position of the processor." +
        //    " (Note, if <code>processing.guarantee</code> is set to <code>" + ExactlyOnce + "</code>, the default value is <code>" + EOS_DEFAULT_COMMIT_INTERVAL_MS + "</code>," +
        //    " otherwise the default value is <code>" + DEFAULT_COMMIT_INTERVAL_MS + "</code>.";

        /// <summary>
        /// {@code max.task.idle.ms}
        /// </summary>
        public static readonly string MAX_TASK_IDLE_MS_CONFIGConfig = "max.task.idle.ms";
        //private static string MAX_TASK_IDLE_MS_DOCConfig = "Maximum amount of time a stream task will stay idle when not All of its partition buffers contain records," +
        //    " to avoid potential out-of-order record processing across multiple input streams.";

        /// <summary>
        /// {@code connections.Max.idle.ms}
        /// </summary>
        public static readonly string CONNECTIONS_MAX_IDLE_MS_CONFIGConfig = "connections.Max.idle.ms";

        /// <summary>
        /// {@code default.deserialization.exception.handler}
        /// </summary>
        public static readonly string DefaultDeserializationExceptionHandlerClassConfig = "default.deserialization.exception.handler";
        // private static string DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_DOCConfig = "Exception handling that : the <code>org.apache.kafka.streams.errors.IDeserializationExceptionHandler</code> interface.";

        /// <summary>
        /// {@code default.production.exception.handler}
        /// </summary>
        public static readonly string DefaultProductionExceptionHandlerClassConfig = "default.production.exception.handler";
        //        private static string DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_DOCConfig = "Exception handling that : the <code>org.apache.kafka.streams.errors.ProductionExceptionHandler</code> interface.";

        /// <summary>
        /// {@code default.windowed.key.serde.inner}
        /// </summary>
        public static readonly string DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASSConfig = "default.windowed.key.serde.inner";

        /// <summary>
        /// {@code default.windowed.value.serde.inner}
        /// </summary>
        public static readonly string DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASSConfig = "default.windowed.value.serde.inner";

        /// <summary>
        /// {@code default key.serde}
        /// </summary>
        public static readonly string DefaultKeySerdeClassConfig = "default.key.serde";
        //private static string DEFAULT_KEY_SERDE_CLASS_DOCConfig = " Default serializer / deserializer for key that : the <code>org.apache.kafka.common.serialization.ISerde</code> interface. "
        //        + "Note when windowed serde is used, one needs to set the inner serde that : the <code>org.apache.kafka.common.serialization.ISerde</code> interface via '"
        //        + DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASSConfig + "' or '" + DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS + "' as well";

        /// <summary>
        /// {@code default value.serde}
        /// </summary>
        public static readonly string DefaultValueSerdeClassConfig = "default.value.serde";
        //private static string DEFAULT_VALUE_SERDE_CLASS_DOCConfig = "Default serializer / deserializer for value that : the <code>org.apache.kafka.common.serialization.ISerde</code> interface. "
        //        + "Note when windowed serde is used, one needs to set the inner serde that : the <code>org.apache.kafka.common.serialization.ISerde</code> interface via '"
        //        + DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASSConfig + "' or '" + DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS + "' as well";

        /// <summary>
        /// {@code default.timestamp.extractor}
        /// </summary>
        public static readonly string DefaultTimestampExtractorClassConfig = "default.timestamp.extractor";
        // private static string DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_DOCConfig = "Default timestamp extractor that : the <code>org.apache.kafka.streams.processor.ITimestampExtractor</code> interface.";

        /// <summary>
        /// {@code metadata.Max.age.ms}
        /// </summary>
        public static readonly string METADATA_MAX_AGE_CONFIGConfig = "metadata.Max.age.ms";

        /// <summary>
        /// {@code metrics.num.samples}
        /// </summary>
        public static readonly string METRICS_NUM_SAMPLES_CONFIGConfig = "metrics.num.samples";

        /// <summary>
        /// {@code metrics.record.level}
        /// </summary>
        public static readonly string METRICS_RECORDING_LEVEL_CONFIGConfig = "metrics.record.level";

        /// <summary>
        /// {@code metric.reporters}
        /// </summary>
        public static readonly string METRIC_REPORTER_CLASSES_CONFIGConfig = "metric.reporters";

        /// <summary>
        /// {@code metrics.sample.window.ms}
        /// </summary>
        public static readonly string METRICS_SAMPLE_WINDOW_MS_CONFIGConfig = "metrics.sample.window.ms";

        /// <summary>
        /// {@code num.standby.replicas}
        /// </summary>
        public static readonly string NUM_STANDBY_REPLICAS_CONFIGConfig = "num.standby.replicas";
        // private static string NUM_STANDBY_REPLICAS_DOCConfig = "The number of standby replicas for each task.";


        /**
         * <code>partition.assignment.strategy</code>
         * A list of class names or class types, ordered by preference,
         * of supported assignors responsible for the partition assignment strategy
         * that the client will use to distribute partition ownership amongst consumer
         * instances when group management is used.
         * Implementing the <code>org.apache.kafka.clients.consumer.ConsumerPartitionAssignor</code>
         * interface allows you to plug in a custom assignment strategy.
         */
        public static readonly string PARTITION_ASSIGNMENT_STRATEGY_CONFIGConfig = "partition.assignment.strategy";

        /// <summary>
        /// {@code num.stream.threads}
        /// </summary>
        public static readonly string NumberOfStreamThreadsConfig = "num.stream.threads";
        // private static string NUM_STREAM_THREADS_DOCConfig = "The number of threads to execute stream processing.";

        /// <summary>
        /// {@code partition.grouper}
        /// </summary>
        public static readonly string PARTITION_GROUPER_CLASS_CONFIGConfig = "partition.grouper";
        // private static string PARTITION_GROUPER_CLASS_DOCConfig = "Partition grouper that : the <code>org.apache.kafka.streams.processor.PartitionGrouper</code> interface.";

        /// <summary>
        /// "The amount of time in milliseconds to block waiting for input.
        /// </summary>
        public static readonly string PollMsConfig = "poll.ms";

        /// <summary>
        /// The processing guarantee that should be used. Possible values are <code>" + AtLeastOnce + "</code> (default) and <code>" + ExactlyOnce + "</code>.
        /// Note that exactly-once processing requires a cluster of at least three brokers by default what is the recommended setting for production; for development you can change this, by adjusting broker setting
        /// <code>transaction.state.log.replication.factor</code> and <code>transaction.state.log.min.isr</code>.
        /// </summary>
        public static readonly string ProcessingGuaranteeConfig = "processing.guarantee";

        /// <summary>
        /// {@code request.timeout.ms}
        /// </summary>
        public static readonly string REQUEST_TIMEOUT_MS_CONFIGConfig = "request.timeout.ms";

        /// <summary>
        /// {@code retries}
        /// </summary>
        public static readonly string RetriesConfig = "retries";

        /// <summary>
        /// {@code retry.backoff.ms}
        /// </summary>
        public static readonly string RetryBackoffMsConfig = "retry.backoff.ms";

        /// <summary>
        /// {@code rocksdb.config.setter}
        /// </summary>
        public static readonly string ROCKSDB_CONFIG_SETTER_CLASS_CONFIGConfig = "rocksdb.config.setter";
        // private static string ROCKSDB_CONFIG_SETTER_CLASS_DOCConfig = "A Rocks DB config setter or Name that : the <code>org.apache.kafka.streams.state.RocksDbConfigSetter</code> interface";

        /// <summary>
        /// {@code security.protocol}
        /// </summary>
        public static readonly string SECURITY_PROTOCOL_CONFIGConfig = "security.protocol";

        /// <summary>
        /// {@code send.buffer.bytes}
        /// </summary>
        public static readonly string SEND_BUFFER_CONFIGConfig = "send.buffer.bytes";

        /// <summary>
        /// The amount of time in milliseconds to wait before deleting state when a partition has migrated. Only state directories that have not been modified for at least <code>state.cleanup.delay.ms</code> will be removed
        /// {@code state.cleanup.delay}
        /// </summary>
        public static readonly string StateCleanupDelayMsConfig = "state.cleanup.delay.ms";

        /// <summary>
        /// {@code state.dir}
        /// </summary>
        public static readonly string STATE_DIR_CONFIGConfig = "state.dir";
        // private static string STATE_DIR_DOCConfig = "Directory location for state store.";

        /// <summary>
        /// {@code topology.optimization}
        /// </summary>
        public static readonly string TOPOLOGY_OPTIMIZATIONConfig = "topology.optimization";
        // private static string TOPOLOGY_OPTIMIZATION_DOCConfig = "A configuration telling Kafka Streams if it should optimize the topology, disabled by default";

        /// <summary>
        /// {@code upgrade.from}
        /// </summary>
        public static readonly string UPGRADE_FROM_CONFIGConfig = "upgrade.from";
        // private static string UPGRADE_FROM_DOCConfig = "Allows upgrading from versions 0.10.0/0.10.1/0.10.2/0.11.0/1.0/1.1 to version 1.2 (or newer) in a backward compatible way. " +
        //    "When upgrading from 1.2 to a newer version it is not required to specify this config." +
        //    "Default is null. Accepted values are \"" + UpgradeFrom0100 + "\", \"" + UpgradeFrom0101 + "\", \"" + UpgradeFrom0102 + "\", \"" + UpgradeFrom0110 + "\", \"" + UpgradeFrom10 + "\", \"" + UpgradeFrom11 + "\" (for upgrading from the corresponding old version).";

        /// <summary>
        /// {@code windowstore.changelog.Additional.retention.ms}
        /// </summary>
        public static readonly string WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIGConfig = "windowstore.changelog.Additional.retention.ms";

        /// <summary>
        /// A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The client will make use of All servers irrespective of which servers are specified here for bootstrapping&mdash;this list only impacts the initial hosts used to discover the full set of servers. This list should be in the form
        /// <code>host1:port1,host2:port2,...</code>. Since these servers are just used for the initial connection to
        /// discover the full cluster membership (which may change dynamically), this list need not contain the full set of
        /// servers (you may want more than one, though, in case a server is down).
        /// </summary>
        public static readonly string BootstrapServersConfig = "bootstrap.servers";

        /// <summary>
        /// The TransactionalId to use for transactional delivery. This enables reliability semantics which span multiple producer sessions since it allows the client to guarantee that transactions using the same TransactionalId have been completed prior to starting any new transactions. If no TransactionalId is provided, then the producer is limited to idempotent delivery.
        /// Note that <code>enable.idempotence</code> must be enabled if a TransactionalId is configured.
        /// The default is <code>null</code>, which means transactions cannot be used.
        /// Note that, by default, transactions require a cluster of at least three brokers which is the recommended setting for production; for development you can change this, by adjusting broker setting <code>transaction.state.log.replication.factor</code>.
        /// </summary>
        public static readonly string TRANSACTIONAL_ID_CONFIGConfig = "transactional.id";
        
        
        public static readonly string MAX_POLL_INTERVAL_MS_CONFIGConfig = "max.poll.interval.ms";

        /// <summary>
        /// The maximum number of unacknowledged requests the client will send on a single connection before blocking.
        /// Note that if this setting is set to be greater than 1 and there are failed sends, there is a risk of
        /// message re-ordering due to retries (i.e., if retries are enabled).
        /// </summary>
        public const string MAX_IN_FLIGHT_REQUESTS_PER_CONNECTIONConfig = "max.in.flight.requests.per.connection";
        // private static string WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_DOCConfig = "Added to a windows maintainMs to ensure data is not deleted from the log prematurely. Allows for clock drift. Default is 1 day";
    }
}
