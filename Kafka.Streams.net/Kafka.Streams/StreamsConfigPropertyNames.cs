namespace Kafka.Streams
{
    public static class StreamsConfigPropertyNames
    {
        /// <summary>
        /// Prefix used to provide default topic configs to be applied when creating internal topics.
        /// 
        ///     default: topic.
        /// </summary>
        public static string TopicPrefix = "topic.";

        /// <summary>
        /// Prefix used to isolate {@link KafkaConsumer consumer} configs from other client configs.
        /// It is recommended to use {@link #consumerPrefix(string)} to.Add this prefix to {@link ConsumerConfig consumer
        /// properties}.
        /// </summary>
        public static string ConsumerPrefix = "consumer.";

        /// <summary>
        /// Prefix used to override {@link KafkaConsumer consumer} configs for the main consumer client from
        /// the general consumer client configs. The override precedence is the following (from highest to lowest precedence):
        /// 1. main.consumer.[config-name)
        /// 2. consumer.[config-name)
        /// 3. [config-name)
        /// </summary>
        public static string MainConsumerPrefix = "main.consumer.";

        /// <summary>
        /// Prefix used to override {@link KafkaConsumer consumer} configs for the restore consumer client from
        /// the general consumer client configs. The override precedence is the following (from highest to lowest precedence):
        /// 1. restore.consumer.[config-name)
        /// 2. consumer.[config-name)
        /// 3. [config-name)
        /// </summary>
        public static string RestoreConsumerPrefix = "restore.consumer.";

        /// <summary>
        /// Prefix used to override {@link KafkaConsumer consumer} configs for the global consumer client from
        /// the general consumer client configs. The override precedence is the following (from highest to lowest precedence):
        /// 1. global.consumer.[config-name)
        /// 2. consumer.[config-name)
        /// 3. [config-name)
        /// </summary>
        public static string GlobalConsumerPrefix = "global.consumer.";

        /// <summary>
        /// Prefix used to isolate {@link KafkaProducer producer} configs from other client configs.
        /// It is recommended to use {@link #producerPrefix(string)} to.Add this prefix to {@link ProducerConfig producer
        /// properties}.
        /// </summary>
        public static string ProducerPrefix = "producer.";

        /// <summary>
        /// Prefix used to isolate {@link Admin admin} configs from other client configs.
        /// It is recommended to use {@link #adminClientPrefix(string)} to add this prefix to {@link ProducerConfig producer
        /// properties}.
        /// </summary>
        public static string AdminClientPrefix = "admin.";

        /// <summary>
        /// Config value for parameter (@link #TOPOLOGY_OPTIMIZATION "topology.optimization" for disabling topology optimization
        /// </summary>
        public static string NoOptimization = "none";

        /// <summary>
        /// Config value for parameter (@link #TOPOLOGY_OPTIMIZATION "topology.optimization" for enabling topology optimization
        /// </summary>
        public static string OPTIMIZE = "all";

        /// <summary>
        /// Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.10.0.x}.
        /// </summary>
        public static string UpgradeFrom0100 = "0.10.0";

        /// <summary>
        /// Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.10.1.x}.
        /// </summary>
        public static string UpgradeFrom0101 = "0.10.1";

        /// <summary>
        /// Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.10.2.x}.
        /// </summary>
        public static string UpgradeFrom0102 = "0.10.2";

        /// <summary>
        /// Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 0.11.0.x}.
        /// </summary>
        public static string UpgradeFrom0110 = "0.11.0";

        /// <summary>
        /// Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 1.0.x}.
        /// </summary>
        public static string UpgradeFrom10 = "1.0";

        /// <summary>
        /// Config value for parameter {@link #UPGRADE_FROM_CONFIG "upgrade.from"} for upgrading an application from version {@code 1.1.x}.
        /// </summary>
        public static string UpgradeFrom11 = "1.1";

        /// <summary>
        /// Config value for parameter {@link #PROCESSING_GUARANTEE_CONFIG "processing.guarantee"} for at-least-once processing guarantees.
        /// </summary>
        public static string AtLeastOnce = "at_least_once";

        /// <summary>
        /// Config value for parameter {@link #PROCESSING_GUARANTEE_CONFIG "processing.guarantee"} for exactly-once processing guarantees.
        /// </summary>
        public static string ExactlyOnce = "exactly_once";

        /// <summary>
        /// {@code application.id}
        /// </summary>
        public static string ApplicationId = "application.id";
        // private static string APPLICATION_ID_DOC = "An identifier for the stream processing application. Must be unique within the Kafka cluster. It is used as 1) the default client-id prefix, 2) the group-id for membership management, 3) the changelog topic prefix.";

        /// <summary>
        /// client.id
        /// </summary>
        public static string ClientId = "client.id";

        /// <summary>
        /// {@code user.endpoint}
        /// </summary>
        public static string ApplicationServer = "application.server";
        // private static string APPLICATION_SERVER_DOC = "A host:port pair pointing to an embedded user defined endpoint that can be used for discovering the locations of state stores within a single KafkaStreams application";

        /// <summary>
        /// {@code buffered.records.per.partition}
        /// </summary>
        public static string BUFFERED_RECORDS_PER_PARTITION_CONFIG = "buffered.records.per.partition";
        // private static string BUFFERED_RECORDS_PER_PARTITION_DOC = "Maximum number of records to buffer per partition.";

        /// <summary>
        /// {@code cache.max.bytes.buffering}
        /// </summary>
        public static string CACHE_MAX_BYTES_BUFFERING_CONFIG = "cache.max.bytes.buffering";
        // private static string CACHE_MAX_BYTES_BUFFERING_DOC = "Maximum number of memory bytes to be used for buffering across all threads";

        /// <summary>
        /// {@code commit.interval.ms}
        /// </summary>
        public static string COMMIT_INTERVAL_MS_CONFIG = "commit.interval.ms";
        //private static string COMMIT_INTERVAL_MS_DOC = "The frequency with which to save the position of the processor." +
        //    " (Note, if <code>processing.guarantee</code> is set to <code>" + ExactlyOnce + "</code>, the default value is <code>" + EOS_DEFAULT_COMMIT_INTERVAL_MS + "</code>," +
        //    " otherwise the default value is <code>" + DEFAULT_COMMIT_INTERVAL_MS + "</code>.";

        /// <summary>
        /// {@code max.task.idle.ms}
        /// </summary>
        public static string MAX_TASK_IDLE_MS_CONFIG = "max.task.idle.ms";
        //private static string MAX_TASK_IDLE_MS_DOC = "Maximum amount of time a stream task will stay idle when not all of its partition buffers contain records," +
        //    " to avoid potential out-of-order record processing across multiple input streams.";

        /// <summary>
        /// {@code connections.max.idle.ms}
        /// </summary>
        public static string CONNECTIONS_MAX_IDLE_MS_CONFIG = "connections.max.idle.ms";

        /// <summary>
        /// {@code default.deserialization.exception.handler}
        /// </summary>
        public static string DefaultDeserializationExceptionHandlerClass = "default.deserialization.exception.handler";
        // private static string DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_DOC = "Exception handling that : the <code>org.apache.kafka.streams.errors.IDeserializationExceptionHandler</code> interface.";

        /// <summary>
        /// {@code default.production.exception.handler}
        /// </summary>
        public static string DefaultProductionExceptionHandlerClass = "default.production.exception.handler";
        //        private static string DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_DOC = "Exception handling that : the <code>org.apache.kafka.streams.errors.ProductionExceptionHandler</code> interface.";

        /// <summary>
        /// {@code default.windowed.key.serde.inner}
        /// </summary>
        public static string DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS = "default.windowed.key.serde.inner";

        /// <summary>
        /// {@code default.windowed.value.serde.inner}
        /// </summary>
        public static string DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS = "default.windowed.value.serde.inner";

        /// <summary>
        /// {@code default key.serde}
        /// </summary>
        public static string DefaultKeySerdeClass = "default.key.serde";
        //private static string DEFAULT_KEY_SERDE_CLASS_DOC = " Default serializer / deserializer for key that : the <code>org.apache.kafka.common.serialization.ISerde</code> interface. "
        //        + "Note when windowed serde is used, one needs to set the inner serde that : the <code>org.apache.kafka.common.serialization.ISerde</code> interface via '"
        //        + DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS + "' or '" + DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS + "' as well";

        /// <summary>
        /// {@code default value.serde}
        /// </summary>
        public static string DefaultValueSerdeClass = "default.value.serde";
        //private static string DEFAULT_VALUE_SERDE_CLASS_DOC = "Default serializer / deserializer for value that : the <code>org.apache.kafka.common.serialization.ISerde</code> interface. "
        //        + "Note when windowed serde is used, one needs to set the inner serde that : the <code>org.apache.kafka.common.serialization.ISerde</code> interface via '"
        //        + DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS + "' or '" + DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS + "' as well";

        /// <summary>
        /// {@code default.timestamp.extractor}
        /// </summary>
        public static string DefaultTimestampExtractorClass = "default.timestamp.extractor";
        // private static string DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_DOC = "Default timestamp extractor that : the <code>org.apache.kafka.streams.processor.ITimestampExtractor</code> interface.";

        /// <summary>
        /// {@code metadata.max.age.ms}
        /// </summary>
        public static string METADATA_MAX_AGE_CONFIG = "metadata.max.age.ms";

        /// <summary>
        /// {@code metrics.num.samples}
        /// </summary>
        public static string METRICS_NUM_SAMPLES_CONFIG = "metrics.num.samples";

        /// <summary>
        /// {@code metrics.record.level}
        /// </summary>
        public static string METRICS_RECORDING_LEVEL_CONFIG = "metrics.record.level";

        /// <summary>
        /// {@code metric.reporters}
        /// </summary>
        public static string METRIC_REPORTER_CLASSES_CONFIG = "metric.reporters";

        /// <summary>
        /// {@code metrics.sample.window.ms}
        /// </summary>
        public static string METRICS_SAMPLE_WINDOW_MS_CONFIG = "metrics.sample.window.ms";

        /// <summary>
        /// {@code num.standby.replicas}
        /// </summary>
        public static string NUM_STANDBY_REPLICAS_CONFIG = "num.standby.replicas";
        // private static string NUM_STANDBY_REPLICAS_DOC = "The number of standby replicas for each task.";

        /// <summary>
        /// {@code num.stream.threads}
        /// </summary>
        public static string NUM_STREAM_THREADS_CONFIG = "num.stream.threads";
        // private static string NUM_STREAM_THREADS_DOC = "The number of threads to execute stream processing.";

        /// <summary>
        /// {@code partition.grouper}
        /// </summary>
        public static string PARTITION_GROUPER_CLASS_CONFIG = "partition.grouper";
        // private static string PARTITION_GROUPER_CLASS_DOC = "Partition grouper that : the <code>org.apache.kafka.streams.processor.PartitionGrouper</code> interface.";

        /// <summary>
        /// {@code poll.ms}
        /// </summary>

        public static string POLL_MS_CONFIG = "poll.ms";
        // private static string POLL_MS_DOC = "The amount of time in milliseconds to block waiting for input.";

        /// <summary>
        /// {@code processing.guarantee}
        /// </summary>
        public static string PROCESSING_GUARANTEE_CONFIG = "processing.guarantee";
        // private static string PROCESSING_GUARANTEE_DOC = "The processing guarantee that should be used. Possible values are <code>" + AtLeastOnce + "</code> (default) and <code>" + ExactlyOnce + "</code>. " +
        //    "Note that exactly-once processing requires a cluster of at least three brokers by default what is the recommended setting for production; for development you can change this, by adjusting broker setting " +
        //    "<code>transaction.state.log.replication.factor</code> and <code>transaction.state.log.min.isr</code>.";

        /// <summary>
        /// {@code request.timeout.ms}
        /// </summary>
        public static string REQUEST_TIMEOUT_MS_CONFIG = "request.timeout.ms";

        /// <summary>
        /// {@code retries}
        /// </summary>
        public static string RETRIES_CONFIG = "retries";

        /// <summary>
        /// {@code retry.backoff.ms}
        /// </summary>
        public static string RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";

        /// <summary>
        /// {@code rocksdb.config.setter}
        /// </summary>
        public static string ROCKSDB_CONFIG_SETTER_CLASS_CONFIG = "rocksdb.config.setter";
        // private static string ROCKSDB_CONFIG_SETTER_CLASS_DOC = "A Rocks DB config setter or name that : the <code>org.apache.kafka.streams.state.RocksDbConfigSetter</code> interface";

        /// <summary>
        /// {@code security.protocol}
        /// </summary>
        public static string SECURITY_PROTOCOL_CONFIG = "security.protocol";

        /// <summary>
        /// {@code send.buffer.bytes}
        /// </summary>
        public static string SEND_BUFFER_CONFIG = "send.buffer.bytes";

        /// <summary>
        /// {@code state.cleanup.delay}
        /// </summary>
        public static string STATE_CLEANUP_DELAY_MS_CONFIG = "state.cleanup.delay.ms";
        // private static string STATE_CLEANUP_DELAY_MS_DOC = "The amount of time in milliseconds to wait before deleting state when a partition has migrated. Only state directories that have not been modified for at least <code>state.cleanup.delay.ms</code> will be removed";

        /// <summary>
        /// {@code state.dir}
        /// </summary>
        public static string STATE_DIR_CONFIG = "state.dir";
        // private static string STATE_DIR_DOC = "Directory location for state store.";

        /// <summary>
        /// {@code topology.optimization}
        /// </summary>
        public static string TOPOLOGY_OPTIMIZATION = "topology.optimization";
        // private static string TOPOLOGY_OPTIMIZATION_DOC = "A configuration telling Kafka Streams if it should optimize the topology, disabled by default";

        /// <summary>
        /// {@code upgrade.from}
        /// </summary>
        public static string UPGRADE_FROM_CONFIG = "upgrade.from";
        // private static string UPGRADE_FROM_DOC = "Allows upgrading from versions 0.10.0/0.10.1/0.10.2/0.11.0/1.0/1.1 to version 1.2 (or newer) in a backward compatible way. " +
        //    "When upgrading from 1.2 to a newer version it is not required to specify this config." +
        //    "Default is null. Accepted values are \"" + UpgradeFrom0100 + "\", \"" + UpgradeFrom0101 + "\", \"" + UpgradeFrom0102 + "\", \"" + UpgradeFrom0110 + "\", \"" + UpgradeFrom10 + "\", \"" + UpgradeFrom11 + "\" (for upgrading from the corresponding old version).";

        /// <summary>
        /// {@code windowstore.changelog.Additional.retention.ms}
        /// </summary>
        public static string WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG = "windowstore.changelog.Additional.retention.ms";
        // private static string WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_DOC = "Added to a windows maintainMs to ensure data is not deleted from the log prematurely. Allows for clock drift. Default is 1 day";
    }
}
