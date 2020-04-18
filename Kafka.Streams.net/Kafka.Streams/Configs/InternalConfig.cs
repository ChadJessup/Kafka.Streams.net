namespace Kafka.Streams.Configs
{
    //private bool eosEnabled;
    //private static long DEFAULT_COMMIT_INTERVAL_MS = 30000L;
    //private static long EOS_DEFAULT_COMMIT_INTERVAL_MS = 100L;

    //public static int DUMMY_THREAD_INDEX = 1;


    //private static string[] NON_CONFIGURABLE_CONSUMER_DEFAULT_CONFIGS = new string[] { ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG };
    //private static string[] NON_CONFIGURABLE_CONSUMER_EOS_CONFIGS = new string[] { ConsumerConfig.ISOLATION_LEVEL_CONFIG };
    //private static string[] NON_CONFIGURABLE_PRODUCER_EOS_CONFIGS = new string[] {ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
    //                                                                                        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION};

    //    static
    //{

    //        CONFIG = new ConfigDef()

    // HIGH

    //            .define(ApplicationIdConfig, // required with no default value
    //                    Type.STRING,
    //                    Importance.HIGH,
    //                    APPLICATION_ID_DOC)
    //            .define(BootstrapServers, // required with no default value
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
    //            .define(DefaultKeySerdeClassConfig,
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
    //            .define(DefaultValueSerdeClassConfig,
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
    //                    in (AT_LEAST_ONCE, StreamsConfig.ExactlyOnceConfig),
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
    //            .define(PollMsConfig,
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

    //         Dictionary<string, object> tempProducerDefaultOverrides = new Dictionary<>();
    //        tempProducerDefaultOverrides.Add(ProducerConfig.LINGER_MS_CONFIG, "100");
    //        PRODUCER_DEFAULT_OVERRIDES = Collections.unmodifiableMap(tempProducerDefaultOverrides);
    //    }

    //    private static  Dictionary<string, object> PRODUCER_EOS_OVERRIDES;
    //    static
    //{

    //         Dictionary<string, object> tempProducerDefaultOverrides = new Dictionary<>(PRODUCER_DEFAULT_OVERRIDES);
    //        tempProducerDefaultOverrides.Add(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, int.MaxValue);
    //        tempProducerDefaultOverrides.Add(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

    //        PRODUCER_EOS_OVERRIDES = Collections.unmodifiableMap(tempProducerDefaultOverrides);
    //    }

    //    private static  Dictionary<string, object> CONSUMER_DEFAULT_OVERRIDES;
    //    static
    //{

    //         Dictionary<string, object> tempConsumerDefaultOverrides = new Dictionary<>();
    //        tempConsumerDefaultOverrides.Add(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
    //        tempConsumerDefaultOverrides.Add(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    //        tempConsumerDefaultOverrides.Add(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    //        tempConsumerDefaultOverrides.Add("internal.leave.group.on.Close", false);
    //        CONSUMER_DEFAULT_OVERRIDES = Collections.unmodifiableMap(tempConsumerDefaultOverrides);
    //    }

    //    private static  Dictionary<string, object> CONSUMER_EOS_OVERRIDES;
    //    static
    //{

    //         Dictionary<string, object> tempConsumerDefaultOverrides = new Dictionary<>(CONSUMER_DEFAULT_OVERRIDES);
    //        tempConsumerDefaultOverrides.Add(ConsumerConfig.ISOLATION_LEVEL_CONFIG, READ_COMMITTED.Name.toLowerCase(Locale.ROOT));
    //        CONSUMER_EOS_OVERRIDES = Collections.unmodifiableMap(tempConsumerDefaultOverrides);
    //    }

    public static class InternalConfig
    {
        public const string TASK_MANAGER_FOR_PARTITION_ASSIGNOR = "__task.manager.instance__";
        public const string ASSIGNMENT_ERROR_CODE = "__assignment.error.code__";
    }
}
