///*






// *

// *





// */








































//using Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.State;
//using Kafka.Streams.State.Sessions;
//using Kafka.Streams.State.Window;

//public class MetricsIntegrationTest {

//    private static int NUM_BROKERS = 1;

    
//    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

//    // Metric group
//    private static string STREAM_THREAD_NODE_METRICS = "stream-metrics";
//    private static string STREAM_TASK_NODE_METRICS = "stream-task-metrics";
//    private static string STREAM_PROCESSOR_NODE_METRICS = "stream-processor-node-metrics";
//    private static string STREAM_CACHE_NODE_METRICS = "stream-record-cache-metrics";
//    private static string STREAM_STORE_IN_MEMORY_STATE_METRICS = "stream-in-memory-state-metrics";
//    private static string STREAM_STORE_IN_MEMORY_LRU_STATE_METRICS = "stream-in-memory-lru-state-metrics";
//    private static string STREAM_STORE_ROCKSDB_STATE_METRICS = "stream-rocksdb-state-metrics";
//    private static string STREAM_STORE_WINDOW_ROCKSDB_STATE_METRICS = "stream-rocksdb-window-state-metrics";
//    private static string STREAM_STORE_SESSION_ROCKSDB_STATE_METRICS = "stream-rocksdb-session-state-metrics";

//    // Metrics name
//    private static string PUT_LATENCY_AVG = "put-latency-avg";
//    private static string PUT_LATENCY_MAX = "put-latency-max";
//    private static string PUT_IF_ABSENT_LATENCY_AVG = "put-if-absent-latency-avg";
//    private static string PUT_IF_ABSENT_LATENCY_MAX = "put-if-absent-latency-max";
//    private static string GET_LATENCY_AVG = "get-latency-avg";
//    private static string GET_LATENCY_MAX = "get-latency-max";
//    private static string DELETE_LATENCY_AVG = "delete-latency-avg";
//    private static string DELETE_LATENCY_MAX = "delete-latency-max";
//    private static string PUT_ALL_LATENCY_AVG = "put-all-latency-avg";
//    private static string PUT_ALL_LATENCY_MAX = "put-all-latency-max";
//    private static string ALL_LATENCY_AVG = "all-latency-avg";
//    private static string ALL_LATENCY_MAX = "all-latency-max";
//    private static string RANGE_LATENCY_AVG = "range-latency-avg";
//    private static string RANGE_LATENCY_MAX = "range-latency-max";
//    private static string FLUSH_LATENCY_AVG = "flush-latency-avg";
//    private static string FLUSH_LATENCY_MAX = "flush-latency-max";
//    private static string RESTORE_LATENCY_AVG = "restore-latency-avg";
//    private static string RESTORE_LATENCY_MAX = "restore-latency-max";
//    private static string PUT_RATE = "put-rate";
//    private static string PUT_TOTAL = "put-total";
//    private static string PUT_IF_ABSENT_RATE = "put-if-absent-rate";
//    private static string PUT_IF_ABSENT_TOTAL = "put-if-absent-total";
//    private static string GET_RATE = "get-rate";
//    private static string DELETE_RATE = "delete-rate";
//    private static string DELETE_TOTAL = "delete-total";
//    private static string PUT_ALL_RATE = "put-all-rate";
//    private static string PUT_ALL_TOTAL = "put-all-total";
//    private static string ALL_RATE = "all-rate";
//    private static string ALL_TOTAL = "all-total";
//    private static string RANGE_RATE = "range-rate";
//    private static string RANGE_TOTAL = "range-total";
//    private static string FLUSH_RATE = "flush-rate";
//    private static string FLUSH_TOTAL = "flush-total";
//    private static string RESTORE_RATE = "restore-rate";
//    private static string RESTORE_TOTAL = "restore-total";
//    private static string PROCESS_LATENCY_AVG = "process-latency-avg";
//    private static string PROCESS_LATENCY_MAX = "process-latency-max";
//    private static string PUNCTUATE_LATENCY_AVG = "punctuate-latency-avg";
//    private static string PUNCTUATE_LATENCY_MAX = "punctuate-latency-max";
//    private static string CREATE_LATENCY_AVG = "create-latency-avg";
//    private static string CREATE_LATENCY_MAX = "create-latency-max";
//    private static string DESTROY_LATENCY_AVG = "destroy-latency-avg";
//    private static string DESTROY_LATENCY_MAX = "destroy-latency-max";
//    private static string PROCESS_RATE = "process-rate";
//    private static string PROCESS_TOTAL = "process-total";
//    private static string PUNCTUATE_RATE = "punctuate-rate";
//    private static string PUNCTUATE_TOTAL = "punctuate-total";
//    private static string CREATE_RATE = "create-rate";
//    private static string CREATE_TOTAL = "create-total";
//    private static string DESTROY_RATE = "destroy-rate";
//    private static string DESTROY_TOTAL = "destroy-total";
//    private static string FORWARD_TOTAL = "forward-total";
//    private static string STREAM_STRING = "stream";
//    private static string COMMIT_LATENCY_AVG = "commit-latency-avg";
//    private static string COMMIT_LATENCY_MAX = "commit-latency-max";
//    private static string POLL_LATENCY_AVG = "poll-latency-avg";
//    private static string POLL_LATENCY_MAX = "poll-latency-max";
//    private static string COMMIT_RATE = "commit-rate";
//    private static string COMMIT_TOTAL = "commit-total";
//    private static string POLL_RATE = "poll-rate";
//    private static string POLL_TOTAL = "poll-total";
//    private static string TASK_CREATED_RATE = "task-created-rate";
//    private static string TASK_CREATED_TOTAL = "task-created-total";
//    private static string TASK_CLOSED_RATE = "task-closed-rate";
//    private static string TASK_CLOSED_TOTAL = "task-closed-total";
//    private static string SKIPPED_RECORDS_RATE = "skipped-records-rate";
//    private static string SKIPPED_RECORDS_TOTAL = "skipped-records-total";
//    private static string RECORD_LATENESS_AVG = "record-lateness-avg";
//    private static string RECORD_LATENESS_MAX = "record-lateness-max";
//    private static string HIT_RATIO_AVG = "hitRatio-avg";
//    private static string HIT_RATIO_MIN = "hitRatio-min";
//    private static string HIT_RATIO_MAX = "hitRatio-max";

//    // stores name
//    private static string TIME_WINDOWED_AGGREGATED_STREAM_STORE = "time-windowed-aggregated-stream-store";
//    private static string SESSION_AGGREGATED_STREAM_STORE = "session-aggregated-stream-store";
//    private static string MY_STORE_IN_MEMORY = "myStoreInMemory";
//    private static string MY_STORE_PERSISTENT_KEY_VALUE = "myStorePersistentKeyValue";
//    private static string MY_STORE_LRU_MAP = "myStoreLruMap";

//    // topic names
//    private static string STREAM_INPUT = "STREAM_INPUT";
//    private static string STREAM_OUTPUT_1 = "STREAM_OUTPUT_1";
//    private static string STREAM_OUTPUT_2 = "STREAM_OUTPUT_2";
//    private static string STREAM_OUTPUT_3 = "STREAM_OUTPUT_3";
//    private static string STREAM_OUTPUT_4 = "STREAM_OUTPUT_4";

//    private StreamsBuilder builder;
//    private Properties streamsConfiguration;
//    private KafkaStreams kafkaStreams;

    
//    public void before() {// throws InterruptedException
//        builder = new StreamsBuilder();
//        CLUSTER.createTopics(STREAM_INPUT, STREAM_OUTPUT_1, STREAM_OUTPUT_2, STREAM_OUTPUT_3, STREAM_OUTPUT_4);
//        streamsConfiguration = new Properties();
//        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-metrics-test");
//        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Int().getClass());
//        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        streamsConfiguration.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.name);
//        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
//        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
//    }

    
//    public void after() {// throws InterruptedException
//        CLUSTER.deleteTopics(STREAM_INPUT, STREAM_OUTPUT_1, STREAM_OUTPUT_2, STREAM_OUTPUT_3, STREAM_OUTPUT_4);
//    }

//    private void startApplication() {// throws InterruptedException
//        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
//        kafkaStreams.start();
//        long timeout = 60000;
//        TestUtils.waitForCondition(
//            () => kafkaStreams.state() == State.RUNNING,
//            timeout,
//            () => "Kafka Streams application did not reach state RUNNING in " + timeout + " ms");
//    }

//    private void closeApplication() {// throws Exception
//        kafkaStreams.close();
//        kafkaStreams.cleanUp();
//        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
//        long timeout = 60000;
//        TestUtils.waitForCondition(
//            () => kafkaStreams.state() == State.NOT_RUNNING,
//            timeout,
//            () => "Kafka Streams application did not reach state NOT_RUNNING in " + timeout + " ms");
//    }

//    [Xunit.Fact]
//    public void shouldAddMetricsOnAllLevels() {// throws Exception
//        builder.stream(STREAM_INPUT, Consumed.with(Serdes.Int(), Serdes.String()))
//            .to(STREAM_OUTPUT_1, Produced.with(Serdes.Int(), Serdes.String()));
//        builder.table(STREAM_OUTPUT_1,
//                      Materialized.As(Stores.inMemoryKeyValueStore(MY_STORE_IN_MEMORY)).withCachingEnabled())
//            .toStream()
//            .to(STREAM_OUTPUT_2);
//        builder.table(STREAM_OUTPUT_2,
//                      Materialized.As(Stores.persistentKeyValueStore(MY_STORE_PERSISTENT_KEY_VALUE)).withCachingEnabled())
//            .toStream()
//            .to(STREAM_OUTPUT_3);
//        builder.table(STREAM_OUTPUT_3,
//                      Materialized.As(Stores.lruMap(MY_STORE_LRU_MAP, 10000)).withCachingEnabled())
//            .toStream()
//            .to(STREAM_OUTPUT_4);
//        startApplication();

//        checkThreadLevelMetrics();
//        checkTaskLevelMetrics();
//        checkProcessorLevelMetrics();
//        checkKeyValueStoreMetricsByType(STREAM_STORE_IN_MEMORY_STATE_METRICS);
//        checkKeyValueStoreMetricsByType(STREAM_STORE_IN_MEMORY_LRU_STATE_METRICS);
//        checkKeyValueStoreMetricsByType(STREAM_STORE_ROCKSDB_STATE_METRICS);
//        checkCacheMetrics();

//        closeApplication();

//        checkMetricsDeregistration();
//    }

//    [Xunit.Fact]
//    public void shouldAddMetricsForWindowStore() {// throws Exception
//        builder.stream(STREAM_INPUT, Consumed.with(Serdes.Int(), Serdes.String()))
//            .groupByKey()
//            .windowedBy(TimeWindows.of(Duration.ofMillis(50)))
//            .aggregate(() => 0L,
//                (aggKey, newValue, aggValue) => aggValue,
//                Materialized<int, long, IWindowStore<Bytes, byte[]>>.As(TIME_WINDOWED_AGGREGATED_STREAM_STORE)
//                    .withValueSerde(Serdes.Long()));
//        startApplication();

//        checkWindowStoreMetrics();

//        closeApplication();

//        checkMetricsDeregistration();
//    }

//    [Xunit.Fact]
//    public void shouldAddMetricsForSessionStore() {// throws Exception
//        builder.stream(STREAM_INPUT, Consumed.with(Serdes.Int(), Serdes.String()))
//            .groupByKey()
//            .windowedBy(SessionWindows.with(Duration.ofMillis(50)))
//            .aggregate(() => 0L,
//                (aggKey, newValue, aggValue) => aggValue,
//                (aggKey, leftAggValue, rightAggValue) => leftAggValue,
//                Materialized<int, long, ISessionStore<Bytes, byte[]>>.As(SESSION_AGGREGATED_STREAM_STORE)
//                    .withValueSerde(Serdes.Long()));
//        startApplication();

//        checkSessionStoreMetrics();

//        closeApplication();

//        checkMetricsDeregistration();
//    }

//    // private void checkThreadLevelMetrics() {
//    //     List<Metric> listMetricThread = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
//    //         .filter(m => m.metricName().group().equals(STREAM_THREAD_NODE_METRICS))
//    //         .collect(Collectors.toList());
//    //     checkMetricByName(listMetricThread, COMMIT_LATENCY_AVG, 1);
//    //     checkMetricByName(listMetricThread, COMMIT_LATENCY_MAX, 1);
//    //     checkMetricByName(listMetricThread, POLL_LATENCY_AVG, 1);
//    //     checkMetricByName(listMetricThread, POLL_LATENCY_MAX, 1);
//    //     checkMetricByName(listMetricThread, PROCESS_LATENCY_AVG, 1);
//    //     checkMetricByName(listMetricThread, PROCESS_LATENCY_MAX, 1);
//    //     checkMetricByName(listMetricThread, PUNCTUATE_LATENCY_AVG, 1);
//    //     checkMetricByName(listMetricThread, PUNCTUATE_LATENCY_MAX, 1);
//    //     checkMetricByName(listMetricThread, COMMIT_RATE, 1);
//    //     checkMetricByName(listMetricThread, COMMIT_TOTAL, 1);
//    //     checkMetricByName(listMetricThread, POLL_RATE, 1);
//    //     checkMetricByName(listMetricThread, POLL_TOTAL, 1);
//    //     checkMetricByName(listMetricThread, PROCESS_RATE, 1);
//    //     checkMetricByName(listMetricThread, PROCESS_TOTAL, 1);
//    //     checkMetricByName(listMetricThread, PUNCTUATE_RATE, 1);
//    //     checkMetricByName(listMetricThread, PUNCTUATE_TOTAL, 1);
//    //     checkMetricByName(listMetricThread, TASK_CREATED_RATE, 1);
//    //     checkMetricByName(listMetricThread, TASK_CREATED_TOTAL, 1);
//    //     checkMetricByName(listMetricThread, TASK_CLOSED_RATE, 1);
//    //     checkMetricByName(listMetricThread, TASK_CLOSED_TOTAL, 1);
//    //     checkMetricByName(listMetricThread, SKIPPED_RECORDS_RATE, 1);
//    //     checkMetricByName(listMetricThread, SKIPPED_RECORDS_TOTAL, 1);
//    // }

//    private void checkTaskLevelMetrics() {
//        List<Metric> listMetricTask = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
//            .filter(m => m.metricName().group().equals(STREAM_TASK_NODE_METRICS))
//            .collect(Collectors.toList());
//        checkMetricByName(listMetricTask, COMMIT_LATENCY_AVG, 5);
//        checkMetricByName(listMetricTask, COMMIT_LATENCY_MAX, 5);
//        checkMetricByName(listMetricTask, COMMIT_RATE, 5);
//        checkMetricByName(listMetricTask, COMMIT_TOTAL, 5);
//        checkMetricByName(listMetricTask, RECORD_LATENESS_AVG, 4);
//        checkMetricByName(listMetricTask, RECORD_LATENESS_MAX, 4);
//    }

//    private void checkProcessorLevelMetrics() {
//        List<Metric> listMetricProcessor = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
//            .filter(m => m.metricName().group().equals(STREAM_PROCESSOR_NODE_METRICS))
//            .collect(Collectors.toList());
//        checkMetricByName(listMetricProcessor, PROCESS_LATENCY_AVG, 18);
//        checkMetricByName(listMetricProcessor, PROCESS_LATENCY_MAX, 18);
//        checkMetricByName(listMetricProcessor, PUNCTUATE_LATENCY_AVG, 18);
//        checkMetricByName(listMetricProcessor, PUNCTUATE_LATENCY_MAX, 18);
//        checkMetricByName(listMetricProcessor, CREATE_LATENCY_AVG, 18);
//        checkMetricByName(listMetricProcessor, CREATE_LATENCY_MAX, 18);
//        checkMetricByName(listMetricProcessor, DESTROY_LATENCY_AVG, 18);
//        checkMetricByName(listMetricProcessor, DESTROY_LATENCY_MAX, 18);
//        checkMetricByName(listMetricProcessor, PROCESS_RATE, 18);
//        checkMetricByName(listMetricProcessor, PROCESS_TOTAL, 18);
//        checkMetricByName(listMetricProcessor, PUNCTUATE_RATE, 18);
//        checkMetricByName(listMetricProcessor, PUNCTUATE_TOTAL, 18);
//        checkMetricByName(listMetricProcessor, CREATE_RATE, 18);
//        checkMetricByName(listMetricProcessor, CREATE_TOTAL, 18);
//        checkMetricByName(listMetricProcessor, DESTROY_RATE, 18);
//        checkMetricByName(listMetricProcessor, DESTROY_TOTAL, 18);
//        checkMetricByName(listMetricProcessor, FORWARD_TOTAL, 18);
//    }

//    private void checkKeyValueStoreMetricsByType(string storeType) {
//        List<Metric> listMetricStore = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
//            .filter(m => m.metricName().group().equals(storeType))
//            .collect(Collectors.toList());
//        checkMetricByName(listMetricStore, PUT_LATENCY_AVG, 2);
//        checkMetricByName(listMetricStore, PUT_LATENCY_MAX, 2);
//        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_AVG, 2);
//        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_MAX, 2);
//        checkMetricByName(listMetricStore, GET_LATENCY_AVG, 2);
//        checkMetricByName(listMetricStore, GET_LATENCY_MAX, 2);
//        checkMetricByName(listMetricStore, DELETE_LATENCY_AVG, 2);
//        checkMetricByName(listMetricStore, DELETE_LATENCY_MAX, 2);
//        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_AVG, 2);
//        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_MAX, 2);
//        checkMetricByName(listMetricStore, ALL_LATENCY_AVG, 2);
//        checkMetricByName(listMetricStore, ALL_LATENCY_MAX, 2);
//        checkMetricByName(listMetricStore, RANGE_LATENCY_AVG, 2);
//        checkMetricByName(listMetricStore, RANGE_LATENCY_MAX, 2);
//        checkMetricByName(listMetricStore, FLUSH_LATENCY_AVG, 2);
//        checkMetricByName(listMetricStore, FLUSH_LATENCY_MAX, 2);
//        checkMetricByName(listMetricStore, RESTORE_LATENCY_AVG, 2);
//        checkMetricByName(listMetricStore, RESTORE_LATENCY_MAX, 2);
//        checkMetricByName(listMetricStore, PUT_RATE, 2);
//        checkMetricByName(listMetricStore, PUT_TOTAL, 2);
//        checkMetricByName(listMetricStore, PUT_IF_ABSENT_RATE, 2);
//        checkMetricByName(listMetricStore, PUT_IF_ABSENT_TOTAL, 2);
//        checkMetricByName(listMetricStore, GET_RATE, 2);
//        checkMetricByName(listMetricStore, DELETE_RATE, 2);
//        checkMetricByName(listMetricStore, DELETE_TOTAL, 2);
//        checkMetricByName(listMetricStore, PUT_ALL_RATE, 2);
//        checkMetricByName(listMetricStore, PUT_ALL_TOTAL, 2);
//        checkMetricByName(listMetricStore, ALL_RATE, 2);
//        checkMetricByName(listMetricStore, ALL_TOTAL, 2);
//        checkMetricByName(listMetricStore, RANGE_RATE, 2);
//        checkMetricByName(listMetricStore, RANGE_TOTAL, 2);
//        checkMetricByName(listMetricStore, FLUSH_RATE, 2);
//        checkMetricByName(listMetricStore, FLUSH_TOTAL, 2);
//        checkMetricByName(listMetricStore, RESTORE_RATE, 2);
//        checkMetricByName(listMetricStore, RESTORE_TOTAL, 2);
//    }

//    private void checkMetricsDeregistration() {
//        List<Metric> listMetricAfterClosingApp = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
//            .filter(m => m.metricName().group().Contains(STREAM_STRING))
//            .collect(Collectors.toList());
//        Assert.Equal(listMetricAfterClosingApp.Count, is(0));
//    }

//    private void checkCacheMetrics() {
//        List<Metric> listMetricCache = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
//            .filter(m => m.metricName().group().equals(STREAM_CACHE_NODE_METRICS))
//            .collect(Collectors.toList());
//        checkMetricByName(listMetricCache, HIT_RATIO_AVG, 6);
//        checkMetricByName(listMetricCache, HIT_RATIO_MIN, 6);
//        checkMetricByName(listMetricCache, HIT_RATIO_MAX, 6);
//    }

//    private void checkWindowStoreMetrics() {
//        List<Metric> listMetricStore = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
//            .filter(m => m.metricName().group().equals(STREAM_STORE_WINDOW_ROCKSDB_STATE_METRICS))
//            .collect(Collectors.toList());
//        checkMetricByName(listMetricStore, PUT_LATENCY_AVG, 2);
//        checkMetricByName(listMetricStore, PUT_LATENCY_MAX, 2);
//        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_AVG, 0);
//        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_MAX, 0);
//        checkMetricByName(listMetricStore, GET_LATENCY_AVG, 0);
//        checkMetricByName(listMetricStore, GET_LATENCY_MAX, 0);
//        checkMetricByName(listMetricStore, DELETE_LATENCY_AVG, 0);
//        checkMetricByName(listMetricStore, DELETE_LATENCY_MAX, 0);
//        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_AVG, 0);
//        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_MAX, 0);
//        checkMetricByName(listMetricStore, ALL_LATENCY_AVG, 0);
//        checkMetricByName(listMetricStore, ALL_LATENCY_MAX, 0);
//        checkMetricByName(listMetricStore, RANGE_LATENCY_AVG, 0);
//        checkMetricByName(listMetricStore, RANGE_LATENCY_MAX, 0);
//        checkMetricByName(listMetricStore, FLUSH_LATENCY_AVG, 2);
//        checkMetricByName(listMetricStore, FLUSH_LATENCY_MAX, 2);
//        checkMetricByName(listMetricStore, RESTORE_LATENCY_AVG, 2);
//        checkMetricByName(listMetricStore, RESTORE_LATENCY_MAX, 2);
//        checkMetricByName(listMetricStore, PUT_RATE, 2);
//        checkMetricByName(listMetricStore, PUT_TOTAL, 2);
//        checkMetricByName(listMetricStore, PUT_IF_ABSENT_RATE, 0);
//        checkMetricByName(listMetricStore, PUT_IF_ABSENT_TOTAL, 0);
//        checkMetricByName(listMetricStore, GET_RATE, 0);
//        checkMetricByName(listMetricStore, DELETE_RATE, 0);
//        checkMetricByName(listMetricStore, DELETE_TOTAL, 0);
//        checkMetricByName(listMetricStore, PUT_ALL_RATE, 0);
//        checkMetricByName(listMetricStore, PUT_ALL_TOTAL, 0);
//        checkMetricByName(listMetricStore, ALL_RATE, 0);
//        checkMetricByName(listMetricStore, ALL_TOTAL, 0);
//        checkMetricByName(listMetricStore, RANGE_RATE, 0);
//        checkMetricByName(listMetricStore, RANGE_TOTAL, 0);
//        checkMetricByName(listMetricStore, FLUSH_RATE, 2);
//        checkMetricByName(listMetricStore, FLUSH_TOTAL, 2);
//        checkMetricByName(listMetricStore, RESTORE_RATE, 2);
//        checkMetricByName(listMetricStore, RESTORE_TOTAL, 2);
//    }

//    private void checkSessionStoreMetrics() {
//        List<Metric> listMetricStore = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
//            .filter(m => m.metricName().group().equals(STREAM_STORE_SESSION_ROCKSDB_STATE_METRICS))
//            .collect(Collectors.toList());
//        checkMetricByName(listMetricStore, PUT_LATENCY_AVG, 2);
//        checkMetricByName(listMetricStore, PUT_LATENCY_MAX, 2);
//        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_AVG, 0);
//        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_MAX, 0);
//        checkMetricByName(listMetricStore, GET_LATENCY_AVG, 0);
//        checkMetricByName(listMetricStore, GET_LATENCY_MAX, 0);
//        checkMetricByName(listMetricStore, DELETE_LATENCY_AVG, 0);
//        checkMetricByName(listMetricStore, DELETE_LATENCY_MAX, 0);
//        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_AVG, 0);
//        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_MAX, 0);
//        checkMetricByName(listMetricStore, ALL_LATENCY_AVG, 0);
//        checkMetricByName(listMetricStore, ALL_LATENCY_MAX, 0);
//        checkMetricByName(listMetricStore, RANGE_LATENCY_AVG, 0);
//        checkMetricByName(listMetricStore, RANGE_LATENCY_MAX, 0);
//        checkMetricByName(listMetricStore, FLUSH_LATENCY_AVG, 2);
//        checkMetricByName(listMetricStore, FLUSH_LATENCY_MAX, 2);
//        checkMetricByName(listMetricStore, RESTORE_LATENCY_AVG, 2);
//        checkMetricByName(listMetricStore, RESTORE_LATENCY_MAX, 2);
//        checkMetricByName(listMetricStore, PUT_RATE, 2);
//        checkMetricByName(listMetricStore, PUT_TOTAL, 2);
//        checkMetricByName(listMetricStore, PUT_IF_ABSENT_RATE, 0);
//        checkMetricByName(listMetricStore, PUT_IF_ABSENT_TOTAL, 0);
//        checkMetricByName(listMetricStore, GET_RATE, 0);
//        checkMetricByName(listMetricStore, DELETE_RATE, 0);
//        checkMetricByName(listMetricStore, DELETE_TOTAL, 0);
//        checkMetricByName(listMetricStore, PUT_ALL_RATE, 0);
//        checkMetricByName(listMetricStore, PUT_ALL_TOTAL, 0);
//        checkMetricByName(listMetricStore, ALL_RATE, 0);
//        checkMetricByName(listMetricStore, ALL_TOTAL, 0);
//        checkMetricByName(listMetricStore, RANGE_RATE, 0);
//        checkMetricByName(listMetricStore, RANGE_TOTAL, 0);
//        checkMetricByName(listMetricStore, FLUSH_RATE, 2);
//        checkMetricByName(listMetricStore, FLUSH_TOTAL, 2);
//        checkMetricByName(listMetricStore, RESTORE_RATE, 2);
//        checkMetricByName(listMetricStore, RESTORE_TOTAL, 2);
//    }

//    private void checkMetricByName(List<Metric> listMetric, string metricName, int numMetric) {
//        List<Metric> metrics = listMetric.stream()
//            .filter(m => m.metricName().name().equals(metricName))
//            .collect(Collectors.toList());
//        Assert.Equal("Size of metrics of type:'" + metricName + "' must be equal to:" + numMetric + " but it's equal to " + metrics.Count, numMetric, metrics.Count);
//        foreach (Metric m in metrics) {
//            Assert.assertNotNull("Metric:'" + m.metricName() + "' must be not null", m.metricValue());
//        }
//    }
//}