/*






 *

 *





 */












































public class GlobalKTableIntegrationTest {
    private static int NUM_BROKERS = 1;

    
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private static volatile AtomicInteger testNo = new AtomicInteger(0);
    private MockTime mockTime = CLUSTER.time;
    private KeyValueMapper<string, long, long> keyMapper = (key, value) => value;
    private ValueJoiner<long, string, string> joiner = (value1, value2) => value1 + "+" + value2;
    private string globalStore = "globalStore";
    private StreamsBuilder builder;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;
    private string globalTableTopic;
    private string streamTopic;
    private GlobalKTable<long, string> globalTable;
    private KStream<string, long> stream;
    private MockProcessorSupplier<string, string> supplier;

    
    public void Before() {// throws Exception
        builder = new StreamsBuilder();
        CreateTopics();
        streamsConfiguration = new Properties();
        string applicationId = "globalTableTopic-table-test-" + testNo.incrementAndGet();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        globalTable = builder.globalTable(globalTableTopic, Consumed.with(Serdes.Long(), Serdes.String()),
                                          Materialized<long, string, KeyValueStore<Bytes, byte[]>>.As(globalStore)
                                                  .withKeySerde(Serdes.Long())
                                                  .withValueSerde(Serdes.String()));
        Consumed<string, long> stringLongConsumed = Consumed.with(Serdes.String(), Serdes.Long());
        stream = builder.stream(streamTopic, stringLongConsumed);
        supplier = new MockProcessorSupplier<>();
    }

    
    public void WhenShuttingDown() {// throws Exception
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    [Xunit.Fact]
    public void ShouldKStreamGlobalKTableLeftJoin() {// throws Exception
        KStream<string, string> streamTableJoin = stream.leftJoin(globalTable, keyMapper, joiner);
        streamTableJoin.process(supplier);
        ProduceInitialGlobalTableValues();
        StartStreams();
        long firstTimestamp = mockTime.milliseconds();
        ProduceTopicValues(streamTopic);

        Dictionary<string, ValueAndTimestamp<string>> expected = new HashMap<>();
        expected.put("a", ValueAndTimestamp.make("1+A", firstTimestamp));
        expected.put("b", ValueAndTimestamp.make("2+B", firstTimestamp + 1L));
        expected.put("c", ValueAndTimestamp.make("3+C", firstTimestamp + 2L));
        expected.put("d", ValueAndTimestamp.make("4+D", firstTimestamp + 3L));
        expected.put("e", ValueAndTimestamp.make("5+null", firstTimestamp + 4L));

        TestUtils.waitForCondition(
            () => {
                if (supplier.capturedProcessorsCount() < 2) {
                    return false;
                }
                Dictionary<string, ValueAndTimestamp<string>> result = new HashMap<>();
                result.putAll(supplier.capturedProcessors(2).get(0).lastValueAndTimestampPerKey);
                result.putAll(supplier.capturedProcessors(2).get(1).lastValueAndTimestampPerKey);
                return result.equals(expected);
            },
            30000L,
            "waiting for initial values");


        firstTimestamp = mockTime.milliseconds();
        ProduceGlobalTableValues();

        ReadOnlyKeyValueStore<long, string> replicatedStore =
            kafkaStreams.store(globalStore, QueryableStoreTypes.keyValueStore());

        TestUtils.waitForCondition(
            () => "J".equals(replicatedStore.get(5L)),
            30000,
            "waiting for data in replicated store");

        ReadOnlyKeyValueStore<long, ValueAndTimestamp<string>> replicatedStoreWithTimestamp =
            kafkaStreams.store(globalStore, QueryableStoreTypes.timestampedKeyValueStore());
        Assert.Equal(replicatedStoreWithTimestamp.get(5L), (ValueAndTimestamp.make("J", firstTimestamp + 4L)));

        firstTimestamp = mockTime.milliseconds();
        ProduceTopicValues(streamTopic);

        expected.put("a", ValueAndTimestamp.make("1+F", firstTimestamp));
        expected.put("b", ValueAndTimestamp.make("2+G", firstTimestamp + 1L));
        expected.put("c", ValueAndTimestamp.make("3+H", firstTimestamp + 2L));
        expected.put("d", ValueAndTimestamp.make("4+I", firstTimestamp + 3L));
        expected.put("e", ValueAndTimestamp.make("5+J", firstTimestamp + 4L));

        TestUtils.waitForCondition(
            () => {
                if (supplier.capturedProcessorsCount() < 2) {
                    return false;
                }
                Dictionary<string, ValueAndTimestamp<string>> result = new HashMap<>();
                result.putAll(supplier.capturedProcessors(2).get(0).lastValueAndTimestampPerKey);
                result.putAll(supplier.capturedProcessors(2).get(1).lastValueAndTimestampPerKey);
                return result.equals(expected);
            },
            30000L,
            "waiting for values");
    }

    [Xunit.Fact]
    public void ShouldKStreamGlobalKTableJoin() {// throws Exception
        KStream<string, string> streamTableJoin = stream.join(globalTable, keyMapper, joiner);
        streamTableJoin.process(supplier);
        ProduceInitialGlobalTableValues();
        StartStreams();
        long firstTimestamp = mockTime.milliseconds();
        ProduceTopicValues(streamTopic);

        Dictionary<string, ValueAndTimestamp<string>> expected = new HashMap<>();
        expected.put("a", ValueAndTimestamp.make("1+A", firstTimestamp));
        expected.put("b", ValueAndTimestamp.make("2+B", firstTimestamp + 1L));
        expected.put("c", ValueAndTimestamp.make("3+C", firstTimestamp + 2L));
        expected.put("d", ValueAndTimestamp.make("4+D", firstTimestamp + 3L));

        TestUtils.waitForCondition(
            () => {
                if (supplier.capturedProcessorsCount() < 2) {
                    return false;
                }
                Dictionary<string, ValueAndTimestamp<string>> result = new HashMap<>();
                result.putAll(supplier.capturedProcessors(2).get(0).lastValueAndTimestampPerKey);
                result.putAll(supplier.capturedProcessors(2).get(1).lastValueAndTimestampPerKey);
                return result.equals(expected);
            },
            30000L,
            "waiting for initial values");


        firstTimestamp = mockTime.milliseconds();
        ProduceGlobalTableValues();

        ReadOnlyKeyValueStore<long, string> replicatedStore =
            kafkaStreams.store(globalStore, QueryableStoreTypes.keyValueStore());

        TestUtils.waitForCondition(
            () => "J".equals(replicatedStore.get(5L)),
            30000,
            "waiting for data in replicated store");

        ReadOnlyKeyValueStore<long, ValueAndTimestamp<string>> replicatedStoreWithTimestamp =
            kafkaStreams.store(globalStore, QueryableStoreTypes.timestampedKeyValueStore());
        Assert.Equal(replicatedStoreWithTimestamp.get(5L), (ValueAndTimestamp.make("J", firstTimestamp + 4L)));

        firstTimestamp = mockTime.milliseconds();
        ProduceTopicValues(streamTopic);

        expected.put("a", ValueAndTimestamp.make("1+F", firstTimestamp));
        expected.put("b", ValueAndTimestamp.make("2+G", firstTimestamp + 1L));
        expected.put("c", ValueAndTimestamp.make("3+H", firstTimestamp + 2L));
        expected.put("d", ValueAndTimestamp.make("4+I", firstTimestamp + 3L));
        expected.put("e", ValueAndTimestamp.make("5+J", firstTimestamp + 4L));

        TestUtils.waitForCondition(
            () => {
                if (supplier.capturedProcessorsCount() < 2) {
                    return false;
                }
                Dictionary<string, ValueAndTimestamp<string>> result = new HashMap<>();
                result.putAll(supplier.capturedProcessors(2).get(0).lastValueAndTimestampPerKey);
                result.putAll(supplier.capturedProcessors(2).get(1).lastValueAndTimestampPerKey);
                return result.equals(expected);
            },
            30000L,
            "waiting for values");
    }

    [Xunit.Fact]
    public void ShouldRestoreGlobalInMemoryKTableOnRestart() {// throws Exception
        builder = new StreamsBuilder();
        globalTable = builder.globalTable(
            globalTableTopic,
            Consumed.with(Serdes.Long(), Serdes.String()),
            Materialized.As(Stores.inMemoryKeyValueStore(globalStore)));

        ProduceInitialGlobalTableValues();

        StartStreams();
        ReadOnlyKeyValueStore<long, string> store = kafkaStreams.store(globalStore, QueryableStoreTypes.keyValueStore());
        Assert.Equal(store.approximateNumEntries(), (4L));
        ReadOnlyKeyValueStore<long, ValueAndTimestamp<string>> timestampedStore =
            kafkaStreams.store(globalStore, QueryableStoreTypes.timestampedKeyValueStore());
        Assert.Equal(timestampedStore.approximateNumEntries(), (4L));
        kafkaStreams.close();

        StartStreams();
        store = kafkaStreams.store(globalStore, QueryableStoreTypes.keyValueStore());
        Assert.Equal(store.approximateNumEntries(), (4L));
        timestampedStore = kafkaStreams.store(globalStore, QueryableStoreTypes.timestampedKeyValueStore());
        Assert.Equal(timestampedStore.approximateNumEntries(), (4L));
    }

    private void CreateTopics() {// throws Exception
        streamTopic = "stream-" + testNo;
        globalTableTopic = "globalTable-" + testNo;
        CLUSTER.createTopics(streamTopic);
        CLUSTER.createTopic(globalTableTopic, 2, 1);
    }
    
    private void StartStreams() {
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();
    }

    private void ProduceTopicValues(string topic) {// throws Exception
        IntegrationTestUtils.produceKeyValuesSynchronously(
                topic,
                Array.asList(
                        new KeyValuePair<>("a", 1L),
                        new KeyValuePair<>("b", 2L),
                        new KeyValuePair<>("c", 3L),
                        new KeyValuePair<>("d", 4L),
                        new KeyValuePair<>("e", 5L)),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        StringSerializer,
                        LongSerializer,
                        new Properties()),
                mockTime);
    }

    private void ProduceInitialGlobalTableValues() {// throws Exception
        IntegrationTestUtils.produceKeyValuesSynchronously(
                globalTableTopic,
                Array.asList(
                        new KeyValuePair<>(1L, "A"),
                        new KeyValuePair<>(2L, "B"),
                        new KeyValuePair<>(3L, "C"),
                        new KeyValuePair<>(4L, "D")
                        ),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        LongSerializer,
                        StringSerializer
                        ),
                mockTime);
    }

    private void ProduceGlobalTableValues() {// throws Exception
        IntegrationTestUtils.produceKeyValuesSynchronously(
                globalTableTopic,
                Array.asList(
                        new KeyValuePair<>(1L, "F"),
                        new KeyValuePair<>(2L, "G"),
                        new KeyValuePair<>(3L, "H"),
                        new KeyValuePair<>(4L, "I"),
                        new KeyValuePair<>(5L, "J")),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        LongSerializer,
                        StringSerializer,
                        new Properties()),
                mockTime);
    }
}
