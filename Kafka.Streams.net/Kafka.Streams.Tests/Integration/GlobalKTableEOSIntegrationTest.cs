//namespace Kafka.Streams.Tests.Integration
//{
//    /*






//    *

//    *





//    */










































//    public class GlobalKTableEOSIntegrationTest
//    {
//        private const int NUM_BROKERS = 1;
//        private static StreamsConfig BROKER_CONFIG;
//    //    static {
//    //    BROKER_CONFIG = new StreamsConfig();
//    //    BROKER_CONFIG.put("transaction.state.log.replication.factor", (short) 1);
//    //    BROKER_CONFIG.put("transaction.state.log.min.isr", 1);
//    //}


//    public static EmbeddedKafkaCluster CLUSTER =
//            new EmbeddedKafkaCluster(NUM_BROKERS, BROKER_CONFIG);

//    private static volatile AtomicInteger testNo = new AtomicInteger(0);
//    private MockTime mockTime = CLUSTER.time;
//    private KeyValueMapper<string, long, long> keyMapper = (key, value) => value;
//    private ValueJoiner<long, string, string> joiner = (value1, value2) => value1 + "+" + value2;
//    private string globalStore = "globalStore";
//    private Dictionary<string, string> results = new HashMap<>();
//    private StreamsBuilder builder;
//    private StreamsConfig streamsConfiguration;
//    private KafkaStreams kafkaStreams;
//    private string globalTableTopic;
//    private string streamTopic;
//    private GlobalKTable<long, string> globalTable;
//    private KStream<string, long> stream;
//    private ForeachAction<string, string> foreachAction;


//    public void Before()
//    {// throws Exception
//        builder = new StreamsBuilder();
//        createTopics();
//        streamsConfiguration = new StreamsConfig();
//        string applicationId = "globalTableTopic-table-eos-test-" + testNo.incrementAndGet();
//        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
//        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath());
//        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
//        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
//        globalTable = builder.globalTable(globalTableTopic, Consumed.With(Serdes.Long(), Serdes.String()),
//                                          Materialized<long, string, IKeyValueStore<Bytes, byte[]>>.As(globalStore)
//                                                  .WithKeySerde(Serdes.Long())
//                                                  .withValueSerde(Serdes.String()));
//        Consumed<string, long> stringLongConsumed = Consumed.With(Serdes.String(), Serdes.Long());
//        stream = builder.Stream(streamTopic, stringLongConsumed);
//        foreachAction = results::put;
//    }


//    public void WhenShuttingDown()
//    {// throws Exception
//        if (kafkaStreams != null)
//        {
//            kafkaStreams.close();
//        }
//        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
//    }

//    [Fact]
//    public void ShouldKStreamGlobalKTableLeftJoin()
//    {// throws Exception
//        KStream<string, string> streamTableJoin = stream.leftJoin(globalTable, keyMapper, joiner);
//        streamTableJoin.ForEach(foreachAction);
//        produceInitialGlobalTableValues();
//        startStreams();
//        produceTopicValues(streamTopic);

//        Dictionary<string, string> expected = new HashMap<>();
//        expected.put("a", "1+A");
//        expected.put("b", "2+B");
//        expected.put("c", "3+C");
//        expected.put("d", "4+D");
//        expected.put("e", "5+null");

//        TestUtils.WaitForCondition(
//            () => results.equals(expected),
//            30000L,
//            "waiting for initial values");


//        produceGlobalTableValues();

//        IReadOnlyKeyValueStore<long, string> replicatedStore =
//            kafkaStreams.store(globalStore, QueryableStoreTypes.KeyValueStore());

//        TestUtils.WaitForCondition(
//            () => "J".equals(replicatedStore.Get(5L)),
//            30000,
//            "waiting for data in replicated store");

//        produceTopicValues(streamTopic);

//        expected.put("a", "1+F");
//        expected.put("b", "2+G");
//        expected.put("c", "3+H");
//        expected.put("d", "4+I");
//        expected.put("e", "5+J");

//        TestUtils.WaitForCondition(
//            () => results.equals(expected),
//            30000L,
//            "waiting for values");
//    }

//    [Fact]
//    public void ShouldKStreamGlobalKTableJoin()
//    {// throws Exception
//        KStream<string, string> streamTableJoin = stream.join(globalTable, keyMapper, joiner);
//        streamTableJoin.ForEach(foreachAction);
//        produceInitialGlobalTableValues();
//        startStreams();
//        produceTopicValues(streamTopic);

//        Dictionary<string, string> expected = new HashMap<>();
//        expected.put("a", "1+A");
//        expected.put("b", "2+B");
//        expected.put("c", "3+C");
//        expected.put("d", "4+D");

//        TestUtils.WaitForCondition(
//            () => results.equals(expected),
//            30000L,
//            "waiting for initial values");


//        produceGlobalTableValues();

//        IReadOnlyKeyValueStore<long, string> replicatedStore =
//            kafkaStreams.store(globalStore, QueryableStoreTypes.KeyValueStore());

//        TestUtils.WaitForCondition(
//            () => "J".equals(replicatedStore.Get(5L)),
//            30000,
//            "waiting for data in replicated store");

//        produceTopicValues(streamTopic);

//        expected.put("a", "1+F");
//        expected.put("b", "2+G");
//        expected.put("c", "3+H");
//        expected.put("d", "4+I");
//        expected.put("e", "5+J");

//        TestUtils.WaitForCondition(
//            () => results.equals(expected),
//            30000L,
//            "waiting for values");
//    }

//    [Fact]
//    public void ShouldRestoreTransactionalMessages()
//    {// throws Exception
//        produceInitialGlobalTableValues();

//        startStreams();

//        Dictionary<long, string> expected = new HashMap<>();
//        expected.put(1L, "A");
//        expected.put(2L, "B");
//        expected.put(3L, "C");
//        expected.put(4L, "D");

//        TestUtils.WaitForCondition(
//            () =>
//            {
//                IReadOnlyKeyValueStore<long, string> store;
//                try
//                {
//                    store = kafkaStreams.store(globalStore, QueryableStoreTypes.KeyValueStore());
//                }
//                catch (InvalidStateStoreException ex)
//                {
//                    return false;
//                }
//                Dictionary<long, string> result = new HashMap<>();
//                Iterator<KeyValuePair<long, string>> it = store.all();
//                while (it.hasNext())
//                {
//                    KeyValuePair<long, string> kv = it.MoveNext();
//                    result.put(kv.key, kv.value);
//                }
//                return result.equals(expected);
//            },
//            30000L,
//            "waiting for initial values");
//    }

//    [Fact]
//    public void ShouldNotRestoreAbortedMessages()
//    {// throws Exception
//        produceAbortedMessages();
//        produceInitialGlobalTableValues();
//        produceAbortedMessages();

//        startStreams();

//        Dictionary<long, string> expected = new HashMap<>();
//        expected.put(1L, "A");
//        expected.put(2L, "B");
//        expected.put(3L, "C");
//        expected.put(4L, "D");

//        TestUtils.WaitForCondition(
//            () =>
//            {
//                IReadOnlyKeyValueStore<long, string> store;
//                try
//                {
//                    store = kafkaStreams.store(globalStore, QueryableStoreTypes.KeyValueStore());
//                }
//                catch (InvalidStateStoreException ex)
//                {
//                    return false;
//                }
//                Dictionary<long, string> result = new HashMap<>();
//                Iterator<KeyValuePair<long, string>> it = store.all();
//                while (it.hasNext())
//                {
//                    KeyValuePair<long, string> kv = it.MoveNext();
//                    result.put(kv.key, kv.value);
//                }
//                return result.equals(expected);
//            },
//            30000L,
//            "waiting for initial values");
//    }

//    private void CreateTopics()
//    {// throws Exception
//        streamTopic = "stream-" + testNo;
//        globalTableTopic = "globalTable-" + testNo;
//        CLUSTER.createTopics(streamTopic);
//        CLUSTER.createTopic(globalTableTopic, 2, 1);
//    }

//    private void StartStreams()
//    {
//        kafkaStreams = new KafkaStreams(builder.Build(), streamsConfiguration);
//        kafkaStreams.start();
//    }

//    private void ProduceTopicValues(string topic)
//    {// throws Exception
//        IntegrationTestUtils.produceKeyValuesSynchronously(
//                topic,
//                Array.asList(
//                        KeyValuePair.Create("a", 1L),
//                        KeyValuePair.Create("b", 2L),
//                        KeyValuePair.Create("c", 3L),
//                        KeyValuePair.Create("d", 4L),
//                        KeyValuePair.Create("e", 5L)),
//                TestUtils.producerConfig(
//                        CLUSTER.bootstrapServers(),
//                        Serdes.String().Serializer,
//                        Serdes.Long().Serializer,
//                        new StreamsConfig()),
//                mockTime);
//    }

//    private void ProduceAbortedMessages()
//    {// throws Exception
//        StreamsConfig properties = new StreamsConfig();
//        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "someid");
//        properties.put(ProducerConfig.RETRIES_CONFIG, 1);
//        IntegrationTestUtils.produceAbortedKeyValuesSynchronouslyWithTimestamp(
//                globalTableTopic, Array.asList(
//                        KeyValuePair.Create(1L, "A"),
//                        KeyValuePair.Create(2L, "B"),
//                        KeyValuePair.Create(3L, "C"),
//                        KeyValuePair.Create(4L, "D")
//                        ),
//                TestUtils.producerConfig(
//                                CLUSTER.bootstrapServers(),
//                                Serdes.Long().Serializer,
//                                Serdes.String().Serializer,
//                                properties),
//                mockTime.GetCurrentInstant().ToUnixTimeMilliseconds(););
//    }

//    private void ProduceInitialGlobalTableValues()
//    {// throws Exception
//        produceInitialGlobalTableValues(true);
//    }

//    private void ProduceInitialGlobalTableValues(bool enableTransactions)
//    {// throws Exception
//        StreamsConfig properties = new StreamsConfig();
//        if (enableTransactions)
//        {
//            properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "someid");
//            properties.put(ProducerConfig.RETRIES_CONFIG, 1);
//        }
//        IntegrationTestUtils.produceKeyValuesSynchronously(
//                globalTableTopic,
//                Array.asList(
//                        KeyValuePair.Create(1L, "A"),
//                        KeyValuePair.Create(2L, "B"),
//                        KeyValuePair.Create(3L, "C"),
//                        KeyValuePair.Create(4L, "D")
//                        ),
//                TestUtils.producerConfig(
//                        CLUSTER.bootstrapServers(),
//                        Serdes.Long().Serializer,
//                        Serdes.String().Serializer,
//                        properties),
//                mockTime,
//                enableTransactions);
//    }

//    private void ProduceGlobalTableValues()
//    {// throws Exception
//        IntegrationTestUtils.produceKeyValuesSynchronously(
//                globalTableTopic,
//                Array.asList(
//                        KeyValuePair.Create(1L, "F"),
//                        KeyValuePair.Create(2L, "G"),
//                        KeyValuePair.Create(3L, "H"),
//                        KeyValuePair.Create(4L, "I"),
//                        KeyValuePair.Create(5L, "J")),
//                TestUtils.producerConfig(
//                        CLUSTER.bootstrapServers(),
//                        Serdes.Long().Serializer,
//                        Serdes.String().Serializer,
//                        new StreamsConfig()),
//                mockTime);
//    }
//}
//}
///*
