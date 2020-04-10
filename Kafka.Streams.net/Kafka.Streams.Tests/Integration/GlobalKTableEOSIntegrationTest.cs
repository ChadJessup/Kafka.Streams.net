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
//    //    BROKER_CONFIG.Put("transaction.state.log.replication.factor", (short) 1);
//    //    BROKER_CONFIG.Put("transaction.state.log.min.isr", 1);
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
//        streamsConfiguration.Put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
//        streamsConfiguration.Put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//        streamsConfiguration.Put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        streamsConfiguration.Put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath());
//        streamsConfiguration.Put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//        streamsConfiguration.Put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
//        streamsConfiguration.Put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
//        globalTable = builder.globalTable(globalTableTopic, Consumed.With(Serdes.Long(), Serdes.String()),
//                                          Materialized<long, string, IKeyValueStore<Bytes, byte[]>>.As(globalStore)
//                                                  .WithKeySerde(Serdes.Long())
//                                                  .withValueSerde(Serdes.String()));
//        Consumed<string, long> stringLongConsumed = Consumed.With(Serdes.String(), Serdes.Long());
//        stream = builder.Stream(streamTopic, stringLongConsumed);
//        foreachAction = results::Put;
//    }


//    public void WhenShuttingDown()
//    {// throws Exception
//        if (kafkaStreams != null)
//        {
//            kafkaStreams.Close();
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
//        expected.Put("a", "1+A");
//        expected.Put("b", "2+B");
//        expected.Put("c", "3+C");
//        expected.Put("d", "4+D");
//        expected.Put("e", "5+null");

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

//        expected.Put("a", "1+F");
//        expected.Put("b", "2+G");
//        expected.Put("c", "3+H");
//        expected.Put("d", "4+I");
//        expected.Put("e", "5+J");

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
//        expected.Put("a", "1+A");
//        expected.Put("b", "2+B");
//        expected.Put("c", "3+C");
//        expected.Put("d", "4+D");

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

//        expected.Put("a", "1+F");
//        expected.Put("b", "2+G");
//        expected.Put("c", "3+H");
//        expected.Put("d", "4+I");
//        expected.Put("e", "5+J");

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
//        expected.Put(1L, "A");
//        expected.Put(2L, "B");
//        expected.Put(3L, "C");
//        expected.Put(4L, "D");

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
//                Iterator<KeyValuePair<long, string>> it = store.All();
//                while (it.HasNext())
//                {
//                    KeyValuePair<long, string> kv = it.MoveNext();
//                    result.Put(kv.key, kv.value);
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
//        expected.Put(1L, "A");
//        expected.Put(2L, "B");
//        expected.Put(3L, "C");
//        expected.Put(4L, "D");

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
//                Iterator<KeyValuePair<long, string>> it = store.All();
//                while (it.HasNext())
//                {
//                    KeyValuePair<long, string> kv = it.MoveNext();
//                    result.Put(kv.key, kv.value);
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
//        properties.Put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "someid");
//        properties.Put(ProducerConfig.RETRIES_CONFIG, 1);
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
//                mockTime.NowAsEpochMilliseconds;);
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
//            properties.Put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "someid");
//            properties.Put(ProducerConfig.RETRIES_CONFIG, 1);
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
