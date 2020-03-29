/*






 *

 *





 */







































public class RepartitionWithMergeOptimizingIntegrationTest {

    private static int NUM_BROKERS = 1;
    private static string INPUT_A_TOPIC = "inputA";
    private static string INPUT_B_TOPIC = "inputB";
    private static string COUNT_TOPIC = "outputTopic_0";
    private static string COUNT_STRING_TOPIC = "outputTopic_1";


    private static int ONE_REPARTITION_TOPIC = 1;
    private static int TWO_REPARTITION_TOPICS = 2;

    private Pattern repartitionTopicPattern = new Regex("Sink: .*-repartition", RegexOptions.Compiled);

    private Properties streamsConfiguration;


    
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private MockTime mockTime = CLUSTER.time;

    
    public void SetUp() {// throws Exception
        Properties props = new Properties();
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 1024 * 10);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);

        streamsConfiguration = StreamsTestUtils.getStreamsConfig(
            "maybe-optimized-with-merge-test-app",
            CLUSTER.bootstrapServers(),
            Serdes.String().getClass().getName(),
            Serdes.String().getClass().getName(),
            props);

        CLUSTER.createTopics(COUNT_TOPIC,
                             COUNT_STRING_TOPIC,
                             INPUT_A_TOPIC,
                             INPUT_B_TOPIC);

        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    
    public void TearDown() {// throws Exception
        CLUSTER.deleteAllTopicsAndWait(30_000L);
    }

    [Xunit.Fact]
    public void ShouldSendCorrectRecords_OPTIMIZED() {// throws Exception
        runIntegrationTest(StreamsConfig.OPTIMIZE,
                           ONE_REPARTITION_TOPIC);
    }

    [Xunit.Fact]
    public void ShouldSendCorrectResults_NO_OPTIMIZATION() {// throws Exception
        runIntegrationTest(StreamsConfig.NO_OPTIMIZATION,
                           TWO_REPARTITION_TOPICS);
    }


    private void RunIntegrationTest(string optimizationConfig,
                                    int expectedNumberRepartitionTopics) {// throws Exception


        StreamsBuilder builder = new StreamsBuilder();

        KStream<string, string> sourceAStream = builder.stream(INPUT_A_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<string, string> sourceBStream = builder.stream(INPUT_B_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<string, string> mappedAStream = sourceAStream.map((k, v) => KeyValuePair.Create(v.split(":")[0], v));
        KStream<string, string> mappedBStream = sourceBStream.map((k, v) => KeyValuePair.Create(v.split(":")[0], v));

        KStream<string, string> mergedStream = mappedAStream.merge(mappedBStream);

        mergedStream.groupByKey().count().toStream().to(COUNT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        mergedStream.groupByKey().count().toStream().mapValues(v => v.toString()).to(COUNT_STRING_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        streamsConfiguration.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, optimizationConfig);

        Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer, StringSerializer);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_A_TOPIC, GetKeyValues(), producerConfig, mockTime);
        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_B_TOPIC, GetKeyValues(), producerConfig, mockTime);

        Properties consumerConfig1 = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer, LongDeserializer);
        Properties consumerConfig2 = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer, StringDeserializer);

        Topology topology = builder.build(streamsConfiguration);
        string topologyString = topology.describe().toString();
        System.Console.Out.WriteLine(topologyString);

        if (optimizationConfig.equals(StreamsConfig.OPTIMIZE)) {
            Assert.Equal(EXPECTED_OPTIMIZED_TOPOLOGY, topologyString);
        } else {
            Assert.Equal(EXPECTED_UNOPTIMIZED_TOPOLOGY, topologyString);
        }


        /*
           confirming number of expected repartition topics here
         */
        Assert.Equal(expectedNumberRepartitionTopics, GetCountOfRepartitionTopicsFound(topologyString));

        KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        streams.start();

        List<KeyValuePair<string, long>> expectedCountKeyValues = Array.asList(KeyValuePair.Create("A", 6L), KeyValuePair.Create("B", 6L), KeyValuePair.Create("C", 6L));
        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig1, COUNT_TOPIC, expectedCountKeyValues);

        List<KeyValuePair<string, string>> expectedStringCountKeyValues = Array.asList(KeyValuePair.Create("A", "6"), KeyValuePair.Create("B", "6"), KeyValuePair.Create("C", "6"));
        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig2, COUNT_STRING_TOPIC, expectedStringCountKeyValues);

        streams.close(Duration.ofSeconds(5));
    }


    private int GetCountOfRepartitionTopicsFound(string topologyString) {
        Matcher matcher = repartitionTopicPattern.matcher(topologyString);
        List<string> repartitionTopicsFound = new ArrayList<>();
        while (matcher.find()) {
            repartitionTopicsFound.add(matcher.group());
        }
        return repartitionTopicsFound.Count;
    }


    private List<KeyValuePair<string, string>> GetKeyValues() {
        List<KeyValuePair<string, string>> keyValueList = new ArrayList<>();
        string[] keys = new string[]{"X", "Y", "Z"};
        string[] values = new string[]{"A:foo", "B:foo", "C:foo"};
        foreach (string key in keys) {
            foreach (string value in values) {
                keyValueList.add(KeyValuePair.Create(key, value));
            }
        }
        return keyValueList;
    }



    private static string EXPECTED_OPTIMIZED_TOPOLOGY = "Topologies:\n"
                                                              + "   Sub-topology: 0\n"
                                                              + "    Source: KSTREAM-SOURCE-0000000000 (topics: [inputA])\n"
                                                              + "      -=> KSTREAM-MAP-0000000002\n"
                                                              + "    Source: KSTREAM-SOURCE-0000000001 (topics: [inputB])\n"
                                                              + "      -=> KSTREAM-MAP-0000000003\n"
                                                              + "    Processor: KSTREAM-MAP-0000000002 (stores: [])\n"
                                                              + "      -=> KSTREAM-MERGE-0000000004\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000000\n"
                                                              + "    Processor: KSTREAM-MAP-0000000003 (stores: [])\n"
                                                              + "      -=> KSTREAM-MERGE-0000000004\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000001\n"
                                                              + "    Processor: KSTREAM-MERGE-0000000004 (stores: [])\n"
                                                              + "      -=> KSTREAM-FILTER-0000000021\n"
                                                              + "      <-- KSTREAM-MAP-0000000002, KSTREAM-MAP-0000000003\n"
                                                              + "    Processor: KSTREAM-FILTER-0000000021 (stores: [])\n"
                                                              + "      -=> KSTREAM-SINK-0000000020\n"
                                                              + "      <-- KSTREAM-MERGE-0000000004\n"
                                                              + "    Sink: KSTREAM-SINK-0000000020 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition)\n"
                                                              + "      <-- KSTREAM-FILTER-0000000021\n"
                                                              + "\n"
                                                              + "  Sub-topology: 1\n"
                                                              + "    Source: KSTREAM-SOURCE-0000000022 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition])\n"
                                                              + "      -=> KSTREAM-AGGREGATE-0000000006, KSTREAM-AGGREGATE-0000000013\n"
                                                              + "    Processor: KSTREAM-AGGREGATE-0000000013 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000012])\n"
                                                              + "      -=> KTABLE-TOSTREAM-0000000017\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000022\n"
                                                              + "    Processor: KSTREAM-AGGREGATE-0000000006 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000005])\n"
                                                              + "      -=> KTABLE-TOSTREAM-0000000010\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000022\n"
                                                              + "    Processor: KTABLE-TOSTREAM-0000000017 (stores: [])\n"
                                                              + "      -=> KSTREAM-MAPVALUES-0000000018\n"
                                                              + "      <-- KSTREAM-AGGREGATE-0000000013\n"
                                                              + "    Processor: KSTREAM-MAPVALUES-0000000018 (stores: [])\n"
                                                              + "      -=> KSTREAM-SINK-0000000019\n"
                                                              + "      <-- KTABLE-TOSTREAM-0000000017\n"
                                                              + "    Processor: KTABLE-TOSTREAM-0000000010 (stores: [])\n"
                                                              + "      -=> KSTREAM-SINK-0000000011\n"
                                                              + "      <-- KSTREAM-AGGREGATE-0000000006\n"
                                                              + "    Sink: KSTREAM-SINK-0000000011 (topic: outputTopic_0)\n"
                                                              + "      <-- KTABLE-TOSTREAM-0000000010\n"
                                                              + "    Sink: KSTREAM-SINK-0000000019 (topic: outputTopic_1)\n"
                                                              + "      <-- KSTREAM-MAPVALUES-0000000018\n\n";


    private static string EXPECTED_UNOPTIMIZED_TOPOLOGY = "Topologies:\n"
                                                                + "   Sub-topology: 0\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000000 (topics: [inputA])\n"
                                                                + "      -=> KSTREAM-MAP-0000000002\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000001 (topics: [inputB])\n"
                                                                + "      -=> KSTREAM-MAP-0000000003\n"
                                                                + "    Processor: KSTREAM-MAP-0000000002 (stores: [])\n"
                                                                + "      -=> KSTREAM-MERGE-0000000004\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000000\n"
                                                                + "    Processor: KSTREAM-MAP-0000000003 (stores: [])\n"
                                                                + "      -=> KSTREAM-MERGE-0000000004\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000001\n"
                                                                + "    Processor: KSTREAM-MERGE-0000000004 (stores: [])\n"
                                                                + "      -=> KSTREAM-FILTER-0000000008, KSTREAM-FILTER-0000000015\n"
                                                                + "      <-- KSTREAM-MAP-0000000002, KSTREAM-MAP-0000000003\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000008 (stores: [])\n"
                                                                + "      -=> KSTREAM-SINK-0000000007\n"
                                                                + "      <-- KSTREAM-MERGE-0000000004\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000015 (stores: [])\n"
                                                                + "      -=> KSTREAM-SINK-0000000014\n"
                                                                + "      <-- KSTREAM-MERGE-0000000004\n"
                                                                + "    Sink: KSTREAM-SINK-0000000007 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition)\n"
                                                                + "      <-- KSTREAM-FILTER-0000000008\n"
                                                                + "    Sink: KSTREAM-SINK-0000000014 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000012-repartition)\n"
                                                                + "      <-- KSTREAM-FILTER-0000000015\n"
                                                                + "\n"
                                                                + "  Sub-topology: 1\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000009 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition])\n"
                                                                + "      -=> KSTREAM-AGGREGATE-0000000006\n"
                                                                + "    Processor: KSTREAM-AGGREGATE-0000000006 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000005])\n"
                                                                + "      -=> KTABLE-TOSTREAM-0000000010\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000009\n"
                                                                + "    Processor: KTABLE-TOSTREAM-0000000010 (stores: [])\n"
                                                                + "      -=> KSTREAM-SINK-0000000011\n"
                                                                + "      <-- KSTREAM-AGGREGATE-0000000006\n"
                                                                + "    Sink: KSTREAM-SINK-0000000011 (topic: outputTopic_0)\n"
                                                                + "      <-- KTABLE-TOSTREAM-0000000010\n"
                                                                + "\n"
                                                                + "  Sub-topology: 2\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000016 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000012-repartition])\n"
                                                                + "      -=> KSTREAM-AGGREGATE-0000000013\n"
                                                                + "    Processor: KSTREAM-AGGREGATE-0000000013 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000012])\n"
                                                                + "      -=> KTABLE-TOSTREAM-0000000017\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000016\n"
                                                                + "    Processor: KTABLE-TOSTREAM-0000000017 (stores: [])\n"
                                                                + "      -=> KSTREAM-MAPVALUES-0000000018\n"
                                                                + "      <-- KSTREAM-AGGREGATE-0000000013\n"
                                                                + "    Processor: KSTREAM-MAPVALUES-0000000018 (stores: [])\n"
                                                                + "      -=> KSTREAM-SINK-0000000019\n"
                                                                + "      <-- KTABLE-TOSTREAM-0000000017\n"
                                                                + "    Sink: KSTREAM-SINK-0000000019 (topic: outputTopic_1)\n"
                                                                + "      <-- KSTREAM-MAPVALUES-0000000018\n\n";

}
