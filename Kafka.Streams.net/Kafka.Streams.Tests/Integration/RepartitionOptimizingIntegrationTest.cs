/*






 *

 *





 */



















































public class RepartitionOptimizingIntegrationTest {

    private static readonly int NUM_BROKERS = 1;
    private static readonly string INPUT_TOPIC = "input";
    private static readonly string COUNT_TOPIC = "outputTopic_0";
    private static readonly string AGGREGATION_TOPIC = "outputTopic_1";
    private static readonly string REDUCE_TOPIC = "outputTopic_2";
    private static readonly string JOINED_TOPIC = "joinedOutputTopic";

    private static readonly int ONE_REPARTITION_TOPIC = 1;
    private static readonly int FOUR_REPARTITION_TOPICS = 4;

    private Pattern repartitionTopicPattern = new Regex("Sink: .*-repartition", RegexOptions.Compiled);

    private Properties streamsConfiguration;


    
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private MockTime mockTime = CLUSTER.time;

    
    public void SetUp() {// throws Exception
        Properties props = new Properties();
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 1024 * 10);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);

        streamsConfiguration = StreamsTestUtils.getStreamsConfig(
            "maybe-optimized-test-app",
            CLUSTER.bootstrapServers(),
            Serdes.String().getClass().getName(),
            Serdes.String().getClass().getName(),
            props);

        CLUSTER.createTopics(INPUT_TOPIC,
                             COUNT_TOPIC,
                             AGGREGATION_TOPIC,
                             REDUCE_TOPIC,
                             JOINED_TOPIC);

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
                           FOUR_REPARTITION_TOPICS);
    }


    private void RunIntegrationTest(string optimizationConfig,
                                    int expectedNumberRepartitionTopics) {// throws Exception

        Initializer<int> initializer = () => 0;
        Aggregator<string, string, int> aggregator = (k, v, agg) => agg + v.Length();

        Reducer<string> reducer = (v1, v2) => v1 + ":" + v2;

        List<string> processorValueCollector = new ArrayList<>();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<string, string> sourceStream = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<string, string> mappedStream = sourceStream.map((k, v) => KeyValuePair.Create(k.toUpperCase(Locale.getDefault()), v));

        mappedStream.filter((k, v) => k.equals("B")).mapValues(v => v.toUpperCase(Locale.getDefault()))
            .process(() => new SimpleProcessor(processorValueCollector));

        KStream<string, long> countStream = mappedStream.groupByKey().count(Materialized.with(Serdes.String(), Serdes.Long())).toStream();

        countStream.to(COUNT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        mappedStream.groupByKey().aggregate(initializer,
                                            aggregator,
                                            Materialized.with(Serdes.String(), Serdes.Int()))
            .toStream().to(AGGREGATION_TOPIC, Produced.with(Serdes.String(), Serdes.Int()));

        // adding operators for case where the repartition node is further downstream
        mappedStream.filter((k, v) => true).peek((k, v) => System.Console.Out.WriteLine(k + ":" + v)).groupByKey()
            .reduce(reducer, Materialized.with(Serdes.String(), Serdes.String()))
            .toStream().to(REDUCE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        mappedStream.filter((k, v) => k.equals("A"))
            .join(countStream, (v1, v2) => v1 + ":" + v2.toString(),
                  JoinWindows.of(ofMillis(5000)),
                  Joined.with(Serdes.String(), Serdes.String(), Serdes.Long()))
            .to(JOINED_TOPIC);

        streamsConfiguration.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, optimizationConfig);

        Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer, StringSerializer);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, GetKeyValues(), producerConfig, mockTime);

        Properties consumerConfig1 = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer, LongDeserializer);
        Properties consumerConfig2 = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer, IntegerDeserializer);
        Properties consumerConfig3 = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer, StringDeserializer);

        Topology topology = builder.build(streamsConfiguration);
        string topologyString = topology.describe().toString();

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

        List<KeyValuePair<string, long>> expectedCountKeyValues = Array.asList(KeyValuePair.Create("A", 3L), KeyValuePair.Create("B", 3L), KeyValuePair.Create("C", 3L));
        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig1, COUNT_TOPIC, expectedCountKeyValues);

        List<KeyValuePair<string, int>> expectedAggKeyValues = Array.asList(KeyValuePair.Create("A", 9), KeyValuePair.Create("B", 9), KeyValuePair.Create("C", 9));
        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig2, AGGREGATION_TOPIC, expectedAggKeyValues);

        List<KeyValuePair<string, string>> expectedReduceKeyValues = Array.asList(KeyValuePair.Create("A", "foo:bar:baz"), KeyValuePair.Create("B", "foo:bar:baz"), KeyValuePair.Create("C", "foo:bar:baz"));
        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig3, REDUCE_TOPIC, expectedReduceKeyValues);

        List<KeyValuePair<string, string>> expectedJoinKeyValues = Array.asList(KeyValuePair.Create("A", "foo:3"), KeyValuePair.Create("A", "bar:3"), KeyValuePair.Create("A", "baz:3"));
        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig3, JOINED_TOPIC, expectedJoinKeyValues);


        List<string> expectedCollectedProcessorValues = Array.asList("FOO", "BAR", "BAZ");

        Assert.Equal(3, (processorValueCollector.Count));
        Assert.Equal(processorValueCollector, (expectedCollectedProcessorValues));

        streams.close(ofSeconds(5));
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
        string[] keys = new string[]{"a", "b", "c"};
        string[] values = new string[]{"foo", "bar", "baz"};
        foreach (string key in keys) {
            foreach (string value in values) {
                keyValueList.add(KeyValuePair.Create(key, value));
            }
        }
        return keyValueList;
    }


    private static class SimpleProcessor : AbstractProcessor<string, string> {

        List<string> valueList;

        SimpleProcessor(List<string> valueList) {
            this.valueList = valueList;
        }

        
        public void Process(string key, string value) {
            valueList.add(value);
        }
    }


    private static readonly string EXPECTED_OPTIMIZED_TOPOLOGY = "Topologies:\n"
                                                              + "   Sub-topology: 0\n"
                                                              + "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n"
                                                              + "      -=> KSTREAM-MAP-0000000001\n"
                                                              + "    Processor: KSTREAM-MAP-0000000001 (stores: [])\n"
                                                              + "      -=> KSTREAM-FILTER-0000000002, KSTREAM-FILTER-0000000040\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000000\n"
                                                              + "    Processor: KSTREAM-FILTER-0000000002 (stores: [])\n"
                                                              + "      -=> KSTREAM-MAPVALUES-0000000003\n"
                                                              + "      <-- KSTREAM-MAP-0000000001\n"
                                                              + "    Processor: KSTREAM-FILTER-0000000040 (stores: [])\n"
                                                              + "      -=> KSTREAM-SINK-0000000039\n"
                                                              + "      <-- KSTREAM-MAP-0000000001\n"
                                                              + "    Processor: KSTREAM-MAPVALUES-0000000003 (stores: [])\n"
                                                              + "      -=> KSTREAM-PROCESSOR-0000000004\n"
                                                              + "      <-- KSTREAM-FILTER-0000000002\n"
                                                              + "    Processor: KSTREAM-PROCESSOR-0000000004 (stores: [])\n"
                                                              + "      -=> none\n"
                                                              + "      <-- KSTREAM-MAPVALUES-0000000003\n"
                                                              + "    Sink: KSTREAM-SINK-0000000039 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000006-repartition)\n"
                                                              + "      <-- KSTREAM-FILTER-0000000040\n"
                                                              + "\n"
                                                              + "  Sub-topology: 1\n"
                                                              + "    Source: KSTREAM-SOURCE-0000000041 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000006-repartition])\n"
                                                              + "      -=> KSTREAM-FILTER-0000000020, KSTREAM-AGGREGATE-0000000007, KSTREAM-AGGREGATE-0000000014, KSTREAM-FILTER-0000000029\n"
                                                              + "    Processor: KSTREAM-AGGREGATE-0000000007 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000006])\n"
                                                              + "      -=> KTABLE-TOSTREAM-0000000011\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000041\n"
                                                              + "    Processor: KTABLE-TOSTREAM-0000000011 (stores: [])\n"
                                                              + "      -=> KSTREAM-SINK-0000000012, KSTREAM-WINDOWED-0000000034\n"
                                                              + "      <-- KSTREAM-AGGREGATE-0000000007\n"
                                                              + "    Processor: KSTREAM-FILTER-0000000020 (stores: [])\n"
                                                              + "      -=> KSTREAM-PEEK-0000000021\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000041\n"
                                                              + "    Processor: KSTREAM-FILTER-0000000029 (stores: [])\n"
                                                              + "      -=> KSTREAM-WINDOWED-0000000033\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000041\n"
                                                              + "    Processor: KSTREAM-PEEK-0000000021 (stores: [])\n"
                                                              + "      -=> KSTREAM-REDUCE-0000000023\n"
                                                              + "      <-- KSTREAM-FILTER-0000000020\n"
                                                              + "    Processor: KSTREAM-WINDOWED-0000000033 (stores: [KSTREAM-JOINTHIS-0000000035-store])\n"
                                                              + "      -=> KSTREAM-JOINTHIS-0000000035\n"
                                                              + "      <-- KSTREAM-FILTER-0000000029\n"
                                                              + "    Processor: KSTREAM-WINDOWED-0000000034 (stores: [KSTREAM-JOINOTHER-0000000036-store])\n"
                                                              + "      -=> KSTREAM-JOINOTHER-0000000036\n"
                                                              + "      <-- KTABLE-TOSTREAM-0000000011\n"
                                                              + "    Processor: KSTREAM-AGGREGATE-0000000014 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000013])\n"
                                                              + "      -=> KTABLE-TOSTREAM-0000000018\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000041\n"
                                                              + "    Processor: KSTREAM-JOINOTHER-0000000036 (stores: [KSTREAM-JOINTHIS-0000000035-store])\n"
                                                              + "      -=> KSTREAM-MERGE-0000000037\n"
                                                              + "      <-- KSTREAM-WINDOWED-0000000034\n"
                                                              + "    Processor: KSTREAM-JOINTHIS-0000000035 (stores: [KSTREAM-JOINOTHER-0000000036-store])\n"
                                                              + "      -=> KSTREAM-MERGE-0000000037\n"
                                                              + "      <-- KSTREAM-WINDOWED-0000000033\n"
                                                              + "    Processor: KSTREAM-REDUCE-0000000023 (stores: [KSTREAM-REDUCE-STATE-STORE-0000000022])\n"
                                                              + "      -=> KTABLE-TOSTREAM-0000000027\n"
                                                              + "      <-- KSTREAM-PEEK-0000000021\n"
                                                              + "    Processor: KSTREAM-MERGE-0000000037 (stores: [])\n"
                                                              + "      -=> KSTREAM-SINK-0000000038\n"
                                                              + "      <-- KSTREAM-JOINTHIS-0000000035, KSTREAM-JOINOTHER-0000000036\n"
                                                              + "    Processor: KTABLE-TOSTREAM-0000000018 (stores: [])\n"
                                                              + "      -=> KSTREAM-SINK-0000000019\n"
                                                              + "      <-- KSTREAM-AGGREGATE-0000000014\n"
                                                              + "    Processor: KTABLE-TOSTREAM-0000000027 (stores: [])\n"
                                                              + "      -=> KSTREAM-SINK-0000000028\n"
                                                              + "      <-- KSTREAM-REDUCE-0000000023\n"
                                                              + "    Sink: KSTREAM-SINK-0000000012 (topic: outputTopic_0)\n"
                                                              + "      <-- KTABLE-TOSTREAM-0000000011\n"
                                                              + "    Sink: KSTREAM-SINK-0000000019 (topic: outputTopic_1)\n"
                                                              + "      <-- KTABLE-TOSTREAM-0000000018\n"
                                                              + "    Sink: KSTREAM-SINK-0000000028 (topic: outputTopic_2)\n"
                                                              + "      <-- KTABLE-TOSTREAM-0000000027\n"
                                                              + "    Sink: KSTREAM-SINK-0000000038 (topic: joinedOutputTopic)\n"
                                                              + "      <-- KSTREAM-MERGE-0000000037\n\n";


    private static readonly string EXPECTED_UNOPTIMIZED_TOPOLOGY = "Topologies:\n"
                                                                + "   Sub-topology: 0\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n"
                                                                + "      -=> KSTREAM-MAP-0000000001\n"
                                                                + "    Processor: KSTREAM-MAP-0000000001 (stores: [])\n"
                                                                + "      -=> KSTREAM-FILTER-0000000020, KSTREAM-FILTER-0000000002, KSTREAM-FILTER-0000000009, KSTREAM-FILTER-0000000016, KSTREAM-FILTER-0000000029\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000000\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000020 (stores: [])\n"
                                                                + "      -=> KSTREAM-PEEK-0000000021\n"
                                                                + "      <-- KSTREAM-MAP-0000000001\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000002 (stores: [])\n"
                                                                + "      -=> KSTREAM-MAPVALUES-0000000003\n"
                                                                + "      <-- KSTREAM-MAP-0000000001\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000029 (stores: [])\n"
                                                                + "      -=> KSTREAM-FILTER-0000000031\n"
                                                                + "      <-- KSTREAM-MAP-0000000001\n"
                                                                + "    Processor: KSTREAM-PEEK-0000000021 (stores: [])\n"
                                                                + "      -=> KSTREAM-FILTER-0000000025\n"
                                                                + "      <-- KSTREAM-FILTER-0000000020\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000009 (stores: [])\n"
                                                                + "      -=> KSTREAM-SINK-0000000008\n"
                                                                + "      <-- KSTREAM-MAP-0000000001\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000016 (stores: [])\n"
                                                                + "      -=> KSTREAM-SINK-0000000015\n"
                                                                + "      <-- KSTREAM-MAP-0000000001\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000025 (stores: [])\n"
                                                                + "      -=> KSTREAM-SINK-0000000024\n"
                                                                + "      <-- KSTREAM-PEEK-0000000021\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000031 (stores: [])\n"
                                                                + "      -=> KSTREAM-SINK-0000000030\n"
                                                                + "      <-- KSTREAM-FILTER-0000000029\n"
                                                                + "    Processor: KSTREAM-MAPVALUES-0000000003 (stores: [])\n"
                                                                + "      -=> KSTREAM-PROCESSOR-0000000004\n"
                                                                + "      <-- KSTREAM-FILTER-0000000002\n"
                                                                + "    Processor: KSTREAM-PROCESSOR-0000000004 (stores: [])\n"
                                                                + "      -=> none\n"
                                                                + "      <-- KSTREAM-MAPVALUES-0000000003\n"
                                                                + "    Sink: KSTREAM-SINK-0000000008 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000006-repartition)\n"
                                                                + "      <-- KSTREAM-FILTER-0000000009\n"
                                                                + "    Sink: KSTREAM-SINK-0000000015 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000013-repartition)\n"
                                                                + "      <-- KSTREAM-FILTER-0000000016\n"
                                                                + "    Sink: KSTREAM-SINK-0000000024 (topic: KSTREAM-REDUCE-STATE-STORE-0000000022-repartition)\n"
                                                                + "      <-- KSTREAM-FILTER-0000000025\n"
                                                                + "    Sink: KSTREAM-SINK-0000000030 (topic: KSTREAM-FILTER-0000000029-repartition)\n"
                                                                + "      <-- KSTREAM-FILTER-0000000031\n"
                                                                + "\n"
                                                                + "  Sub-topology: 1\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000010 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000006-repartition])\n"
                                                                + "      -=> KSTREAM-AGGREGATE-0000000007\n"
                                                                + "    Processor: KSTREAM-AGGREGATE-0000000007 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000006])\n"
                                                                + "      -=> KTABLE-TOSTREAM-0000000011\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000010\n"
                                                                + "    Processor: KTABLE-TOSTREAM-0000000011 (stores: [])\n"
                                                                + "      -=> KSTREAM-SINK-0000000012, KSTREAM-WINDOWED-0000000034\n"
                                                                + "      <-- KSTREAM-AGGREGATE-0000000007\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000032 (topics: [KSTREAM-FILTER-0000000029-repartition])\n"
                                                                + "      -=> KSTREAM-WINDOWED-0000000033\n"
                                                                + "    Processor: KSTREAM-WINDOWED-0000000033 (stores: [KSTREAM-JOINTHIS-0000000035-store])\n"
                                                                + "      -=> KSTREAM-JOINTHIS-0000000035\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000032\n"
                                                                + "    Processor: KSTREAM-WINDOWED-0000000034 (stores: [KSTREAM-JOINOTHER-0000000036-store])\n"
                                                                + "      -=> KSTREAM-JOINOTHER-0000000036\n"
                                                                + "      <-- KTABLE-TOSTREAM-0000000011\n"
                                                                + "    Processor: KSTREAM-JOINOTHER-0000000036 (stores: [KSTREAM-JOINTHIS-0000000035-store])\n"
                                                                + "      -=> KSTREAM-MERGE-0000000037\n"
                                                                + "      <-- KSTREAM-WINDOWED-0000000034\n"
                                                                + "    Processor: KSTREAM-JOINTHIS-0000000035 (stores: [KSTREAM-JOINOTHER-0000000036-store])\n"
                                                                + "      -=> KSTREAM-MERGE-0000000037\n"
                                                                + "      <-- KSTREAM-WINDOWED-0000000033\n"
                                                                + "    Processor: KSTREAM-MERGE-0000000037 (stores: [])\n"
                                                                + "      -=> KSTREAM-SINK-0000000038\n"
                                                                + "      <-- KSTREAM-JOINTHIS-0000000035, KSTREAM-JOINOTHER-0000000036\n"
                                                                + "    Sink: KSTREAM-SINK-0000000012 (topic: outputTopic_0)\n"
                                                                + "      <-- KTABLE-TOSTREAM-0000000011\n"
                                                                + "    Sink: KSTREAM-SINK-0000000038 (topic: joinedOutputTopic)\n"
                                                                + "      <-- KSTREAM-MERGE-0000000037\n"
                                                                + "\n"
                                                                + "  Sub-topology: 2\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000017 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000013-repartition])\n"
                                                                + "      -=> KSTREAM-AGGREGATE-0000000014\n"
                                                                + "    Processor: KSTREAM-AGGREGATE-0000000014 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000013])\n"
                                                                + "      -=> KTABLE-TOSTREAM-0000000018\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000017\n"
                                                                + "    Processor: KTABLE-TOSTREAM-0000000018 (stores: [])\n"
                                                                + "      -=> KSTREAM-SINK-0000000019\n"
                                                                + "      <-- KSTREAM-AGGREGATE-0000000014\n"
                                                                + "    Sink: KSTREAM-SINK-0000000019 (topic: outputTopic_1)\n"
                                                                + "      <-- KTABLE-TOSTREAM-0000000018\n"
                                                                + "\n"
                                                                + "  Sub-topology: 3\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000026 (topics: [KSTREAM-REDUCE-STATE-STORE-0000000022-repartition])\n"
                                                                + "      -=> KSTREAM-REDUCE-0000000023\n"
                                                                + "    Processor: KSTREAM-REDUCE-0000000023 (stores: [KSTREAM-REDUCE-STATE-STORE-0000000022])\n"
                                                                + "      -=> KTABLE-TOSTREAM-0000000027\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000026\n"
                                                                + "    Processor: KTABLE-TOSTREAM-0000000027 (stores: [])\n"
                                                                + "      -=> KSTREAM-SINK-0000000028\n"
                                                                + "      <-- KSTREAM-REDUCE-0000000023\n"
                                                                + "    Sink: KSTREAM-SINK-0000000028 (topic: outputTopic_2)\n"
                                                                + "      <-- KTABLE-TOSTREAM-0000000027\n\n";

}
