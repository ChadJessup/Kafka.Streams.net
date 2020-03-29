/*






 *

 *





 */

























































using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using System.Collections.Generic;
/**
* End-to-end integration test based on using regex and named topics for creating sources, using
* an embedded Kafka cluster.
*/
public class RegexSourceIntegrationTest
{
    private static int NUM_BROKERS = 1;

    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private MockTime mockTime = CLUSTER.time;

    private static string TOPIC_1 = "topic-1";
    private static string TOPIC_2 = "topic-2";
    private static string TOPIC_A = "topic-A";
    private static string TOPIC_C = "topic-C";
    private static string TOPIC_Y = "topic-Y";
    private static string TOPIC_Z = "topic-Z";
    private static string FA_TOPIC = "fa";
    private static string FOO_TOPIC = "foo";
    private static string PARTITIONED_TOPIC_1 = "partitioned-1";
    private static string PARTITIONED_TOPIC_2 = "partitioned-2";

    private static string STRING_SERDE_CLASSNAME = Serdes.String().getClass().getName();
    private Properties streamsConfiguration;
    private static string STREAM_TASKS_NOT_UPDATED = "Stream tasks not updated";
    private KafkaStreams streams;
    private static volatile AtomicInteger topicSuffixGenerator = new AtomicInteger(0);
    private string outputTopic;



    public static void startKafkaCluster()
    {// throws InterruptedException
        CLUSTER.createTopics(
            TOPIC_1,
            TOPIC_2,
            TOPIC_A,
            TOPIC_C,
            TOPIC_Y,
            TOPIC_Z,
            FA_TOPIC,
            FOO_TOPIC);
        CLUSTER.createTopic(PARTITIONED_TOPIC_1, 2, 1);
        CLUSTER.createTopic(PARTITIONED_TOPIC_2, 2, 1);
    }


    public void setUp()
    {// throws InterruptedException
        outputTopic = createTopic(topicSuffixGenerator.incrementAndGet());
        Properties properties = new Properties();
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        properties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        streamsConfiguration = StreamsTestUtils.getStreamsConfig("regex-source-integration-test",
                                                                 CLUSTER.bootstrapServers(),
                                                                 STRING_SERDE_CLASSNAME,
                                                                 STRING_SERDE_CLASSNAME,
                                                                 properties);
    }


    public void tearDown()
    { //throws IOException
        if (streams != null)
        {
            streams.close();
        }
        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    [Xunit.Fact]
    public void testRegexMatchesTopicsAWhenCreated()
    {// throws Exception

        ISerde<string> stringSerde = Serdes.String();
        List<string> expectedFirstAssignment = Collections.singletonList("TEST-TOPIC-1");
        List<string> expectedSecondAssignment = Array.asList("TEST-TOPIC-1", "TEST-TOPIC-2");

        CLUSTER.createTopic("TEST-TOPIC-1");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<string, string> pattern1Stream = builder.stream(new Regex("TEST-TOPIC-\\d", RegexOptions.Compiled));

        pattern1Stream.to(outputTopic, Produced.with(stringSerde, stringSerde));
        List<string> assignedTopics = new CopyOnWriteArrayList<>();
        streams = new KafkaStreams(builder.build(), streamsConfiguration, new DefaultKafkaClientSupplier()
        {


            public Consumer<byte[], byte[]> getConsumer(Dictionary<string, object> config)
        {
            return new KafkaConsumer<byte[], byte[]>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())
            {


                    public void subscribe(Pattern topics, ConsumerRebalanceListener listener)
            {
                base.subscribe(topics, new TheConsumerRebalanceListener(assignedTopics, listener));
            }
        };

    }
});

        streams.start();
        TestUtils.waitForCondition(() => assignedTopics.equals(expectedFirstAssignment), STREAM_TASKS_NOT_UPDATED);

        CLUSTER.createTopic("TEST-TOPIC-2");

        TestUtils.waitForCondition(() => assignedTopics.equals(expectedSecondAssignment), STREAM_TASKS_NOT_UPDATED);

    }

    private string createTopic(int suffix)
{// throws InterruptedException
    string outputTopic = "outputTopic_" + suffix;
    CLUSTER.createTopic(outputTopic);
    return outputTopic;
}

[Xunit.Fact]
public void testRegexMatchesTopicsAWhenDeleted()
{// throws Exception

    Serde<string> stringSerde = Serdes.String();
    List<string> expectedFirstAssignment = Array.asList("TEST-TOPIC-A", "TEST-TOPIC-B");
    List<string> expectedSecondAssignment = Collections.singletonList("TEST-TOPIC-B");

    CLUSTER.createTopics("TEST-TOPIC-A", "TEST-TOPIC-B");

    StreamsBuilder builder = new StreamsBuilder();

    KStream<string, string> pattern1Stream = builder.stream(new Regex("TEST-TOPIC-[A-Z]", RegexOptions.Compiled));

    pattern1Stream.to(outputTopic, Produced.with(stringSerde, stringSerde));

    List<string> assignedTopics = new CopyOnWriteArrayList<>();
    streams = new KafkaStreams(builder.build(), streamsConfiguration, new DefaultKafkaClientSupplier()
    {


            public Consumer<byte[], byte[]> getConsumer(Dictionary<string, object> config)
    {
        return new KafkaConsumer<byte[], byte[]>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())
        {


                    public void subscribe(Pattern topics, ConsumerRebalanceListener listener)
        {
            base.subscribe(topics, new TheConsumerRebalanceListener(assignedTopics, listener));
        }
    };

}
        });


        streams.start();
        TestUtils.waitForCondition(() => assignedTopics.equals(expectedFirstAssignment), STREAM_TASKS_NOT_UPDATED);

        CLUSTER.deleteTopic("TEST-TOPIC-A");

        TestUtils.waitForCondition(() => assignedTopics.equals(expectedSecondAssignment), STREAM_TASKS_NOT_UPDATED);
    }

    [Xunit.Fact]
public void shouldAddStateStoreToRegexDefinedSource()
{// throws InterruptedException

    ProcessorSupplier<string, string> processorSupplier = new MockProcessorSupplier<>();
    StoreBuilder storeBuilder = new MockKeyValueStoreBuilder("testStateStore", false);
    long thirtySecondTimeout = 30 * 1000;

    TopologyWrapper topology = new TopologyWrapper();
    topology.addSource("ingest", new Regex("topic-\\d+", RegexOptions.Compiled));
    topology.addProcessor("my-processor", processorSupplier, "ingest");
    topology.addStateStore(storeBuilder, "my-processor");

    streams = new KafkaStreams(topology, streamsConfiguration);

    try
    {
        streams.start();

        TestCondition stateStoreNameBoundToSourceTopic = () =>
        {
            Dictionary<string, List<string>> stateStoreToSourceTopic = topology.getInternalBuilder().stateStoreNameToSourceTopics();
            List<string> topicNamesList = stateStoreToSourceTopic.get("testStateStore");
            return topicNamesList != null && !topicNamesList.isEmpty() && topicNamesList.get(0).equals("topic-1");
        };

        TestUtils.waitForCondition(stateStoreNameBoundToSourceTopic, thirtySecondTimeout, "Did not find topic: [topic-1] connected to state store: [testStateStore]");

    }
    finally
    {
        streams.close();
    }
}

[Xunit.Fact]
public void testShouldReadFromRegexAndNamedTopics()
{// throws Exception

    string topic1TestMessage = "topic-1 test";
    string topic2TestMessage = "topic-2 test";
    string topicATestMessage = "topic-A test";
    string topicCTestMessage = "topic-C test";
    string topicYTestMessage = "topic-Y test";
    string topicZTestMessage = "topic-Z test";


    Serde<string> stringSerde = Serdes.String();

    StreamsBuilder builder = new StreamsBuilder();

    KStream<string, string> pattern1Stream = builder.stream(new Regex("topic-\\d", RegexOptions.Compiled));
    KStream<string, string> pattern2Stream = builder.stream(new Regex("topic-[A-D]", RegexOptions.Compiled));
    KStream<string, string> namedTopicsStream = builder.stream(Array.asList(TOPIC_Y, TOPIC_Z));

    pattern1Stream.to(outputTopic, Produced.with(stringSerde, stringSerde));
    pattern2Stream.to(outputTopic, Produced.with(stringSerde, stringSerde));
    namedTopicsStream.to(outputTopic, Produced.with(stringSerde, stringSerde));

    streams = new KafkaStreams(builder.build(), streamsConfiguration);
    streams.start();

    Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer, StringSerializer);

    IntegrationTestUtils.produceValuesSynchronously(TOPIC_1, Collections.singleton(topic1TestMessage), producerConfig, mockTime);
    IntegrationTestUtils.produceValuesSynchronously(TOPIC_2, Collections.singleton(topic2TestMessage), producerConfig, mockTime);
    IntegrationTestUtils.produceValuesSynchronously(TOPIC_A, Collections.singleton(topicATestMessage), producerConfig, mockTime);
    IntegrationTestUtils.produceValuesSynchronously(TOPIC_C, Collections.singleton(topicCTestMessage), producerConfig, mockTime);
    IntegrationTestUtils.produceValuesSynchronously(TOPIC_Y, Collections.singleton(topicYTestMessage), producerConfig, mockTime);
    IntegrationTestUtils.produceValuesSynchronously(TOPIC_Z, Collections.singleton(topicZTestMessage), producerConfig, mockTime);

    Properties consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer, StringDeserializer);

    List<string> expectedReceivedValues = Array.asList(topicATestMessage, topic1TestMessage, topic2TestMessage, topicCTestMessage, topicYTestMessage, topicZTestMessage);
    List<KeyValuePair<string, string>> receivedKeyValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, 6);
    List<string> actualValues = new ArrayList<>(6);

    foreach (KeyValuePair<string, string> receivedKeyValue in receivedKeyValues)
    {
        actualValues.add(receivedKeyValue.value);
    }

    Collections.sort(actualValues);
    Collections.sort(expectedReceivedValues);
    Assert.Equal(actualValues, (expectedReceivedValues));
}

[Xunit.Fact]
public void testMultipleConsumersCanReadFromPartitionedTopic()
{// throws Exception

    KafkaStreams partitionedStreamsLeader = null;
    KafkaStreams partitionedStreamsFollower = null;
    try
    {
        Serde<string> stringSerde = Serdes.String();
        StreamsBuilder builderLeader = new StreamsBuilder();
        StreamsBuilder builderFollower = new StreamsBuilder();
        List<string> expectedAssignment = Array.asList(PARTITIONED_TOPIC_1, PARTITIONED_TOPIC_2);

        KStream<string, string> partitionedStreamLeader = builderLeader.stream(new Regex("partitioned-\\d", RegexOptions.Compiled));
        KStream<string, string> partitionedStreamFollower = builderFollower.stream(new Regex("partitioned-\\d", RegexOptions.Compiled));


        partitionedStreamLeader.to(outputTopic, Produced.with(stringSerde, stringSerde));
        partitionedStreamFollower.to(outputTopic, Produced.with(stringSerde, stringSerde));

        List<string> leaderAssignment = new ArrayList<>();
        List<string> followerAssignment = new ArrayList<>();

        partitionedStreamsLeader = new KafkaStreams(builderLeader.build(), streamsConfiguration, new DefaultKafkaClientSupplier()
        {


                public Consumer<byte[], byte[]> getConsumer(Dictionary<string, object> config)
        {
            return new KafkaConsumer<byte[], byte[]>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())
            {


                        public void subscribe(Pattern topics, ConsumerRebalanceListener listener)
            {
                base.subscribe(topics, new TheConsumerRebalanceListener(leaderAssignment, listener));
            }
        };

    }
            });
            partitionedStreamsFollower  = new KafkaStreams(builderFollower.build(), streamsConfiguration, new DefaultKafkaClientSupplier()
{

    public Consumer<byte[], byte[]> getConsumer(Dictionary<string, object> config)
    {
        return new KafkaConsumer<byte[], byte[]>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())
        {


                        public void subscribe(Pattern topics, ConsumerRebalanceListener listener)
        {
            base.subscribe(topics, new TheConsumerRebalanceListener(followerAssignment, listener));
        }
    };

}
            });

            partitionedStreamsLeader.start();
            partitionedStreamsFollower.start();
            TestUtils.waitForCondition(() => followerAssignment.equals(expectedAssignment) && leaderAssignment.equals(expectedAssignment), "topic assignment not completed");
        } finally {
            if (partitionedStreamsLeader != null) {
                partitionedStreamsLeader.close();
            }
            if (partitionedStreamsFollower != null) {
                partitionedStreamsFollower.close();
            }
        }

    }

    [Xunit.Fact]
public void testNoMessagesSentExceptionFromOverlappingPatterns()
{// throws Exception

    string fMessage = "fMessage";
    string fooMessage = "fooMessage";
    Serde<string> stringSerde = Serdes.String();
    StreamsBuilder builder = new StreamsBuilder();

    // overlapping patterns here, no messages should be sent as TopologyException
    // will be thrown when the processor topology is built.
    KStream<string, string> pattern1Stream = builder.stream(new Regex("foo.*", RegexOptions.Compiled));
    KStream<string, string> pattern2Stream = builder.stream(new Regex("f.*", RegexOptions.Compiled));

    pattern1Stream.to(outputTopic, Produced.with(stringSerde, stringSerde));
    pattern2Stream.to(outputTopic, Produced.with(stringSerde, stringSerde));

    AtomicBoolean expectError = new AtomicBoolean(false);

    streams = new KafkaStreams(builder.build(), streamsConfiguration);
    streams.setStateListener((newState, oldState) =>
    {
        if (newState == KafkaStreams.State.ERROR)
        {
            expectError.set(true);
        }
    });
    streams.start();

    Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer, StringSerializer);

    IntegrationTestUtils.produceValuesSynchronously(FA_TOPIC, Collections.singleton(fMessage), producerConfig, mockTime);
    IntegrationTestUtils.produceValuesSynchronously(FOO_TOPIC, Collections.singleton(fooMessage), producerConfig, mockTime);

    Properties consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer, StringDeserializer);
    try
    {
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, 2, 5000);
        throw new IllegalStateException("This should not happen: an assertion error should have been thrown before this.");
    }
    catch (AssertionError e)
    {
        // this is fine
    }

    Assert.Equal(expectError.get(), is (true));
}

private static class TheConsumerRebalanceListener : ConsumerRebalanceListener
{
    private List<string> assignedTopics;
    private ConsumerRebalanceListener listener;

    TheConsumerRebalanceListener(List<string> assignedTopics, ConsumerRebalanceListener listener)
    {
        this.assignedTopics = assignedTopics;
        this.listener = listener;
    }


    public void onPartitionsRevoked(Collection<TopicPartition> partitions)
    {
        assignedTopics.Clear();
        listener.onPartitionsRevoked(partitions);
    }


    public void onPartitionsAssigned(Collection<TopicPartition> partitions)
    {
        foreach (TopicPartition partition in partitions)
        {
            assignedTopics.add(partition.topic());
        }
        Collections.sort(assignedTopics);
        listener.onPartitionsAssigned(partitions);
    }
}

}
