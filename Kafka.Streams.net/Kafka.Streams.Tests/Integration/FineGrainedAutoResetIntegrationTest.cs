using Confluent.Kafka;
using Xunit;
using System;
using Kafka.Streams.KStream;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Topologies;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Kafka.Streams;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Kafka.Streams.Tests.Integration
{
    public class FineGrainedAutoResetIntegrationTest
    {

        private static int NUM_BROKERS = 1;
        private static string DEFAULT_OUTPUT_TOPIC = "outputTopic";
        private static string OUTPUT_TOPIC_0 = "outputTopic_0";
        private static string OUTPUT_TOPIC_1 = "outputTopic_1";
        private static string OUTPUT_TOPIC_2 = "outputTopic_2";


        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
        private MockTime mockTime = CLUSTER.time;

        private static string TOPIC_1_0 = "topic-1_0";
        private static string TOPIC_2_0 = "topic-2_0";
        private static string TOPIC_A_0 = "topic-A_0";
        private static string TOPIC_C_0 = "topic-C_0";
        private static string TOPIC_Y_0 = "topic-Y_0";
        private static string TOPIC_Z_0 = "topic-Z_0";
        private static string TOPIC_1_1 = "topic-1_1";
        private static string TOPIC_2_1 = "topic-2_1";
        private static string TOPIC_A_1 = "topic-A_1";
        private static string TOPIC_C_1 = "topic-C_1";
        private static string TOPIC_Y_1 = "topic-Y_1";
        private static string TOPIC_Z_1 = "topic-Z_1";
        private static string TOPIC_1_2 = "topic-1_2";
        private static string TOPIC_2_2 = "topic-2_2";
        private static string TOPIC_A_2 = "topic-A_2";
        private static string TOPIC_C_2 = "topic-C_2";
        private static string TOPIC_Y_2 = "topic-Y_2";
        private static string TOPIC_Z_2 = "topic-Z_2";
        private static string NOOP = "noop";
        private ISerde<string> stringSerde = Serdes.String();

        private static string STRING_SERDE_CLASSNAME = Serdes.String().getClass().getName();
        private Properties streamsConfiguration;

        private string topic1TestMessage = "topic-1 test";
        private string topic2TestMessage = "topic-2 test";
        private string topicATestMessage = "topic-A test";
        private string topicCTestMessage = "topic-C test";
        private string topicYTestMessage = "topic-Y test";
        private string topicZTestMessage = "topic-Z test";



        public static void startKafkaCluster()
        {// throws InterruptedException
            CLUSTER.createTopics(
                TOPIC_1_0,
                TOPIC_2_0,
                TOPIC_A_0,
                TOPIC_C_0,
                TOPIC_Y_0,
                TOPIC_Z_0,
                TOPIC_1_1,
                TOPIC_2_1,
                TOPIC_A_1,
                TOPIC_C_1,
                TOPIC_Y_1,
                TOPIC_Z_1,
                TOPIC_1_2,
                TOPIC_2_2,
                TOPIC_A_2,
                TOPIC_C_2,
                TOPIC_Y_2,
                TOPIC_Z_2,
                NOOP,
                DEFAULT_OUTPUT_TOPIC,
                OUTPUT_TOPIC_0,
                OUTPUT_TOPIC_1,
                OUTPUT_TOPIC_2);
        }


        public void setUp(){ //throws IOException

        Properties props = new Properties();
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        streamsConfiguration = StreamsTestUtils.getStreamsConfig(
                "testAutoOffsetId",
                CLUSTER.bootstrapServers(),
                STRING_SERDE_CLASSNAME,
                STRING_SERDE_CLASSNAME,
                props);

        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    [Xunit.Fact]
    public void shouldOnlyReadRecordsWhereEarliestSpecifiedWithNoCommittedOffsetsWithGlobalAutoOffsetResetLatest()
    {// throws Exception
        streamsConfiguration.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");

        List<string> expectedReceivedValues = Array.asList(topic1TestMessage, topic2TestMessage);
        shouldOnlyReadForEarliest("_0", TOPIC_1_0, TOPIC_2_0, TOPIC_A_0, TOPIC_C_0, TOPIC_Y_0, TOPIC_Z_0, OUTPUT_TOPIC_0, expectedReceivedValues);
    }

    [Xunit.Fact]
    public void shouldOnlyReadRecordsWhereEarliestSpecifiedWithNoCommittedOffsetsWithDefaultGlobalAutoOffsetResetEarliest()
    {// throws Exception
        List<string> expectedReceivedValues = Array.asList(topic1TestMessage, topic2TestMessage, topicYTestMessage, topicZTestMessage);
        shouldOnlyReadForEarliest("_1", TOPIC_1_1, TOPIC_2_1, TOPIC_A_1, TOPIC_C_1, TOPIC_Y_1, TOPIC_Z_1, OUTPUT_TOPIC_1, expectedReceivedValues);
    }

    [Xunit.Fact]
    public void shouldOnlyReadRecordsWhereEarliestSpecifiedWithInvalidCommittedOffsets()
    {// throws Exception
        commitInvalidOffsets();

        List<string> expectedReceivedValues = Array.asList(topic1TestMessage, topic2TestMessage, topicYTestMessage, topicZTestMessage);
        shouldOnlyReadForEarliest("_2", TOPIC_1_2, TOPIC_2_2, TOPIC_A_2, TOPIC_C_2, TOPIC_Y_2, TOPIC_Z_2, OUTPUT_TOPIC_2, expectedReceivedValues);
    }

    private void shouldOnlyReadForEarliest(
        string topicSuffix,
        string topic1,
        string topic2,
        string topicA,
        string topicC,
        string topicY,
        string topicZ,
        string outputTopic,
        List<string> expectedReceivedValues)
    {// throws Exception

        StreamsBuilder builder = new StreamsBuilder();


        IKStream<string, string> pattern1Stream = builder.stream(new Regex("topic-\\d" + topicSuffix, RegexOptions.Compiled), Consumed.with(Topology.AutoOffsetReset.EARLIEST));
        IKStream<string, string> pattern2Stream = builder.stream(new Regex("topic-[A-D]" + topicSuffix, RegexOptions.Compiled), Consumed.with(Topology.AutoOffsetReset.LATEST));
        IKStream<string, string> namedTopicsStream = builder.stream(Array.asList(topicY, topicZ));

        pattern1Stream.to(outputTopic, Produced.with(stringSerde, stringSerde));
        pattern2Stream.to(outputTopic, Produced.with(stringSerde, stringSerde));
        namedTopicsStream.to(outputTopic, Produced.with(stringSerde, stringSerde));

        Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer, StringSerializer);

        IntegrationTestUtils.produceValuesSynchronously(topic1, Collections.singletonList(topic1TestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(topic2, Collections.singletonList(topic2TestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(topicA, Collections.singletonList(topicATestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(topicC, Collections.singletonList(topicCTestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(topicY, Collections.singletonList(topicYTestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(topicZ, Collections.singletonList(topicZTestMessage), producerConfig, mockTime);

        Properties consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer, StringDeserializer);

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        List<KeyValuePair<string, string>> receivedKeyValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, expectedReceivedValues.Count);
        List<string> actualValues = new ArrayList<>(expectedReceivedValues.Count);

        foreach (KeyValuePair<string, string> receivedKeyValue in receivedKeyValues)
        {
            actualValues.add(receivedKeyValue.value);
        }

        streams.close();
        Collections.sort(actualValues);
        Collections.sort(expectedReceivedValues);
        Assert.Equal(actualValues, (expectedReceivedValues));
    }

    private void commitInvalidOffsets()
    {
        KafkaConsumer<string, string> consumer = new KafkaConsumer<>(TestUtils.consumerConfig(
            CLUSTER.bootstrapServers(),
            "commit_invalid_offset_app", // Having a separate application id to avoid waiting for last test poll interval timeout.
            StringDeserializer,
            StringDeserializer));

        Dictionary<TopicPartition, OffsetAndMetadata> invalidOffsets = new HashMap<>();
        invalidOffsets.put(new TopicPartition(TOPIC_1_2, 0), new OffsetAndMetadata(5, null));
        invalidOffsets.put(new TopicPartition(TOPIC_2_2, 0), new OffsetAndMetadata(5, null));
        invalidOffsets.put(new TopicPartition(TOPIC_A_2, 0), new OffsetAndMetadata(5, null));
        invalidOffsets.put(new TopicPartition(TOPIC_C_2, 0), new OffsetAndMetadata(5, null));
        invalidOffsets.put(new TopicPartition(TOPIC_Y_2, 0), new OffsetAndMetadata(5, null));
        invalidOffsets.put(new TopicPartition(TOPIC_Z_2, 0), new OffsetAndMetadata(5, null));

        consumer.commitSync(invalidOffsets);

        consumer.close();
    }

    [Xunit.Fact]
    public void shouldThrowExceptionOverlappingPattern()
    {
        StreamsBuilder builder = new StreamsBuilder();
        //NOTE this would realistically get caught when building topology, the test is for completeness
        builder.stream(new Regex("topic-[A-D]_1", RegexOptions.Compiled), Consumed.with(Topology.AutoOffsetReset.EARLIEST));

        try
        {
            builder.stream(new Regex("topic-[A-D]_1", RegexOptions.Compiled), Consumed.with(Topology.AutoOffsetReset.LATEST));
            builder.build();
            Assert.True(false, "Should have thrown TopologyException");
        }
        catch (TopologyException expected)
        {
            // do nothing
        }
    }

    [Xunit.Fact]
    public void shouldThrowExceptionOverlappingTopic()
    {
        StreamsBuilder builder = new StreamsBuilder();
        //NOTE this would realistically get caught when building topology, the test is for completeness
        builder.stream(new Regex("topic-[A-D]_1", RegexOptions.Compiled), Consumed.with(Topology.AutoOffsetReset.EARLIEST));
        try
        {
            builder.stream(Array.asList(TOPIC_A_1, TOPIC_Z_1), Consumed.with(Topology.AutoOffsetReset.LATEST));
            builder.build();
            Assert.True(false, "Should have thrown TopologyException");
        }
        catch (TopologyException expected)
        {
            // do nothing
        }
    }

    [Xunit.Fact]
    public void shouldThrowStreamsExceptionNoResetSpecified()
    {// throws InterruptedException
        Properties props = new Properties();
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        Properties localConfig = StreamsTestUtils.getStreamsConfig(
                "testAutoOffsetWithNone",
                CLUSTER.bootstrapServers(),
                STRING_SERDE_CLASSNAME,
                STRING_SERDE_CLASSNAME,
                props);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<string, string> exceptionStream = builder.stream(NOOP);

        exceptionStream.to(DEFAULT_OUTPUT_TOPIC, Produced.with(stringSerde, stringSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), localConfig);

        TestingUncaughtExceptionHandler uncaughtExceptionHandler = new TestingUncaughtExceptionHandler();

        streams.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        streams.start();
        TestUtils.waitForCondition(() => uncaughtExceptionHandler.correctExceptionThrown,
                "The expected NoOffsetForPartitionException was never thrown");
        streams.close();
    }


    private class TestingUncaughtExceptionHandler : Thread.UncaughtExceptionHandler
    {
        bool correctExceptionThrown = false;


        public void uncaughtException(Thread t, Throwable e)
        {
            Assert.Equal(e.getClass().getSimpleName(), ("StreamsException"));
            Assert.Equal(e.getCause().getClass().getSimpleName(), ("NoOffsetForPartitionException"));
            correctExceptionThrown = true;
        }
    }
}
