//using Confluent.Kafka;
//using Xunit;
//using System;
//using Kafka.Streams.KStream;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Topologies;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.Kafka.Streams;
//using System.Collections.Generic;
//using System.Text.RegularExpressions;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Errors;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.Tests.Helpers;
//using System.Threading;

//namespace Kafka.Streams.Tests.Integration
//{
//    public class FineGrainedAutoResetIntegrationTest
//    {
//        private const int NUM_BROKERS = 1;
//        private const string DEFAULT_OUTPUT_TOPIC = "outputTopic";
//        private const string OUTPUT_TOPIC_0 = "outputTopic_0";
//        private const string OUTPUT_TOPIC_1 = "outputTopic_1";
//        private const string OUTPUT_TOPIC_2 = "outputTopic_2";

//        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
//        private readonly MockTime mockTime = CLUSTER.time;

//        private const string TOPIC_1_0 = "topic-1_0";
//        private const string TOPIC_2_0 = "topic-2_0";
//        private const string TOPIC_A_0 = "topic-A_0";
//        private const string TOPIC_C_0 = "topic-C_0";
//        private const string TOPIC_Y_0 = "topic-Y_0";
//        private const string TOPIC_Z_0 = "topic-Z_0";
//        private const string TOPIC_1_1 = "topic-1_1";
//        private const string TOPIC_2_1 = "topic-2_1";
//        private const string TOPIC_A_1 = "topic-A_1";
//        private const string TOPIC_C_1 = "topic-C_1";
//        private const string TOPIC_Y_1 = "topic-Y_1";
//        private const string TOPIC_Z_1 = "topic-Z_1";
//        private const string TOPIC_1_2 = "topic-1_2";
//        private const string TOPIC_2_2 = "topic-2_2";
//        private const string TOPIC_A_2 = "topic-A_2";
//        private const string TOPIC_C_2 = "topic-C_2";
//        private const string TOPIC_Y_2 = "topic-Y_2";
//        private const string TOPIC_Z_2 = "topic-Z_2";
//        private const string NOOP = "noop";
//        private readonly ISerde<string> stringSerde = Serdes.String();

//        private static readonly string? STRING_SERDE_CLASSNAME = Serdes.String().GetType().FullName;
//        private StreamsConfig? streamsConfiguration;

//        private readonly string topic1TestMessage = "topic-1 test";
//        private readonly string topic2TestMessage = "topic-2 test";
//        private readonly string topicATestMessage = "topic-A test";
//        private readonly string topicCTestMessage = "topic-C test";
//        private readonly string topicYTestMessage = "topic-Y test";
//        private readonly string topicZTestMessage = "topic-Z test";

//        public static void StartKafkaCluster()
//        {// throws InterruptedException
//            CLUSTER.createTopics(
//                TOPIC_1_0,
//                TOPIC_2_0,
//                TOPIC_A_0,
//                TOPIC_C_0,
//                TOPIC_Y_0,
//                TOPIC_Z_0,
//                TOPIC_1_1,
//                TOPIC_2_1,
//                TOPIC_A_1,
//                TOPIC_C_1,
//                TOPIC_Y_1,
//                TOPIC_Z_1,
//                TOPIC_1_2,
//                TOPIC_2_2,
//                TOPIC_A_2,
//                TOPIC_C_2,
//                TOPIC_Y_2,
//                TOPIC_Z_2,
//                NOOP,
//                DEFAULT_OUTPUT_TOPIC,
//                OUTPUT_TOPIC_0,
//                OUTPUT_TOPIC_1,
//                OUTPUT_TOPIC_2);
//        }


//        public void SetUp()
//        { //throws IOException

//            StreamsConfig props = new StreamsConfig();
//            props.Set(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//            props.Set(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
//            props.Set(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
//            props.Set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

//            streamsConfiguration = StreamsTestUtils.getStreamsConfig(
//                    "testAutoOffsetId",
//                    CLUSTER.bootstrapServers(),
//                    STRING_SERDE_CLASSNAME,
//                    STRING_SERDE_CLASSNAME,
//                    props);

//            // Remove any state from previous test runs
//            IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
//        }

//        [Xunit.Fact]
//        public void ShouldOnlyReadRecordsWhereEarliestSpecifiedWithNoCommittedOffsetsWithGlobalAutoOffsetResetLatest()
//        {// throws Exception
//            streamsConfiguration.Set(StreamsConfig.ConsumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");

//            List<string> expectedReceivedValues = Array.asList(topic1TestMessage, topic2TestMessage);
//            ShouldOnlyReadForEarliest("_0", TOPIC_1_0, TOPIC_2_0, TOPIC_A_0, TOPIC_C_0, TOPIC_Y_0, TOPIC_Z_0, OUTPUT_TOPIC_0, expectedReceivedValues);
//        }

//        [Xunit.Fact]
//        public void ShouldOnlyReadRecordsWhereEarliestSpecifiedWithNoCommittedOffsetsWithDefaultGlobalAutoOffsetResetEarliest()
//        {// throws Exception
//            List<string> expectedReceivedValues = Array.asList(topic1TestMessage, topic2TestMessage, topicYTestMessage, topicZTestMessage);
//            ShouldOnlyReadForEarliest("_1", TOPIC_1_1, TOPIC_2_1, TOPIC_A_1, TOPIC_C_1, TOPIC_Y_1, TOPIC_Z_1, OUTPUT_TOPIC_1, expectedReceivedValues);
//        }

//        [Xunit.Fact]
//        public void ShouldOnlyReadRecordsWhereEarliestSpecifiedWithInvalidCommittedOffsets()
//        {// throws Exception
//            CommitInvalidOffsets();

//            List<string> expectedReceivedValues = Array.asList(topic1TestMessage, topic2TestMessage, topicYTestMessage, topicZTestMessage);
//            ShouldOnlyReadForEarliest("_2", TOPIC_1_2, TOPIC_2_2, TOPIC_A_2, TOPIC_C_2, TOPIC_Y_2, TOPIC_Z_2, OUTPUT_TOPIC_2, expectedReceivedValues);
//        }

//        private void ShouldOnlyReadForEarliest(
//            string topicSuffix,
//            string topic1,
//            string topic2,
//            string topicA,
//            string topicC,
//            string topicY,
//            string topicZ,
//            string outputTopic,
//            List<string> expectedReceivedValues)
//        {// throws Exception

//            StreamsBuilder builder = new StreamsBuilder();

//            IKStream<string, string> pattern1Stream = builder.Stream(new Regex("topic-\\d" + topicSuffix, RegexOptions.Compiled), Consumed.With(Topology.AutoOffsetReset.EARLIEST));
//            IKStream<string, string> pattern2Stream = builder.Stream(new Regex("topic-[A-D]" + topicSuffix, RegexOptions.Compiled), Consumed.With(Topology.AutoOffsetReset.LATEST));
//            IKStream<string, string> namedTopicsStream = builder.Stream(Array.asList(topicY, topicZ));

//            pattern1Stream.To(outputTopic, Produced.With(stringSerde, stringSerde));
//            pattern2Stream.To(outputTopic, Produced.With(stringSerde, stringSerde));
//            namedTopicsStream.To(outputTopic, Produced.With(stringSerde, stringSerde));

//            StreamsConfig producerConfig = TestUtils.ProducerConfig(CLUSTER.bootstrapServers(), Serdes.String().Serializer, Serdes.String().Serializer);

//            IntegrationTestUtils.produceValuesSynchronously(topic1, Collections.singletonList(topic1TestMessage), producerConfig, mockTime);
//            IntegrationTestUtils.produceValuesSynchronously(topic2, Collections.singletonList(topic2TestMessage), producerConfig, mockTime);
//            IntegrationTestUtils.produceValuesSynchronously(topicA, Collections.singletonList(topicATestMessage), producerConfig, mockTime);
//            IntegrationTestUtils.produceValuesSynchronously(topicC, Collections.singletonList(topicCTestMessage), producerConfig, mockTime);
//            IntegrationTestUtils.produceValuesSynchronously(topicY, Collections.singletonList(topicYTestMessage), producerConfig, mockTime);
//            IntegrationTestUtils.produceValuesSynchronously(topicZ, Collections.singletonList(topicZTestMessage), producerConfig, mockTime);

//            StreamsConfig consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), Serdes.String().Deserializer, Serdes.String().Deserializer);

//            KafkaStreams streams = new KafkaStreams(builder.Build(), streamsConfiguration);
//            streams.start();

//            List<KeyValuePair<string, string>> receivedKeyValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, expectedReceivedValues.Count);
//            List<string> actualValues = new List<string>(expectedReceivedValues.Count);

//            foreach (KeyValuePair<string, string> receivedKeyValue in receivedKeyValues)
//            {
//                actualValues.Add(receivedKeyValue.Value);
//            }

//            streams.close();
//            Collections.sort(actualValues);
//            Collections.sort(expectedReceivedValues);
//            Assert.Equal(actualValues, (expectedReceivedValues));
//        }

//        private void CommitInvalidOffsets()
//        {
//            IConsumer<string, string> consumer = new KafkaConsumer<>(TestUtils.consumerConfig(
//                CLUSTER.bootstrapServers(),
//                "commit_invalid_offset_app", // Having a separate application id to avoid waiting for last test poll interval timeout.
//                Serdes.String().Deserializer,
//                Serdes.String().Deserializer));

//            Dictionary<TopicPartition, OffsetAndMetadata> invalidOffsets = new Dictionary<TopicPartition, OffsetAndMetadata>();
//            invalidOffsets.Add(new TopicPartition(TOPIC_1_2, 0), new OffsetAndMetadata(5));
//            invalidOffsets.Add(new TopicPartition(TOPIC_2_2, 0), new OffsetAndMetadata(5));
//            invalidOffsets.Add(new TopicPartition(TOPIC_A_2, 0), new OffsetAndMetadata(5));
//            invalidOffsets.Add(new TopicPartition(TOPIC_C_2, 0), new OffsetAndMetadata(5));
//            invalidOffsets.Add(new TopicPartition(TOPIC_Y_2, 0), new OffsetAndMetadata(5));
//            invalidOffsets.Add(new TopicPartition(TOPIC_Z_2, 0), new OffsetAndMetadata(5));

//            consumer.commitSync(invalidOffsets);

//            consumer.close();
//        }

//        [Xunit.Fact]
//        public void ShouldThrowExceptionOverlappingPattern()
//        {
//            StreamsBuilder builder = new StreamsBuilder();
//            //NOTE this would realistically get caught when building topology, the test is for completeness
//            builder.Stream(new Regex("topic-[A-D]_1", RegexOptions.Compiled), Consumed.With(Topology.AutoOffsetReset.EARLIEST));

//            try
//            {
//                builder.Stream(new Regex("topic-[A-D]_1", RegexOptions.Compiled), Consumed.With(Topology.AutoOffsetReset.LATEST));
//                builder.Build();

//                Assert.True(false, "Should have thrown TopologyException");
//            }
//            catch (TopologyException expected)
//            {
//                // do nothing
//            }
//        }

//        [Xunit.Fact]
//        public void ShouldThrowExceptionOverlappingTopic()
//        {
//            StreamsBuilder builder = new StreamsBuilder();
//            //NOTE this would realistically get caught when building topology, the test is for completeness
//            builder.Stream(new Regex("topic-[A-D]_1", RegexOptions.Compiled), Consumed.With(Topology.AutoOffsetReset.EARLIEST));
//            try
//            {
//                builder.Stream(Array.asList(TOPIC_A_1, TOPIC_Z_1), Consumed.With(Topology.AutoOffsetReset.LATEST));
//                builder.Build();
//                Assert.True(false, "Should have thrown TopologyException");
//            }
//            catch (TopologyException expected)
//            {
//                // do nothing
//            }
//        }

//        [Xunit.Fact]
//        public void ShouldThrowStreamsExceptionNoResetSpecified()
//        {// throws InterruptedException
//            StreamsConfig props = new StreamsConfig();
//            props.Set(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//            props.Set(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
//            props.Set(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
//            props.Set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

//            StreamsConfig localConfig = StreamsTestUtils.getStreamsConfig(
//                    "testAutoOffsetWithNone",
//                    CLUSTER.bootstrapServers(),
//                    STRING_SERDE_CLASSNAME,
//                    STRING_SERDE_CLASSNAME,
//                    props);

//            StreamsBuilder builder = new StreamsBuilder();
//            IKStream<string, string> exceptionStream = builder.Stream<string, string>(NOOP);

//            exceptionStream.To(DEFAULT_OUTPUT_TOPIC, Produced.With(stringSerde, stringSerde));

//            KafkaStreams streams = new KafkaStreams(builder.Build(), localConfig);

//            TestingUncaughtExceptionHandler uncaughtExceptionHandler = new TestingUncaughtExceptionHandler();

//            streams.setUncaughtExceptionHandler(uncaughtExceptionHandler);
//            streams.start();
//            TestUtils.WaitForCondition(() => uncaughtExceptionHandler.correctExceptionThrown,
//                    "The expected NoOffsetForPartitionException was never thrown");
//            streams.close();
//        }


//        private class TestingUncaughtExceptionHandler //: UncaughtExceptionHandler
//        {
//            bool correctExceptionThrown = false;

//            public void UncaughtException(Thread t, IDisposable e)
//            {
//                Assert.Equal(e.GetType().FullName, ("StreamsException"));
//                Assert.Equal(e.GetType().FullName, ("NoOffsetForPartitionException"));
//                correctExceptionThrown = true;
//            }
//        }
//    }
//}
