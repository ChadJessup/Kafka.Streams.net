//using Confluent.Kafka;
//using Kafka.Common;
//using Kafka.Streams.Clients;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State;
//using Kafka.Streams.Temporary;
//using Kafka.Streams.Tests;
//using Kafka.Streams.Tests.Helpers;
//using Kafka.Streams.Tests.Mocks;
//using Kafka.Streams.Threads.KafkaStreams;
//using System;
//using System.Collections.Generic;
//using System.Text.RegularExpressions;
//using Xunit;

//namespace Kafka.Streams.Tests.Integration
//{
//    /**
//   * End-to-end integration test based on using regex and named topics for creating sources, using
//   * an embedded Kafka cluster.
//*/
//    public class RegexSourceIntegrationTest
//    {
//        private static readonly int NUM_BROKERS = 1;

//        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
//        private MockTime mockTime = CLUSTER.time;

//        private static readonly string TOPIC_1 = "topic-1";
//        private static readonly string TOPIC_2 = "topic-2";
//        private static readonly string TOPIC_A = "topic-A";
//        private static readonly string TOPIC_C = "topic-C";
//        private static readonly string TOPIC_Y = "topic-Y";
//        private static readonly string TOPIC_Z = "topic-Z";
//        private static readonly string FA_TOPIC = "fa";
//        private static readonly string FOO_TOPIC = "foo";
//        private static readonly string PARTITIONED_TOPIC_1 = "partitioned-1";
//        private static readonly string PARTITIONED_TOPIC_2 = "partitioned-2";

//        private static readonly string STRING_SERDE_CLASSNAME = Serdes.String().GetType().FullName;
//        private StreamsConfig streamsConfiguration;
//        private static readonly string STREAM_TASKS_NOT_UPDATED = "Stream tasks not updated";
//        private IKafkaStreamsThread streams;
//        private static volatile int topicSuffixGenerator = 0;
//        private string outputTopic;

//        public static void StartKafkaCluster()
//        {// throws InterruptedException
//            CLUSTER.CreateTopics(
//                TOPIC_1,
//                TOPIC_2,
//                TOPIC_A,
//                TOPIC_C,
//                TOPIC_Y,
//                TOPIC_Z,
//                FA_TOPIC,
//                FOO_TOPIC);
//            CLUSTER.CreateTopic(PARTITIONED_TOPIC_1, 2, 1);
//            CLUSTER.CreateTopic(PARTITIONED_TOPIC_2, 2, 1);
//        }


//        public RegexSourceIntegrationTest()
//        {// throws InterruptedException
//            outputTopic = CreateTopic(++topicSuffixGenerator);
//            StreamsConfig properties = new StreamsConfig();
//            properties.CacheMaxBytesBuffering = 0;
//            properties.CommitIntervalMs = 100;
//            properties.Put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
//            properties.Put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

//            streamsConfiguration = StreamsTestUtils.getStreamsConfig(
//                "regex-source-integration-test",
//                CLUSTER.bootstrapServers(),
//                STRING_SERDE_CLASSNAME,
//                STRING_SERDE_CLASSNAME,
//                properties);
//        }


//        public void TearDown()
//        { //throws IOException
//            if (streams != null)
//            {
//                streams.Close();
//            }
//            // Remove any state from previous test runs
//            IntegrationTestUtils.PurgeLocalStreamsState(streamsConfiguration);
//        }

//        [Fact]
//        public void TestRegexMatchesTopicsAWhenCreated()
//        {// throws Exception

//            ISerde<string> stringSerde = Serdes.String();
//            List<string> expectedFirstAssignment = Collections.singletonList("TEST-TOPIC-1");
//            List<string> expectedSecondAssignment = Arrays.asList("TEST-TOPIC-1", "TEST-TOPIC-2");

//            CLUSTER.CreateTopic("TEST-TOPIC-1");

//            StreamsBuilder builder = new StreamsBuilder();

//            IKStream<string, string> pattern1Stream = builder.Stream<string, string>(new Regex("TEST-TOPIC-\\d", RegexOptions.Compiled));

//            pattern1Stream.To(outputTopic, Produced.With(stringSerde, stringSerde));
//            List<string> assignedTopics = new CopyOnWriteArrayList<>();
//            streams = new KafkaStreamsThread(builder.Build(), streamsConfiguration, new DefaultKafkaClientSupplier());
//            //            {
//            //
//            //
//            //            IConsumer<byte[], byte[]> getConsumer(Dictionary<string, object> config)
//            //            {
//            //                return new KafkaConsumer<byte[], byte[]>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())
//            //                {
//            //
//            //
//            //                    public void subscribe(Pattern topics, ConsumerRebalanceListener listener)
//            //            {
//            //                base.subscribe(topics, new TheConsumerRebalanceListener(assignedTopics, listener));
//            //            }
//            //        }
//            //
//            //    }
//            //});

//            streams.Start();
//            TestUtils.WaitForCondition(() => assignedTopics.Equals(expectedFirstAssignment), STREAM_TASKS_NOT_UPDATED);

//            CLUSTER.CreateTopic("TEST-TOPIC-2");

//            TestUtils.WaitForCondition(() => assignedTopics.Equals(expectedSecondAssignment), STREAM_TASKS_NOT_UPDATED);

//        }

//        private string CreateTopic(int suffix)
//        {// throws InterruptedException
//            string outputTopic = "outputTopic_" + suffix;
//            CLUSTER.CreateTopic(outputTopic);
//            return outputTopic;
//        }

//        [Fact]
//        public void TestRegexMatchesTopicsAWhenDeleted()
//        {// throws Exception

//            ISerde<string> stringSerde = Serdes.String();
//            List<string> expectedFirstAssignment = Arrays.asList("TEST-TOPIC-A", "TEST-TOPIC-B");
//            List<string> expectedSecondAssignment = Collections.singletonList("TEST-TOPIC-B");

//            Cluster.CreateTopics("TEST-TOPIC-A", "TEST-TOPIC-B");

//            StreamsBuilder builder = new StreamsBuilder();

//            IKStream<string, string> pattern1Stream = builder.Stream<string, string>(new Regex("TEST-TOPIC-[A-Z]", RegexOptions.Compiled));

//            pattern1Stream.To(outputTopic, Produced.With(stringSerde, stringSerde));

//            List<string> assignedTopics = null; // new CopyOnWriteArrayList<>();
//            streams = new KafkaStreamsThread(
//                builder.Build(),
//                streamsConfiguration,
//                new DefaultKafkaClientSupplier());
//            //    {
//            //
//            //
//            //            public IConsumer<byte[], byte[]> getConsumer(Dictionary<string, object> config)
//            //    {
//            //        return new KafkaConsumer<byte[], byte[]>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())
//            //        {
//            //
//            //
//            //                    public void subscribe(Pattern topics, ConsumerRebalanceListener listener)
//            //        {
//            //            base.subscribe(topics, new TheConsumerRebalanceListener(assignedTopics, listener));
//            //        }
//            //    };
//            //
//            //}
//            //        });


//            streams.Start();
//            TestUtils.WaitForCondition(() => assignedTopics.Equals(expectedFirstAssignment), STREAM_TASKS_NOT_UPDATED);

//            CLUSTER.deleteTopic("TEST-TOPIC-A");

//            TestUtils.WaitForCondition(() => assignedTopics.Equals(expectedSecondAssignment), STREAM_TASKS_NOT_UPDATED);
//        }

//        [Fact]
//        public void ShouldAddStateStoreToRegexDefinedSource()
//        {// throws InterruptedException

//            IProcessorSupplier<string, string> processorSupplier = new MockProcessorSupplier<string, string>();
//            var storeBuilder = new MockKeyValueStoreBuilder("testStateStore", false);
//            long thirtySecondTimeout = 30 * 1000;

//            TopologyWrapper topology = new TopologyWrapper();
//            topology.AddSource("ingest", new Regex("topic-\\d+", RegexOptions.Compiled));
//            topology.AddProcessor("my-processor", processorSupplier, "ingest");
//            topology.AddStateStore(storeBuilder, "my-processor");

//            streams = new KafkaStreamsThread(topology, streamsConfiguration);

//            try
//            {
//                streams.Start();

//                TestCondition stateStoreNameBoundToSourceTopic = () =>
//                {
//                    Dictionary<string, List<string>> stateStoreToSourceTopic = topology.getInternalBuilder().stateStoreNameToSourceTopics();
//                    List<string> topicNamesList = stateStoreToSourceTopic.Get("testStateStore");
//                    return topicNamesList != null && !topicNamesList.IsEmpty() && topicNamesList.Get(0).Equals("topic-1");
//                };

//                TestUtils.WaitForCondition(stateStoreNameBoundToSourceTopic, thirtySecondTimeout, "Did not find topic: [topic-1] connected to state store: [testStateStore]");

//            }
//            finally
//            {
//                streams.Close();
//            }
//        }

//        [Fact]
//        public void TestShouldReadFromRegexAndNamedTopics()
//        {// throws Exception

//            string topic1TestMessage = "topic-1 test";
//            string topic2TestMessage = "topic-2 test";
//            string topicATestMessage = "topic-A test";
//            string topicCTestMessage = "topic-C test";
//            string topicYTestMessage = "topic-Y test";
//            string topicZTestMessage = "topic-Z test";


//            ISerde<string> stringSerde = Serdes.String();

//            StreamsBuilder builder = new StreamsBuilder();

//            IKStream<string, string> pattern1Stream = builder.Stream<string, string>(new Regex("topic-\\d", RegexOptions.Compiled));
//            IKStream<string, string> pattern2Stream = builder.Stream<string, string>(new Regex("topic-[A-D]", RegexOptions.Compiled));
//            IKStream<string, string> namedTopicsStream = builder.Stream<string, string>(Arrays.asList(TOPIC_Y, TOPIC_Z));

//            pattern1Stream.To(outputTopic, Produced.With(stringSerde, stringSerde));
//            pattern2Stream.To(outputTopic, Produced.With(stringSerde, stringSerde));
//            namedTopicsStream.To(outputTopic, Produced.With(stringSerde, stringSerde));

//            streams = new KafkaStreamsThread(builder.Build(), streamsConfiguration);
//            streams.Start();

//            StreamsConfig ProducerConfig = TestUtils.ProducerConfig(CLUSTER.bootstrapServers(), Serdes.String().Serializer, Serdes.String().Serializer);

//            IntegrationTestUtils.ProduceValuesSynchronously(TOPIC_1, Collections.singleton(topic1TestMessage), ProducerConfig, mockTime);
//            IntegrationTestUtils.ProduceValuesSynchronously(TOPIC_2, Collections.singleton(topic2TestMessage), ProducerConfig, mockTime);
//            IntegrationTestUtils.ProduceValuesSynchronously(TOPIC_A, Collections.singleton(topicATestMessage), ProducerConfig, mockTime);
//            IntegrationTestUtils.ProduceValuesSynchronously(TOPIC_C, Collections.singleton(topicCTestMessage), ProducerConfig, mockTime);
//            IntegrationTestUtils.ProduceValuesSynchronously(TOPIC_Y, Collections.singleton(topicYTestMessage), ProducerConfig, mockTime);
//            IntegrationTestUtils.ProduceValuesSynchronously(TOPIC_Z, Collections.singleton(topicZTestMessage), ProducerConfig, mockTime);

//            StreamsConfig consumerConfig = TestUtils.ConsumerConfig(CLUSTER.bootstrapServers(), Serdes.String().Deserializer, Serdes.String().Deserializer);

//            List<string> expectedReceivedValues = Arrays.asList(topicATestMessage, topic1TestMessage, topic2TestMessage, topicCTestMessage, topicYTestMessage, topicZTestMessage);
//            List<KeyValuePair<string, string>> receivedKeyValues = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, 6);
//            List<string> actualValues = new List<string>(6);

//            foreach (KeyValuePair<string, string> receivedKeyValue in receivedKeyValues)
//            {
//                actualValues.Add(receivedKeyValue.Value);
//            }

//            Collections.sort(actualValues);
//            Collections.sort(expectedReceivedValues);
//            Assert.Equal(actualValues, expectedReceivedValues);
//        }

//        [Fact]
//        public void TestMultipleConsumersCanReadFromPartitionedTopic()
//        {// throws Exception

//            IKafkaStreamsThread partitionedStreamsLeader = null;
//            IKafkaStreamsThread partitionedStreamsFollower = null;
//            ISerde<string> stringSerde = Serdes.String();
//            StreamsBuilder builderLeader = new StreamsBuilder();
//            StreamsBuilder builderFollower = new StreamsBuilder();
//            List<string> expectedAssignment = Arrays.asList(PARTITIONED_TOPIC_1, PARTITIONED_TOPIC_2);

//            IKStream<string, string> partitionedStreamLeader = builderLeader.Stream<string, string>(new Regex("partitioned-\\d", RegexOptions.Compiled));
//            IKStream<string, string> partitionedStreamFollower = builderFollower.Stream<string, string>(new Regex("partitioned-\\d", RegexOptions.Compiled));

//            partitionedStreamLeader.To(outputTopic, Produced.With(stringSerde, stringSerde));
//            partitionedStreamFollower.To(outputTopic, Produced.With(stringSerde, stringSerde));

//            List<string> leaderAssignment = new List<string>();
//            List<string> followerAssignment = new List<string>();

//            partitionedStreamsLeader = new KafkaStreamsThread(builderLeader.Build(), streamsConfiguration, new DefaultKafkaClientSupplier());
//            //        {
//            //
//            //
//            //                public IConsumer<byte[], byte[]> getConsumer(Dictionary<string, object> config)
//            //        {
//            //            return new KafkaConsumer<byte[], byte[]>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())
//            //            {
//            //
//            //
//            //                        public void subscribe(Pattern topics, ConsumerRebalanceListener listener)
//            //            {
//            //                base.subscribe(topics, new TheConsumerRebalanceListener(leaderAssignment, listener));
//            //            }
//            //        };
//            //            });
//            partitionedStreamsFollower = new KafkaStreamsThread(builderFollower.Build(), streamsConfiguration, new DefaultKafkaClientSupplier());
//            //{
//            //
//            //    public IConsumer<byte[], byte[]> getConsumer(Dictionary<string, object> config)
//            //    {
//            //        return new KafkaConsumer<byte[], byte[]>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())
//            //        {
//            //
//            //
//            //                        public void subscribe(Pattern topics, ConsumerRebalanceListener listener)
//            //        {
//            //            base.subscribe(topics, new TheConsumerRebalanceListener(followerAssignment, listener));
//            //        }
//            //    };
//            //
//            //}
//            //            });

//            partitionedStreamsLeader.Start();
//            partitionedStreamsFollower.Start();
//            TestUtils.WaitForCondition(() => followerAssignment.Equals(expectedAssignment) && leaderAssignment.Equals(expectedAssignment), "topic assignment not completed");

//            if (partitionedStreamsLeader != null)
//            {
//                partitionedStreamsLeader.Close();
//            }

//            if (partitionedStreamsFollower != null)
//            {
//                partitionedStreamsFollower.Close();
//            }
//        }

//        [Fact]
//        public void TestNoMessagesSentExceptionFromOverlappingPatterns()
//        {// throws Exception

//            string fMessage = "fMessage";
//            string fooMessage = "fooMessage";
//            ISerde<string> stringSerde = Serdes.String();
//            StreamsBuilder builder = new StreamsBuilder();

//            // overlapping patterns here, no messages should be sent as TopologyException
//            // will be thrown when the processor topology is built.
//            IKStream<string, string> pattern1Stream = builder.Stream<string, string>(new Regex("foo.*", RegexOptions.Compiled));
//            IKStream<string, string> pattern2Stream = builder.Stream<string, string>(new Regex("f.*", RegexOptions.Compiled));

//            pattern1Stream.To(outputTopic, Produced.With(stringSerde, stringSerde));
//            pattern2Stream.To(outputTopic, Produced.With(stringSerde, stringSerde));

//            bool expectError = false;

//            streams = new KafkaStreamsThread(builder.Build(), streamsConfiguration);
//            streams.SetStateListener((newState, oldState) =>
//            {
//                if (newState == KafkaStreamsThreadStates.ERROR)
//                {
//                    expectError.set(true);
//                }
//            });

//            streams.Start();

//            StreamsConfig ProducerConfig = TestUtils.ProducerConfig(CLUSTER.bootstrapServers(), Serdes.String().Serializer, Serdes.String().Serializer);

//            IntegrationTestUtils.ProduceValuesSynchronously(FA_TOPIC, Collections.singleton(fMessage), ProducerConfig, mockTime);
//            IntegrationTestUtils.ProduceValuesSynchronously(FOO_TOPIC, Collections.singleton(fooMessage), ProducerConfig, mockTime);

//            StreamsConfig consumerConfig = TestUtils.ConsumerConfig(CLUSTER.bootstrapServers(), Serdes.String().Deserializer, Serdes.String().Deserializer);
//            try
//            {
//                IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, 2, 5000);
//                throw new Exception("This should not happen: an assertion error should have been thrown before this.");
//            }
//            catch (Exception e)
//            {
//                // this is fine
//            }

//            Assert.True(expectError);
//        }

//        private class TheConsumerRebalanceListener : IConsumerRebalanceListener
//        {
//            private readonly List<string> assignedTopics;
//            private IConsumerRebalanceListener listener;

//            TheConsumerRebalanceListener(List<string> assignedTopics, IConsumerRebalanceListener listener)
//            {
//                this.assignedTopics = assignedTopics;
//                this.listener = listener;
//            }


//            public void OnPartitionsRevoked(List<TopicPartition> partitions)
//            {
//                assignedTopics.Clear();
//                listener.OnPartitionsRevoked(partitions);
//            }


//            public void OnPartitionsAssigned(List<TopicPartition> partitions)
//            {
//                foreach (TopicPartition partition in partitions)
//                {
//                    assignedTopics.Add(partition.Topic);
//                }
//                Collections.sort(assignedTopics);
//                listener.OnPartitionsAssigned(partitions);
//            }

//            public void OnPartitionsRevoked(IConsumer<byte[], byte[]> consumer, List<TopicPartitionOffset> revokedPartitions)
//            {
//                throw new NotImplementedException();
//            }

//            public void OnPartitionsAssigned(IConsumer<byte[], byte[]> consumer, List<TopicPartition> assignedPartitions)
//            {
//                throw new NotImplementedException();
//            }
//        }

//    }
//}
