//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream;
//using Kafka.Streams.Tests.Helpers;
//using System.Collections.Generic;

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
//        private KafkaStreams streams;
//        private static volatile AtomicInteger topicSuffixGenerator = new AtomicInteger(0);
//        private string outputTopic;



//        public static void StartKafkaCluster()
//        {// throws InterruptedException
//            CLUSTER.createTopics(
//                TOPIC_1,
//                TOPIC_2,
//                TOPIC_A,
//                TOPIC_C,
//                TOPIC_Y,
//                TOPIC_Z,
//                FA_TOPIC,
//                FOO_TOPIC);
//            CLUSTER.createTopic(PARTITIONED_TOPIC_1, 2, 1);
//            CLUSTER.createTopic(PARTITIONED_TOPIC_2, 2, 1);
//        }


//        public void SetUp()
//        {// throws InterruptedException
//            outputTopic = createTopic(topicSuffixGenerator.incrementAndGet());
//            StreamsConfig properties = new StreamsConfig();
//            properties.Put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//            properties.Put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
//            properties.Put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
//            properties.Put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

//            streamsConfiguration = StreamsTestUtils.getStreamsConfig("regex-source-integration-test",
//                                                                     CLUSTER.bootstrapServers(),
//                                                                     STRING_SERDE_CLASSNAME,
//                                                                     STRING_SERDE_CLASSNAME,
//                                                                     properties);
//        }


//        public void TearDown()
//        { //throws IOException
//            if (streams != null)
//            {
//                streams.Close();
//            }
//            // Remove any state from previous test runs
//            IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
//        }

//        [Fact]
//        public void TestRegexMatchesTopicsAWhenCreated()
//        {// throws Exception

//            ISerde<string> stringSerde = Serdes.String();
//            List<string> expectedFirstAssignment = Collections.singletonList("TEST-TOPIC-1");
//            List<string> expectedSecondAssignment = Array.asList("TEST-TOPIC-1", "TEST-TOPIC-2");

//            CLUSTER.createTopic("TEST-TOPIC-1");

//            StreamsBuilder builder = new StreamsBuilder();

//            KStream<string, string> pattern1Stream = builder.Stream(new Regex("TEST-TOPIC-\\d", RegexOptions.Compiled));

//            pattern1Stream.To(outputTopic, Produced.With(stringSerde, stringSerde));
//            List<string> assignedTopics = new CopyOnWriteArrayList<>();
//            streams = new KafkaStreams(builder.Build(), streamsConfiguration, new DefaultKafkaClientSupplier()
//            {


//            public Consumer<byte[], byte[]> getConsumer(Dictionary<string, object> config)
//            {
//                return new KafkaConsumer<byte[], byte[]>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())
//                {


//                    public void subscribe(Pattern topics, ConsumerRebalanceListener listener)
//                {
//                    base.subscribe(topics, new TheConsumerRebalanceListener(assignedTopics, listener));
//                }
//            };

//        }
//    });

//        streams.start();
//        TestUtils.WaitForCondition(() => assignedTopics.equals(expectedFirstAssignment), STREAM_TASKS_NOT_UPDATED);

//        CLUSTER.createTopic("TEST-TOPIC-2");

//        TestUtils.WaitForCondition(() => assignedTopics.equals(expectedSecondAssignment), STREAM_TASKS_NOT_UPDATED));

//    }

//    private string CreateTopic(int suffix)
//    {// throws InterruptedException
//        string outputTopic = "outputTopic_" + suffix;
//        CLUSTER.createTopic(outputTopic);
//        return outputTopic;
//    }

//    [Fact]
//    public void TestRegexMatchesTopicsAWhenDeleted()
//    {// throws Exception

//        Serde<string> stringSerde = Serdes.String();
//        List<string> expectedFirstAssignment = Array.asList("TEST-TOPIC-A", "TEST-TOPIC-B");
//        List<string> expectedSecondAssignment = Collections.singletonList("TEST-TOPIC-B");

//        CLUSTER.createTopics("TEST-TOPIC-A", "TEST-TOPIC-B");

//        StreamsBuilder builder = new StreamsBuilder();

//        KStream<string, string> pattern1Stream = builder.Stream(new Regex("TEST-TOPIC-[A-Z]", RegexOptions.Compiled));

//        pattern1Stream.To(outputTopic, Produced.With(stringSerde, stringSerde));

//        List<string> assignedTopics = new CopyOnWriteArrayList<>();
//        streams = new KafkaStreams(builder.Build(), streamsConfiguration, new DefaultKafkaClientSupplier()
//        {


//            public Consumer<byte[], byte[]> getConsumer(Dictionary<string, object> config)
//        {
//            return new KafkaConsumer<byte[], byte[]>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())
//            {


//                    public void subscribe(Pattern topics, ConsumerRebalanceListener listener)
//            {
//                base.subscribe(topics, new TheConsumerRebalanceListener(assignedTopics, listener));
//            }
//        };

//    }
//        });


//        streams.start();
//        TestUtils.WaitForCondition(() => assignedTopics.equals(expectedFirstAssignment), STREAM_TASKS_NOT_UPDATED);

//        CLUSTER.deleteTopic("TEST-TOPIC-A");

//        TestUtils.WaitForCondition(() => assignedTopics.equals(expectedSecondAssignment), STREAM_TASKS_NOT_UPDATED);
//    }

//    [Fact]
//    public void ShouldAddStateStoreToRegexDefinedSource()
//    {// throws InterruptedException

//        ProcessorSupplier<string, string> processorSupplier = new MockProcessorSupplier<>();
//        IStoreBuilder storeBuilder = new MockKeyValueStoreBuilder("testStateStore", false);
//        long thirtySecondTimeout = 30 * 1000;

//        TopologyWrapper topology = new TopologyWrapper();
//        topology.AddSource("ingest", new Regex("topic-\\d+", RegexOptions.Compiled));
//        topology.AddProcessor("my-processor", processorSupplier, "ingest");
//        topology.addStateStore(storeBuilder, "my-processor");

//        streams = new KafkaStreams(topology, streamsConfiguration);

//        try
//        {
//            streams.start();

//            TestCondition stateStoreNameBoundToSourceTopic = () =>
//            {
//                Dictionary<string, List<string>> stateStoreToSourceTopic = topology.getInternalBuilder().stateStoreNameToSourceTopics();
//                List<string> topicNamesList = stateStoreToSourceTopic.Get("testStateStore");
//                return topicNamesList != null && !topicNamesList.isEmpty() && topicNamesList.Get(0).equals("topic-1");
//            };

//            TestUtils.WaitForCondition(stateStoreNameBoundToSourceTopic, thirtySecondTimeout, "Did not find topic: [topic-1] connected to state store: [testStateStore]");

//        }
//        finally
//        {
//            streams.Close();
//        }
//    }

//    [Fact]
//    public void TestShouldReadFromRegexAndNamedTopics()
//    {// throws Exception

//        string topic1TestMessage = "topic-1 test";
//        string topic2TestMessage = "topic-2 test";
//        string topicATestMessage = "topic-A test";
//        string topicCTestMessage = "topic-C test";
//        string topicYTestMessage = "topic-Y test";
//        string topicZTestMessage = "topic-Z test";


//        Serde<string> stringSerde = Serdes.String();

//        StreamsBuilder builder = new StreamsBuilder();

//        KStream<string, string> pattern1Stream = builder.Stream(new Regex("topic-\\d", RegexOptions.Compiled));
//        KStream<string, string> pattern2Stream = builder.Stream(new Regex("topic-[A-D]", RegexOptions.Compiled));
//        KStream<string, string> namedTopicsStream = builder.Stream(Array.asList(TOPIC_Y, TOPIC_Z));

//        pattern1Stream.To(outputTopic, Produced.With(stringSerde, stringSerde));
//        pattern2Stream.To(outputTopic, Produced.With(stringSerde, stringSerde));
//        namedTopicsStream.To(outputTopic, Produced.With(stringSerde, stringSerde));

//        streams = new KafkaStreams(builder.Build(), streamsConfiguration);
//        streams.start();

//        StreamsConfig producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), Serdes.String().Serializer, Serdes.String().Serializer);

//        IntegrationTestUtils.produceValuesSynchronously(TOPIC_1, Collections.singleton(topic1TestMessage), producerConfig, mockTime);
//        IntegrationTestUtils.produceValuesSynchronously(TOPIC_2, Collections.singleton(topic2TestMessage), producerConfig, mockTime);
//        IntegrationTestUtils.produceValuesSynchronously(TOPIC_A, Collections.singleton(topicATestMessage), producerConfig, mockTime);
//        IntegrationTestUtils.produceValuesSynchronously(TOPIC_C, Collections.singleton(topicCTestMessage), producerConfig, mockTime);
//        IntegrationTestUtils.produceValuesSynchronously(TOPIC_Y, Collections.singleton(topicYTestMessage), producerConfig, mockTime);
//        IntegrationTestUtils.produceValuesSynchronously(TOPIC_Z, Collections.singleton(topicZTestMessage), producerConfig, mockTime);

//        StreamsConfig consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), Serdes.String().Deserializer, Serdes.String().Deserializer);

//        List<string> expectedReceivedValues = Array.asList(topicATestMessage, topic1TestMessage, topic2TestMessage, topicCTestMessage, topicYTestMessage, topicZTestMessage);
//        List<KeyValuePair<string, string>> receivedKeyValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, 6);
//        List<string> actualValues = new ArrayList<>(6);

//        foreach (KeyValuePair<string, string> receivedKeyValue in receivedKeyValues)
//        {
//            actualValues.Add(receivedKeyValue.value);
//        }

//        Collections.sort(actualValues);
//        Collections.sort(expectedReceivedValues);
//        Assert.Equal(actualValues, (expectedReceivedValues));
//    }

//    [Fact]
//    public void TestMultipleConsumersCanReadFromPartitionedTopic()
//    {// throws Exception

//        KafkaStreams partitionedStreamsLeader = null;
//        KafkaStreams partitionedStreamsFollower = null;
//        try
//        {
//            Serde<string> stringSerde = Serdes.String();
//            StreamsBuilder builderLeader = new StreamsBuilder();
//            StreamsBuilder builderFollower = new StreamsBuilder();
//            List<string> expectedAssignment = Array.asList(PARTITIONED_TOPIC_1, PARTITIONED_TOPIC_2);

//            KStream<string, string> partitionedStreamLeader = builderLeader.Stream(new Regex("partitioned-\\d", RegexOptions.Compiled));
//            KStream<string, string> partitionedStreamFollower = builderFollower.Stream(new Regex("partitioned-\\d", RegexOptions.Compiled));


//            partitionedStreamLeader.To(outputTopic, Produced.With(stringSerde, stringSerde));
//            partitionedStreamFollower.To(outputTopic, Produced.With(stringSerde, stringSerde));

//            List<string> leaderAssignment = new ArrayList<>();
//            List<string> followerAssignment = new ArrayList<>();

//            partitionedStreamsLeader = new KafkaStreams(builderLeader.Build(), streamsConfiguration, new DefaultKafkaClientSupplier()
//            {


//                public Consumer<byte[], byte[]> getConsumer(Dictionary<string, object> config)
//            {
//                return new KafkaConsumer<byte[], byte[]>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())
//                {


//                        public void subscribe(Pattern topics, ConsumerRebalanceListener listener)
//                {
//                    base.subscribe(topics, new TheConsumerRebalanceListener(leaderAssignment, listener));
//                }
//            };

//        }
//            });
//            partitionedStreamsFollower  = new KafkaStreams(builderFollower.Build(), streamsConfiguration, new DefaultKafkaClientSupplier()
//    {

//        public Consumer<byte[], byte[]> getConsumer(Dictionary<string, object> config)
//        {
//            return new KafkaConsumer<byte[], byte[]>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())
//            {


//                        public void subscribe(Pattern topics, ConsumerRebalanceListener listener)
//            {
//                base.subscribe(topics, new TheConsumerRebalanceListener(followerAssignment, listener));
//            }
//        };

//    }
//            });

//            partitionedStreamsLeader.start();
//            partitionedStreamsFollower.start();
//            TestUtils.WaitForCondition(() => followerAssignment.equals(expectedAssignment) && leaderAssignment.equals(expectedAssignment), "topic assignment not completed");
//        } finally {
//            if (partitionedStreamsLeader != null) {
//                partitionedStreamsLeader.Close();
//            }
//            if (partitionedStreamsFollower != null) {
//                partitionedStreamsFollower.Close();
//            }
//        }

//    }

//    [Fact]
//    public void TestNoMessagesSentExceptionFromOverlappingPatterns()
//    {// throws Exception

//        string fMessage = "fMessage";
//        string fooMessage = "fooMessage";
//        Serde<string> stringSerde = Serdes.String();
//        StreamsBuilder builder = new StreamsBuilder();

//        // overlapping patterns here, no messages should be sent as TopologyException
//        // will be thrown when the processor topology is built.
//        KStream<string, string> pattern1Stream = builder.Stream(new Regex("foo.*", RegexOptions.Compiled));
//        KStream<string, string> pattern2Stream = builder.Stream(new Regex("f.*", RegexOptions.Compiled));

//        pattern1Stream.To(outputTopic, Produced.With(stringSerde, stringSerde));
//        pattern2Stream.To(outputTopic, Produced.With(stringSerde, stringSerde));

//        AtomicBoolean expectError = new AtomicBoolean(false);

//        streams = new KafkaStreams(builder.Build(), streamsConfiguration);
//        streams.setStateListener((newState, oldState) =>
//        {
//            if (newState == KafkaStreams.State.ERROR)
//            {
//                expectError.set(true);
//            }
//        });
//        streams.start();

//        StreamsConfig producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), Serdes.String().Serializer, Serdes.String().Serializer);

//        IntegrationTestUtils.produceValuesSynchronously(FA_TOPIC, Collections.singleton(fMessage), producerConfig, mockTime);
//        IntegrationTestUtils.produceValuesSynchronously(FOO_TOPIC, Collections.singleton(fooMessage), producerConfig, mockTime);

//        StreamsConfig consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), Serdes.String().Deserializer, Serdes.String().Deserializer);
//        try
//        {
//            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, 2, 5000);
//            throw new IllegalStateException("This should not happen: an assertion error should have been thrown before this.");
//        }
//        catch (AssertionError e)
//        {
//            // this is fine
//        }

//        Assert.Equal(expectError.Get(), is (true));
//    }

//    private static class TheConsumerRebalanceListener : ConsumerRebalanceListener
//    {
//        private readonly List<string> assignedTopics;
//        private ConsumerRebalanceListener listener;

//        TheConsumerRebalanceListener(List<string> assignedTopics, ConsumerRebalanceListener listener)
//        {
//            this.assignedTopics = assignedTopics;
//            this.listener = listener;
//        }


//        public void OnPartitionsRevoked(Collection<TopicPartition> partitions)
//        {
//            assignedTopics.Clear();
//            listener.onPartitionsRevoked(partitions);
//        }


//        public void OnPartitionsAssigned(Collection<TopicPartition> partitions)
//        {
//            foreach (TopicPartition partition in partitions)
//            {
//                assignedTopics.Add(partition.Topic);
//            }
//            Collections.sort(assignedTopics);
//            listener.onPartitionsAssigned(partitions);
//        }
//    }

//}
//}