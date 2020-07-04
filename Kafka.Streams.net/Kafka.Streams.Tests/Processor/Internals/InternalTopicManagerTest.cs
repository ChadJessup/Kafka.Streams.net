//using Kafka.Common;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Errors;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.Temporary;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    public class InternalTopicManagerTest
//    {
//        private Node broker1 = new Node(0, "dummyHost-1", 1234);
//        private Node broker2 = new Node(1, "dummyHost-2", 1234);
//        private readonly List<Node> cluster = new List<Node>(2);
//        //   {
//        //       add(broker1),
//        //       add(broker2),

//        private string topic = "test_topic";
//        private string topic2 = "test_topic_2";
//        private string topic3 = "test_topic_3";
//        private List<Node> singleReplica { get; } = Collections.singletonList(broker1);

//        private MockAdminClient mockAdminClient;
//        private InternalTopicManager internalTopicManager;

//        private Dictionary<string, object> config = new Dictionary<string, object>();
//        //        {
//        //        {
//        //            Put(StreamsConfig.ApplicationIdConfig, "app-id");
//        //        Put(StreamsConfig.BootstrapServersConfig, broker1.host() + ":" + broker1.port());
//        //        Put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
//        //        Put(StreamsConfig.adminClientPrefix(StreamsConfig.RETRIES_CONFIG), 1);
//        //        Put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), 16384);
//        //    }
//        //};


//        public void Init()
//        {
//            mockAdminClient = new MockAdminClient(cluster, broker1);
//            internalTopicManager = new InternalTopicManager(
//                mockAdminClient,
//                new StreamsConfig(config));
//        }


//        public void Shutdown()
//        {
//            mockAdminClient.Close();
//        }

//        [Fact]
//        public void ShouldReturnCorrectPartitionCounts()
//        {
//            mockAdminClient.addTopic(
//                false,
//                topic,
//                Collections.singletonList(new TopicPartitionInfo(0, broker1, singleReplica, Collections.emptyList<Node>())),
//                null);
//            Assert.Equal(Collections.singletonMap(topic, 1), internalTopicManager.getNumPartitions(Collections.singleton(topic)));
//        }

//        [Fact]
//        public void ShouldCreateRequiredTopics()
//        {// throws Exception
//            InternalTopicConfig topicConfig = new RepartitionTopicConfig(topic, Collections.emptyMap<string, string>());
//            topicConfig.SetNumberOfPartitions(1);
//            InternalTopicConfig topicConfig2 = new UnwindowedChangelogTopicConfig(topic2, Collections.emptyMap<string, string>());
//            topicConfig2.SetNumberOfPartitions(1);
//            InternalTopicConfig topicConfig3 = new WindowedChangelogTopicConfig(topic3, Collections.emptyMap<string, string>());
//            topicConfig3.SetNumberOfPartitions(1);

//            internalTopicManager.makeReady(Collections.singletonMap(topic, topicConfig));
//            internalTopicManager.makeReady(Collections.singletonMap(topic2, topicConfig2));
//            internalTopicManager.makeReady(Collections.singletonMap(topic3, topicConfig3));

//            //            Assert.Equal(Utils.mkSet(topic, topic2, topic3), mockAdminClient.listTopics().names().Get());
//            //            {
//            //            Assert.Equal(new TopicDescription(topic, false, new ArrayList<TopicPartitionInfo>() {
//            //                add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.<Node>emptyList()));
//            //        }
//            //    }), mockAdminClient.describeTopics(Collections.singleton(topic)).values().Get(topic).Get());
//            //        Assert.Equal(new TopicDescription(topic2, false, new ArrayList<TopicPartitionInfo>() {
//            //            {
//            //                add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.<Node>emptyList()));
//            //            }
//            //        }), mockAdminClient.describeTopics(Collections.singleton(topic2)).values().Get(topic2).Get());
//            //        Assert.Equal(new TopicDescription(topic3, false, new ArrayList<TopicPartitionInfo>() {
//            //            {
//            //                add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.<Node>emptyList()));
//            //            }
//            //        }), mockAdminClient.describeTopics(Collections.singleton(topic3)).values().Get(topic3).Get());

//            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
//            ConfigResource resource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2);
//            ConfigResource resource3 = new ConfigResource(ConfigResource.Type.TOPIC, topic3);

//            Assert.Equal(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE), mockAdminClient.describeConfigs(Collections.singleton(resource)).values().Get(resource).Get().Get(TopicConfig.CLEANUP_POLICY_CONFIG));
//            Assert.Equal(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT), mockAdminClient.describeConfigs(Collections.singleton(resource2)).values().Get(resource2).Get().Get(TopicConfig.CLEANUP_POLICY_CONFIG));
//            Assert.Equal(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE), mockAdminClient.describeConfigs(Collections.singleton(resource3)).values().Get(resource3).Get().Get(TopicConfig.CLEANUP_POLICY_CONFIG));

//        }

//        [Fact]
//        public void ShouldNotCreateTopicIfExistsWithDifferentPartitions()
//        {
//            mockAdminClient.addTopic(
//                false,
//                topic);
//            //                new ArrayList<TopicPartitionInfo>() {
//            //                {
//            //                    add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.<Node>emptyList()));
//            //            add(new TopicPartitionInfo(1, broker1, singleReplica, Collections.< Node > emptyList()));
//            //        }
//            //    },
//            //            null);

//            try
//            {
//                InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic, Collections.emptyMap<string, string>());
//                internalTopicConfig.setNumberOfPartitions(1);
//                internalTopicManager.makeReady(Collections.singletonMap(topic, internalTopicConfig));
//                Assert.True(false, "Should have thrown StreamsException");
//            }
//            catch (StreamsException expected) { /* pass */ }
//        }

//        [Fact]
//        public void ShouldNotThrowExceptionIfExistsWithDifferentReplication()
//        {
//            mockAdminClient.addTopic(
//                false,
//                topic,
//                Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList<Node>())),
//                null);

//            // attempt to Create it again with replication 1
//            InternalTopicManager internalTopicManager2 = new InternalTopicManager(
//                mockAdminClient,
//                new StreamsConfig(config));

//            InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic, Collections.emptyMap());
//            internalTopicConfig.SetNumberOfPartitions(1);
//            internalTopicManager2.makeReady(Collections.singletonMap(topic, internalTopicConfig));
//        }

//        [Fact]
//        public void ShouldNotThrowExceptionForEmptyTopicMap()
//        {
//            internalTopicManager.makeReady(Collections.emptyMap());
//        }

//        [Fact]
//        public void ShouldExhaustRetriesOnTimeoutExceptionForMakeReady()
//        {
//            mockAdminClient.timeoutNextRequest(1);

//            InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic, Collections.emptyMap());
//            internalTopicConfig.SetNumberOfPartitions(1);
//            try
//            {
//                internalTopicManager.makeReady(Collections.singletonMap(topic, internalTopicConfig));
//                Assert.True(false, "Should have thrown StreamsException.");
//            }
//            catch (StreamsException expected)
//            {
//                //Assert.Equal(TimeoutException, expected.getCause().GetType());
//            }
//        }

//        [Fact]
//        public void ShouldLogWhenTopicNotFoundAndNotThrowException()
//        {
//            //LogCaptureAppender.setClassLoggerToDebug(InternalTopicManager);
//            //LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();
//            mockAdminClient.addTopic(
//                false,
//                topic,
//                Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList<string>())),
//                null);

//            InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic, Collections.emptyMap<string, string>());
//            internalTopicConfig.SetNumberOfPartitions(1);

//            InternalTopicConfig internalTopicConfigII = new RepartitionTopicConfig("internal-topic", Collections.emptyMap<string, string>());
//            internalTopicConfigII.SetNumberOfPartitions(1);

//            Dictionary<string, InternalTopicConfig> topicConfigMap = new Dictionary<string, InternalTopicConfig>();
//            topicConfigMap.Put(topic, internalTopicConfig);
//            topicConfigMap.Put("internal-topic", internalTopicConfigII);


//            internalTopicManager.makeReady(topicConfigMap);
//            bool foundExpectedMessage = false;
//            foreach (string message in appender.getMessages())
//            {
//                foundExpectedMessage |= message.Contains("Topic internal-topic is unknown or not found, hence not existed yet.");
//            }
//            Assert.True(foundExpectedMessage);

//        }

//        [Fact]
//        public void ShouldExhaustRetriesOnMarkedForDeletionTopic()
//        {
//            mockAdminClient.addTopic(
//                false,
//                topic,
//                Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList<string>())),
//                null);
//            mockAdminClient.markTopicForDeletion(topic);

//            InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic, Collections.emptyMap<string, string>());
//            internalTopicConfig.SetNumberOfPartitions(1);
//            try
//            {
//                internalTopicManager.makeReady(Collections.singletonMap(topic, internalTopicConfig));
//                Assert.True(false, "Should have thrown StreamsException.");
//            }
//            catch (StreamsException expected)
//            {
//                //Assert.Null(expected.getCause());
//                Assert.StartsWith("Could not Create topics after 1 retries", expected.Message);
//            }
//        }

//    }
//}
