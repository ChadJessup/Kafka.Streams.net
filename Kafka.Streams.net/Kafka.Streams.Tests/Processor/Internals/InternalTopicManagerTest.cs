/*






 *

 *





 */






























using Kafka.Streams.Processors.Internals;
using Xunit;

public class InternalTopicManagerTest {

    private Node broker1 = new Node(0, "dummyHost-1", 1234);
    private Node broker2 = new Node(1, "dummyHost-2", 1234);
    private List<Node> cluster = new ArrayList<Node>(2) {
        {
            add(broker1);
            add(broker2);
        }
    };
    private string topic = "test_topic";
    private string topic2 = "test_topic_2";
    private string topic3 = "test_topic_3";
    private List<Node> singleReplica = Collections.singletonList(broker1);

    private MockAdminClient mockAdminClient;
    private InternalTopicManager internalTopicManager;

    private Dictionary<string, object> config = new HashMap<string, object>() {
        {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id");
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker1.host() + ":" + broker1.port());
            put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
            put(StreamsConfig.adminClientPrefix(StreamsConfig.RETRIES_CONFIG), 1);
            put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), 16384);
        }
    };

    
    public void Init() {
        mockAdminClient = new MockAdminClient(cluster, broker1);
        internalTopicManager = new InternalTopicManager(
            mockAdminClient,
            new StreamsConfig(config));
    }

    
    public void Shutdown() {
        mockAdminClient.close();
    }

    [Xunit.Fact]
    public void ShouldReturnCorrectPartitionCounts() {
        mockAdminClient.addTopic(
            false,
            topic,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, singleReplica, Collections.<Node>emptyList())),
            null);
        Assert.Equal(Collections.singletonMap(topic, 1), internalTopicManager.getNumPartitions(Collections.singleton(topic)));
    }

    [Xunit.Fact]
    public void ShouldCreateRequiredTopics() {// throws Exception
        InternalTopicConfig topicConfig = new RepartitionTopicConfig(topic, Collections.<string, string>emptyMap());
        topicConfig.setNumberOfPartitions(1);
        InternalTopicConfig topicConfig2 = new UnwindowedChangelogTopicConfig(topic2, Collections.<string, string>emptyMap());
        topicConfig2.setNumberOfPartitions(1);
        InternalTopicConfig topicConfig3 = new WindowedChangelogTopicConfig(topic3, Collections.<string, string>emptyMap());
        topicConfig3.setNumberOfPartitions(1);

        internalTopicManager.makeReady(Collections.singletonMap(topic, topicConfig));
        internalTopicManager.makeReady(Collections.singletonMap(topic2, topicConfig2));
        internalTopicManager.makeReady(Collections.singletonMap(topic3, topicConfig3));

        Assert.Equal(Utils.mkSet(topic, topic2, topic3), mockAdminClient.listTopics().names().get());
        Assert.Equal(new TopicDescription(topic, false, new ArrayList<TopicPartitionInfo>() {
            {
                add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.<Node>emptyList()));
            }
        }), mockAdminClient.describeTopics(Collections.singleton(topic)).values().get(topic).get());
        Assert.Equal(new TopicDescription(topic2, false, new ArrayList<TopicPartitionInfo>() {
            {
                add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.<Node>emptyList()));
            }
        }), mockAdminClient.describeTopics(Collections.singleton(topic2)).values().get(topic2).get());
        Assert.Equal(new TopicDescription(topic3, false, new ArrayList<TopicPartitionInfo>() {
            {
                add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.<Node>emptyList()));
            }
        }), mockAdminClient.describeTopics(Collections.singleton(topic3)).values().get(topic3).get());

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        ConfigResource resource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2);
        ConfigResource resource3 = new ConfigResource(ConfigResource.Type.TOPIC, topic3);

        Assert.Equal(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE), mockAdminClient.describeConfigs(Collections.singleton(resource)).values().get(resource).get().get(TopicConfig.CLEANUP_POLICY_CONFIG));
        Assert.Equal(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT), mockAdminClient.describeConfigs(Collections.singleton(resource2)).values().get(resource2).get().get(TopicConfig.CLEANUP_POLICY_CONFIG));
        Assert.Equal(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE), mockAdminClient.describeConfigs(Collections.singleton(resource3)).values().get(resource3).get().get(TopicConfig.CLEANUP_POLICY_CONFIG));

    }

    [Xunit.Fact]
    public void ShouldNotCreateTopicIfExistsWithDifferentPartitions() {
        mockAdminClient.addTopic(
            false,
            topic,
            new ArrayList<TopicPartitionInfo>() {
                {
                    add(new TopicPartitionInfo(0, broker1, singleReplica, Collections.<Node>emptyList()));
                    add(new TopicPartitionInfo(1, broker1, singleReplica, Collections.<Node>emptyList()));
                }
            },
            null);

        try {
            InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic, Collections.<string, string>emptyMap());
            internalTopicConfig.setNumberOfPartitions(1);
            internalTopicManager.makeReady(Collections.singletonMap(topic, internalTopicConfig));
            Assert.True(false, "Should have thrown StreamsException");
        } catch (StreamsException expected) { /* pass */ }
    }

    [Xunit.Fact]
    public void ShouldNotThrowExceptionIfExistsWithDifferentReplication() {
        mockAdminClient.addTopic(
            false,
            topic,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.<Node>emptyList())),
            null);

        // attempt to create it again with replication 1
        InternalTopicManager internalTopicManager2 = new InternalTopicManager(
            mockAdminClient,
            new StreamsConfig(config));

        InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);
        internalTopicManager2.makeReady(Collections.singletonMap(topic, internalTopicConfig));
    }

    [Xunit.Fact]
    public void ShouldNotThrowExceptionForEmptyTopicMap() {
        internalTopicManager.makeReady(Collections.emptyMap());
    }

    [Xunit.Fact]
    public void ShouldExhaustRetriesOnTimeoutExceptionForMakeReady() {
        mockAdminClient.timeoutNextRequest(1);

        InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);
        try {
            internalTopicManager.makeReady(Collections.singletonMap(topic, internalTopicConfig));
            Assert.True(false, "Should have thrown StreamsException.");
        } catch (StreamsException expected) {
            Assert.Equal(TimeoutException, expected.getCause().getClass());
        }
    }

    [Xunit.Fact]
    public void ShouldLogWhenTopicNotFoundAndNotThrowException() {
        LogCaptureAppender.setClassLoggerToDebug(InternalTopicManager);
        LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();
        mockAdminClient.addTopic(
            false,
            topic,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList())),
            null);

        InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);

        InternalTopicConfig internalTopicConfigII = new RepartitionTopicConfig("internal-topic", Collections.emptyMap());
        internalTopicConfigII.setNumberOfPartitions(1);

        Dictionary<string, InternalTopicConfig> topicConfigMap = new HashMap<>();
        topicConfigMap.put(topic, internalTopicConfig);
        topicConfigMap.put("internal-topic", internalTopicConfigII);


        internalTopicManager.makeReady(topicConfigMap);
        bool foundExpectedMessage = false;
        foreach (string message in appender.getMessages()) {
            foundExpectedMessage |= message.Contains("Topic internal-topic is unknown or not found, hence not existed yet.");
        }
        Assert.True(foundExpectedMessage);

    }

    [Xunit.Fact]
    public void ShouldExhaustRetriesOnMarkedForDeletionTopic() {
        mockAdminClient.addTopic(
            false,
            topic,
            Collections.singletonList(new TopicPartitionInfo(0, broker1, cluster, Collections.emptyList())),
            null);
        mockAdminClient.markTopicForDeletion(topic);

        InternalTopicConfig internalTopicConfig = new RepartitionTopicConfig(topic, Collections.emptyMap());
        internalTopicConfig.setNumberOfPartitions(1);
        try {
            internalTopicManager.makeReady(Collections.singletonMap(topic, internalTopicConfig));
            Assert.True(false, "Should have thrown StreamsException.");
        } catch (StreamsException expected) {
            assertNull(expected.getCause());
            Assert.True(expected.getMessage().startsWith("Could not create topics after 1 retries"));
        }
    }

}
