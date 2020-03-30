namespace Kafka.Streams.Tests.Integration.utils
{
}
///*






// *

// *





// */

























///**
// * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance and supplied number of Kafka brokers.
// */
//public class EmbeddedKafkaCluster : ExternalResource {

//    private static Logger log = LoggerFactory.getLogger(EmbeddedKafkaCluster);
//    private static int DEFAULT_BROKER_PORT = 0; // 0 results in a random port being selected
//    private static int TOPIC_CREATION_TIMEOUT = 30000;
//    private static int TOPIC_DELETION_TIMEOUT = 30000;
//    private EmbeddedZookeeper zookeeper = null;
//    private KafkaEmbedded[] brokers;

//    private Properties brokerConfig;
//    public MockTime time;

//    public EmbeddedKafkaCluster(int numBrokers) {
//        this(numBrokers, new Properties());
//    }

//    public EmbeddedKafkaCluster(int numBrokers,
//                                Properties brokerConfig) {
//        this(numBrokers, brokerConfig, System.currentTimeMillis());
//    }

//    public EmbeddedKafkaCluster(int numBrokers,
//                                Properties brokerConfig,
//                                long mockTimeMillisStart) {
//        this(numBrokers, brokerConfig, mockTimeMillisStart, System.nanoTime());
//    }

//    public EmbeddedKafkaCluster(int numBrokers,
//                                Properties brokerConfig,
//                                long mockTimeMillisStart,
//                                long mockTimeNanoStart) {
//        brokers = new KafkaEmbedded[numBrokers];
//        this.brokerConfig = brokerConfig;
//        time = new MockTime(mockTimeMillisStart, mockTimeNanoStart);
//    }

//    /**
//     * Creates and starts a Kafka cluster.
//     */
//    public void start(){ //throws IOException, InterruptedException
//        log.debug("Initiating embedded Kafka cluster startup");
//        log.debug("Starting a ZooKeeper instance");
//        zookeeper = new EmbeddedZookeeper();
//        log.debug("ZooKeeper instance is running at {}", zKConnectString());

//        brokerConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zKConnectString());
//        brokerConfig.put(KafkaConfig$.MODULE$.PortProp(), DEFAULT_BROKER_PORT);
//        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
//        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.LogCleanerDedupeBufferSizeProp(), 2 * 1024 * 1024L);
//        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.GroupMinSessionTimeoutMsProp(), 0);
//        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.GroupInitialRebalanceDelayMsProp(), 0);
//        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), (short) 1);
//        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);

//        for (int i = 0; i < brokers.Length; i++) {
//            brokerConfig.put(KafkaConfig$.MODULE$.BrokerIdProp(), i);
//            log.debug("Starting a Kafka instance on port {} ...", brokerConfig.get(KafkaConfig$.MODULE$.PortProp()));
//            brokers[i] = new KafkaEmbedded(brokerConfig, time);

//            log.debug("Kafka instance is running at {}, connected to ZooKeeper at {}",
//                brokers[i].brokerList(), brokers[i].zookeeperConnect());
//        }
//    }

//    private void putIfAbsent(Properties props, string propertyKey, object propertyValue) {
//        if (!props.containsKey(propertyKey)) {
//            brokerConfig.put(propertyKey, propertyValue);
//        }
//    }

//    /**
//     * Stop the Kafka cluster.
//     */
//    private void stop() {
//        foreach (KafkaEmbedded broker in brokers) {
//            broker.stop();
//        }
//        zookeeper.shutdown();
//    }

//    /**
//     * The ZooKeeper connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
//     * Example: `127.0.0.1:2181`.
//     * <p>
//     * You can use this to e.g. tell Kafka brokers how to connect to this instance.
//     */
//    public string zKConnectString() {
//        return "127.0.0.1:" + zookeeper.port();
//    }

//    /**
//     * This cluster's `bootstrap.servers` value.  Example: `127.0.0.1:9092`.
//     * <p>
//     * You can use this to tell Kafka producers how to connect to this cluster.
//     */
//    public string bootstrapServers() {
//        return brokers[0].brokerList();
//    }


//    protected void before() {// throws Throwable
//        start();
//    }


//    protected void after() {
//        stop();
//    }

//    /**
//     * Create multiple Kafka topics each with 1 partition and a replication factor of 1.
//     *
//     * @param topics The name of the topics.
//     */
//    public void createTopics(string... topics) {// throws InterruptedException
//        foreach (string topic in topics) {
//            createTopic(topic, 1, 1, Collections.emptyMap());
//        }
//    }

//    /**
//     * Create a Kafka topic with 1 partition and a replication factor of 1.
//     *
//     * @param topic The name of the topic.
//     */
//    public void createTopic(string topic) {// throws InterruptedException
//        createTopic(topic, 1, 1, Collections.emptyMap());
//    }

//    /**
//     * Create a Kafka topic with the given parameters.
//     *
//     * @param topic       The name of the topic.
//     * @param partitions  The number of partitions for this topic.
//     * @param replication The replication factor for (the partitions of) this topic.
//     */
//    public void createTopic(string topic, int partitions, int replication) {// throws InterruptedException
//        createTopic(topic, partitions, replication, Collections.emptyMap());
//    }

//    /**
//     * Create a Kafka topic with the given parameters.
//     *
//     * @param topic       The name of the topic.
//     * @param partitions  The number of partitions for this topic.
//     * @param replication The replication factor for (partitions of) this topic.
//     * @param topicConfig Additional topic-level configuration settings.
//     */
//    public void createTopic(string topic,
//                            int partitions,
//                            int replication,
//                            Dictionary<string, string> topicConfig) {// throws InterruptedException
//        brokers[0].createTopic(topic, partitions, replication, topicConfig);
//        List<TopicPartition> topicPartitions = new ArrayList<>();
//        for (int partition = 0; partition < partitions; partition++) {
//            topicPartitions.add(new TopicPartition(topic, partition));
//        }
//        IntegrationTestUtils.waitForTopicPartitions(brokers(), topicPartitions, TOPIC_CREATION_TIMEOUT);
//    }

//    /**
//     * Deletes a topic returns immediately.
//     *
//     * @param topic the name of the topic
//     */
//    public void deleteTopic(string topic) {// throws InterruptedException
//        deleteTopicsAndWait(-1L, topic);
//    }

//    /**
//     * Deletes a topic and blocks for max 30 sec until the topic got deleted.
//     *
//     * @param topic the name of the topic
//     */
//    public void deleteTopicAndWait(string topic) {// throws InterruptedException
//        deleteTopicsAndWait(TOPIC_DELETION_TIMEOUT, topic);
//    }

//    /**
//     * Deletes a topic and blocks until the topic got deleted.
//     *
//     * @param timeoutMs the max time to wait for the topic to be deleted (does not block if {@code <= 0})
//     * @param topic the name of the topic
//     */
//    public void deleteTopicAndWait(long timeoutMs, string topic) {// throws InterruptedException
//        deleteTopicsAndWait(timeoutMs, topic);
//    }

//    /**
//     * Deletes multiple topics returns immediately.
//     *
//     * @param topics the name of the topics
//     */
//    public void deleteTopics(string... topics) {// throws InterruptedException
//        deleteTopicsAndWait(-1, topics);
//    }

//    /**
//     * Deletes multiple topics and blocks for max 30 sec until all topics got deleted.
//     *
//     * @param topics the name of the topics
//     */
//    public void deleteTopicsAndWait(string... topics) {// throws InterruptedException
//        deleteTopicsAndWait(TOPIC_DELETION_TIMEOUT, topics);
//    }

//    /**
//     * Deletes multiple topics and blocks until all topics got deleted.
//     *
//     * @param timeoutMs the max time to wait for the topics to be deleted (does not block if {@code <= 0})
//     * @param topics the name of the topics
//     */
//    public void deleteTopicsAndWait(long timeoutMs, string... topics) {// throws InterruptedException
//        foreach (string topic in topics) {
//            try {
//                brokers[0].deleteTopic(topic);
//            } catch (UnknownTopicOrPartitionException e) { }
//        }

//        if (timeoutMs > 0) {
//            TestUtils.waitForCondition(new TopicsDeletedCondition(topics), timeoutMs, "Topics not deleted after " + timeoutMs + " milli seconds.");
//        }
//    }

//    /**
//     * Deletes all topics and blocks until all topics got deleted.
//     *
//     * @param timeoutMs the max time to wait for the topics to be deleted (does not block if {@code <= 0})
//     */
//    public void deleteAllTopicsAndWait(long timeoutMs) {// throws InterruptedException
//        HashSet<string> topics = JavaConverters.setAsJavaSetConverter(brokers[0].kafkaServer().zkClient().getAllTopicsInCluster()).asJava();
//        foreach (string topic in topics) {
//            try {
//                brokers[0].deleteTopic(topic);
//            } catch (UnknownTopicOrPartitionException e) { }
//        }

//        if (timeoutMs > 0) {
//            TestUtils.waitForCondition(new TopicsDeletedCondition(topics), timeoutMs, "Topics not deleted after " + timeoutMs + " milli seconds.");
//        }
//    }

//    public void deleteAndRecreateTopics(string... topics) {// throws InterruptedException
//        deleteTopicsAndWait(TOPIC_DELETION_TIMEOUT, topics);
//        createTopics(topics);
//    }

//    public void deleteAndRecreateTopics(long timeoutMs, string... topics) {// throws InterruptedException
//        deleteTopicsAndWait(timeoutMs, topics);
//        createTopics(topics);
//    }

//    public void waitForRemainingTopics(long timeoutMs, string... topics) {// throws InterruptedException
//        TestUtils.waitForCondition(new TopicsRemainingCondition(topics), timeoutMs, "Topics are not expected after " + timeoutMs + " milli seconds.");
//    }

//    private class TopicsDeletedCondition : TestCondition {
//        HashSet<string> deletedTopics = new HashSet<>();

//        private TopicsDeletedCondition(string... topics) {
//            Collections.addAll(deletedTopics, topics);
//        }

//        private TopicsDeletedCondition(Collection<string> topics) {
//            deletedTopics.addAll(topics);
//        }


//        public bool conditionMet() {
//            HashSet<string> allTopics = new HashSet<>(
//                    JavaConverters.setAsJavaSetConverter(brokers[0].kafkaServer().zkClient().getAllTopicsInCluster()).asJava());
//            return !allTopics.removeAll(deletedTopics);
//        }
//    }

//    private class TopicsRemainingCondition : TestCondition {
//        HashSet<string> remainingTopics = new HashSet<>();

//        private TopicsRemainingCondition(string... topics) {
//            Collections.addAll(remainingTopics, topics);
//        }


//        public bool conditionMet() {
//            HashSet<string> allTopics = JavaConverters.setAsJavaSetConverter(brokers[0].kafkaServer().zkClient().getAllTopicsInCluster()).asJava();
//            return allTopics.equals(remainingTopics);
//        }
//    }

//    private List<KafkaServer> brokers() {
//        List<KafkaServer> servers = new ArrayList<>();
//        foreach (KafkaEmbedded broker in brokers) {
//            servers.add(broker.kafkaServer());
//        }
//        return servers;
//    }
//}
