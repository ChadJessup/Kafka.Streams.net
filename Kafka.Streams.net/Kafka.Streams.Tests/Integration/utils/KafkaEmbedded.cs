//using Kafka.Streams.Configs;
//using Microsoft.Extensions.Logging;
//using System.Collections.Generic;
//using System.IO;

//namespace Kafka.Streams.Tests.Integration.utils
//{
//    /**
//     * Runs an in-memory, "embedded" instance of a Kafka broker, which listens at `127.0.0.1:9092` by
//     * default.
//     * <p>
//     * Requires a running ZooKeeper instance to connect to.
//     */
//    public class KafkaEmbedded
//    {

//        private static ILogger log = LoggerFactory.getLogger(KafkaEmbedded);

//        private const string DEFAULT_ZK_CONNECT = "127.0.0.1:2181";

//        private StreamsConfig effectiveConfig;
//        private File logDir;
//        private TemporaryFolder tmpFolder;
//        private KafkaServer kafka;

//        /**
//         * Creates and starts an embedded Kafka broker.
//         *
//         * @param config Broker configuration settings.  Used to modify, for example, on which port the
//         *               broker should listen to.  Note that you cannot change the `log.dirs` setting
//         *               currently.
//         */

//        public KafkaEmbedded(StreamsConfig config, MockTime time)
//        { //throws IOException
//            tmpFolder = new TemporaryFolder();
//            tmpFolder.Create();
//            logDir = tmpFolder.newFolder();
//            effectiveConfig = EffectiveConfigFrom(config);
//            bool loggingEnabled = true;
//            KafkaConfig kafkaConfig = new KafkaConfig(effectiveConfig, loggingEnabled);
//            log.debug("Starting embedded Kafka broker (with log.dirs={} and ZK ensemble at {}) ...",
//                logDir, ZookeeperConnect());
//            kafka = TestUtils.createServer(kafkaConfig, time);
//            log.debug("Startup of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
//                BrokerList(), ZookeeperConnect());
//        }

//        /**
//         * Creates the configuration for starting the Kafka broker by merging default values with
//         * overwrites.
//         *
//         * @param initialConfig Broker configuration settings that override the default config.
//         */
//        private StreamsConfig EffectiveConfigFrom(StreamsConfig initialConfig)
//        {
//            StreamsConfig effectiveConfig = new StreamsConfig();
//            //effectiveConfig.Put(KafkaConfig$.MODULE$.BrokerIdProp(), 0);
//            //effectiveConfig.Put(KafkaConfig$.MODULE$.HostNameProp(), "localhost");
//            //effectiveConfig.Put(KafkaConfig$.MODULE$.PortProp(), "9092");
//            //effectiveConfig.Put(KafkaConfig$.MODULE$.NumPartitionsProp(), 1);
//            //effectiveConfig.Put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
//            //effectiveConfig.Put(KafkaConfig$.MODULE$.MessageMaxBytesProp(), 1000000);
//            //effectiveConfig.Put(KafkaConfig$.MODULE$.ControlledShutdownEnableProp(), true);
//            //effectiveConfig.Put(KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp(), 10000);

//            effectiveConfig.SetAll(initialConfig);
//            //effectiveConfig.setProperty(KafkaConfig$.MODULE$.LogDirProp(), logDir.FullName);
//            return effectiveConfig;
//        }

//        /**
//         * This broker's `metadata.broker.list` value.  Example: `localhost:9092`.
//         * <p>
//         * You can use this to tell Kafka producers and consumers how to connect to this instance.
//         */

//        public string BrokerList()
//        {
//            //object listenerConfig = effectiveConfig.Get(KafkaConfig$.MODULE$.InterBrokerListenerNameProp());
//            //return kafka.config().hostName() + ":" + kafka.boundPort(
//            //    new ListenerName(listenerConfig != null ? listenerConfig.ToString() : "PLAINTEXT"));
//            return "";
//        }


//        /**
//         * The ZooKeeper connection string aka `zookeeper.connect`.
//         */

//        public string ZookeeperConnect()
//        {
//            return effectiveConfig.getProperty("zookeeper.connect", DEFAULT_ZK_CONNECT);
//        }

//        /**
//         * Stop the broker.
//         */

//        public void Stop()
//        {
//            log.debug("Shutting down embedded Kafka broker at {} (with ZK ensemble at {}) ...",
//                BrokerList(), ZookeeperConnect());
//            kafka.shutdown();
//            kafka.awaitShutdown();
//            log.debug("Removing log dir at {} ...", logDir);
//            try
//            {
//                Utils.delete(logDir);
//            }
//            catch (IOException e)
//            {
//                throw new RuntimeException("", e);
//            }
//            tmpFolder.delete();
//            log.debug("Shutdown of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
//                BrokerList(), ZookeeperConnect());
//        }

//        /**
//         * Create a Kafka topic with 1 partition and a replication factor of 1.
//         *
//         * @param topic The Name of the topic.
//         */
//        public void CreateTopic(string topic)
//        {
//            createTopic(topic, 1, 1, Collections.emptyMap());
//        }

//        /**
//         * Create a Kafka topic with the given parameters.
//         *
//         * @param topic       The Name of the topic.
//         * @param partitions  The number of partitions for this topic.
//         * @param replication The replication factor for (the partitions of) this topic.
//         */
//        public void CreateTopic(string topic, int partitions, int replication)
//        {
//            createTopic(topic, partitions, replication, Collections.emptyMap());
//        }

//        /**
//         * Create a Kafka topic with the given parameters.
//         *
//         * @param topic       The Name of the topic.
//         * @param partitions  The number of partitions for this topic.
//         * @param replication The replication factor for (partitions of) this topic.
//         * @param topicConfig Additional topic-level configuration settings.
//         */
//        public void CreateTopic(
//            string topic,
//            int partitions,
//            int replication,
//            Dictionary<string, string> topicConfig)
//        {
//            log.debug("Creating topic { Name: {}, partitions: {}, replication: {}, config: {} }",
//                topic, partitions, replication, topicConfig);
//            NewTopic newTopic = new NewTopic(topic, partitions, (short)replication);
//            newTopic.configs(topicConfig);

//            Admin adminClient = createAdminClient();
//            adminClient.createTopics(Collections.singletonList(newTopic)).All().Get();
//        }

//        public Admin createAdminClient()
//        {
//            StreamsConfig adminClientConfig = new StreamsConfig();
//            adminClientConfig.Put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BrokerList());
//            //object listeners = effectiveConfig.Get(KafkaConfig$.MODULE$.ListenersProp());
//            //if (listeners != null && listeners.ToString().Contains("SSL"))
//            //{
//            //    adminClientConfig.Put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, effectiveConfig.Get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
//            //    adminClientConfig.Put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ((Password)effectiveConfig.Get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).Value);
//            //    adminClientConfig.Put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
//            //}
//            return Admin.Create(adminClientConfig);
//        }


//        public void deleteTopic(string topic)
//        {
//            log.debug("Deleting topic { Name: {} }", topic);
//            Admin adminClient = createAdminClient();
//            adminClient.deleteTopics(Collections.singletonList(topic)).All().Get();
//        }


//        public KafkaServer KafkaServer()
//        {
//            return kafka;
//        }
//    }
//}
