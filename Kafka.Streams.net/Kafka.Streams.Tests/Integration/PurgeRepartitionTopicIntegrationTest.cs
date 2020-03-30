namespace Kafka.Streams.Tests.Integration
{
    /*






    *

    *





    */






































    public class PurgeRepartitionTopicIntegrationTest
    {

        private static readonly int NUM_BROKERS = 1;

        private static readonly string INPUT_TOPIC = "input-stream";
        private static readonly string APPLICATION_ID = "restore-test";
        private static readonly string REPARTITION_TOPIC = APPLICATION_ID + "-KSTREAM-AGGREGATE-STATE-STORE-0000000002-repartition";

        private static Admin adminClient;
        private static KafkaStreams kafkaStreams;
        private static readonly int PURGE_INTERVAL_MS = 10;
        private static readonly int PURGE_SEGMENT_BYTES = 2000;


        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS, new Properties() {
        {
            put("log.retention.check.interval.ms", PURGE_INTERVAL_MS);
        put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, 0);
    }
    });

    private Time time = CLUSTER.time;

    private class RepartitionTopicCreatedWithExpectedConfigs : TestCondition
    {

        public bool ConditionMet()
        {
            try
            {
                HashSet<string> topics = adminClient.listTopics().names().get();

                if (!topics.Contains(REPARTITION_TOPIC))
                {
                    return false;
                }
            }
            catch (Exception e)
            {
                return false;
            }

            try
            {
                ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, REPARTITION_TOPIC);
                Config config = adminClient
                    .describeConfigs(Collections.singleton(resource))
                    .values()
                    .get(resource)
                    .get();
                return config.get(TopicConfig.CLEANUP_POLICY_CONFIG).Value.equals(TopicConfig.CLEANUP_POLICY_DELETE)
                        && config.get(TopicConfig.SEGMENT_MS_CONFIG).Value.equals(PURGE_INTERVAL_MS.toString())
                        && config.get(TopicConfig.SEGMENT_BYTES_CONFIG).Value.equals(PURGE_SEGMENT_BYTES.toString());
            }
            catch (Exception e)
            {
                return false;
            }
        }
    }

    private interface TopicSizeVerifier
    {
        bool Verify(long currentSize);
    }

    private class RepartitionTopicVerified : TestCondition
    {
        private TopicSizeVerifier verifier;

        RepartitionTopicVerified(TopicSizeVerifier verifier)
        {
            this.verifier = verifier;
        }


        public bool ConditionMet()
        {
            time.sleep(PURGE_INTERVAL_MS);

            try
            {
                Collection<DescribeLogDirsResponse.LogDirInfo> logDirInfo =
                    adminClient.describeLogDirs(Collections.singleton(0)).values().get(0).get().values();

                foreach (DescribeLogDirsResponse.LogDirInfo partitionInfo in logDirInfo)
                {
                    DescribeLogDirsResponse.ReplicaInfo replicaInfo =
                        partitionInfo.replicaInfos.get(new TopicPartition(REPARTITION_TOPIC, 0));
                    if (replicaInfo != null && verifier.Verify(replicaInfo.size))
                    {
                        return true;
                    }
                }
            }
            catch (Exception e)
            {
                // swallow
            }

            return false;
        }
    }


    public static void CreateTopics()
    {// throws Exception
        CLUSTER.createTopic(INPUT_TOPIC, 1, 1);
    }


    public void Setup()
    {
        // create admin client for verification
        Properties adminConfig = new Properties();
        adminConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        adminClient = Admin.create(adminConfig);

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, PURGE_INTERVAL_MS);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Int().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Int().getClass());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(APPLICATION_ID).getPath());
        streamsConfiguration.put(StreamsConfig.topicPrefix(TopicConfig.SEGMENT_MS_CONFIG), PURGE_INTERVAL_MS);
        streamsConfiguration.put(StreamsConfig.topicPrefix(TopicConfig.SEGMENT_BYTES_CONFIG), PURGE_SEGMENT_BYTES);
        streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), PURGE_SEGMENT_BYTES / 2);    // we cannot allow batch size larger than segment size

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC)
               .groupBy(MockMapper.selectKeyKeyValueMapper())
               .count();

        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration, time);
    }


    public void Shutdown()
    {
        if (kafkaStreams != null)
        {
            kafkaStreams.close(Duration.ofSeconds(30));
        }
    }

    [Xunit.Fact]
    public void ShouldRestoreState()
    {// throws Exception
        // produce some data to input topic
        List<KeyValuePair<int, int>> messages = new ArrayList<>();
        for (int i = 0; i < 1000; i++)
        {
            messages.add(new KeyValuePair<>(i, i));
        }
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC,
                messages,
                TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                        IntegerSerializer,
                        IntegerSerializer),
                time.milliseconds());

        kafkaStreams.start();

        TestUtils.waitForCondition(new RepartitionTopicCreatedWithExpectedConfigs(), 60000,
                "Repartition topic " + REPARTITION_TOPIC + " not created with the expected configs after 60000 ms.");

        TestUtils.waitForCondition(
            new RepartitionTopicVerified(currentSize => currentSize > 0),
            60000,
            "Repartition topic " + REPARTITION_TOPIC + " not received data after 60000 ms."
        );

        // we need long enough timeout to by-pass the log manager's InitialTaskDelayMs, which is hard-coded on server side
        TestUtils.waitForCondition(
            new RepartitionTopicVerified(currentSize => currentSize <= PURGE_SEGMENT_BYTES),
            60000,
            "Repartition topic " + REPARTITION_TOPIC + " not purged data after 60000 ms."
        );
    }
}
}
/*






*

*





*/

















































