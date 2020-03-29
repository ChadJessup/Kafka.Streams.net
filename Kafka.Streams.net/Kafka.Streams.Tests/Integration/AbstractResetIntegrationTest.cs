/*






 *

 *





 */























































using Kafka.Streams.KStream;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Topologies;
using System.Collections.Generic;
using Xunit;

public abstract class AbstractResetIntegrationTest
{
    static string testId;
    static EmbeddedKafkaCluster cluster;

    private static MockTime mockTime;
    private static KafkaStreams streams;
    private static Admin adminClient = null;

    abstract Dictionary<string, object> getClientSslConfig();

    @AfterClass
    public static void afterClassCleanup()
    {
        if (adminClient != null)
        {
            adminClient.close(Duration.ofSeconds(10));
            adminClient = null;
        }
    }

    private string appID = "abstract-reset-integration-test";
    private Properties commonClientConfig;
    private Properties streamsConfig;
    private Properties producerConfig;
    private Properties resultConsumerConfig;

    private void prepareEnvironment()
    {
        if (adminClient == null)
        {
            adminClient = Admin.create(commonClientConfig);
        }

        bool timeSet = false;
        while (!timeSet)
        {
            timeSet = setCurrentTime();
        }
    }

    private bool setCurrentTime()
    {
        bool currentTimeSet = false;
        try
        {
            mockTime = cluster.time;
            // we align time to seconds to get clean window boundaries and thus ensure the same result for each run
            // otherwise, input records could fall into different windows for different runs depending on the initial mock time
            long alignedTime = (System.currentTimeMillis() / 1000 + 1) * 1000;
            mockTime.setCurrentTimeMs(alignedTime);
            currentTimeSet = true;
        }
        catch (IllegalArgumentException e)
        {
            // don't care will retry until set
        }
        return currentTimeSet;
    }

    private void prepareConfigs()
    {
        commonClientConfig = new Properties();
        commonClientConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

        Dictionary<string, object> sslConfig = getClientSslConfig();
        if (sslConfig != null)
        {
            commonClientConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
            commonClientConfig.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ((Password)sslConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).Value);
            commonClientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        }

        producerConfig = new Properties();
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer);
        producerConfig.putAll(commonClientConfig);

        resultConsumerConfig = new Properties();
        resultConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, testId + "-result-consumer");
        resultConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        resultConsumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer);
        resultConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer);
        resultConsumerConfig.putAll(commonClientConfig);

        streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, testFolder.getRoot().getPath());
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + STREAMS_CONSUMER_TIMEOUT);
        streamsConfig.putAll(commonClientConfig);
    }


    public TemporaryFolder testFolder = new TemporaryFolder(TestUtils.tempDirectory());

    private static string INPUT_TOPIC = "inputTopic";
    private static string OUTPUT_TOPIC = "outputTopic";
    private static string OUTPUT_TOPIC_2 = "outputTopic2";
    private static string OUTPUT_TOPIC_2_RERUN = "outputTopic2_rerun";
    private static string INTERMEDIATE_USER_TOPIC = "userTopic";
    private static string NON_EXISTING_TOPIC = "nonExistingTopic";

    private static long STREAMS_CONSUMER_TIMEOUT = 2000L;
    private static long CLEANUP_CONSUMER_TIMEOUT = 2000L;
    private static int TIMEOUT_MULTIPLIER = 15;

    private class ConsumerGroupInactiveCondition : TestCondition
    {

        public bool conditionMet()
        {
            try
            {
                ConsumerGroupDescription groupDescription = adminClient.describeConsumerGroups(Collections.singletonList(appID)).describedGroups().get(appID).get();
                return groupDescription.members().isEmpty();
            }
            catch (ExecutionException | InterruptedException e) {
                return false;
            }
            }
        }

        void prepareTest()
        {// throws Exception
            prepareConfigs();
            prepareEnvironment();

            // busy wait until cluster (ie, ConsumerGroupCoordinator) is available
            TestUtils.waitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                    "Test consumer group " + appID + " still active even after waiting " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

            cluster.deleteAndRecreateTopics(INPUT_TOPIC, OUTPUT_TOPIC, OUTPUT_TOPIC_2, OUTPUT_TOPIC_2_RERUN);

            add10InputElements();
        }

        void cleanupTest()
        {// throws Exception
            if (streams != null)
            {
                streams.close(Duration.ofSeconds(30));
            }
            IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);
        }

        private void add10InputElements()
        {// throws java.util.concurrent.ExecutionException, InterruptedException
            List<KeyValuePair<long, string>> records = Array.asList(KeyValuePair.Create(0L, "aaa"),
                                                                       KeyValuePair.Create(1L, "bbb"),
                                                                       KeyValuePair.Create(0L, "ccc"),
                                                                       KeyValuePair.Create(1L, "ddd"),
                                                                       KeyValuePair.Create(0L, "eee"),
                                                                       KeyValuePair.Create(1L, "fff"),
                                                                       KeyValuePair.Create(0L, "ggg"),
                                                                       KeyValuePair.Create(1L, "hhh"),
                                                                       KeyValuePair.Create(0L, "iii"),
                                                                       KeyValuePair.Create(1L, "jjj"));

            foreach (KeyValuePair<long, string> record in records)
            {
                mockTime.sleep(10);
                IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(record), producerConfig, mockTime.milliseconds());
            }
        }

        void shouldNotAllowToResetWhileStreamsIsRunning()
        {
            appID = testId + "-not-reset-during-runtime";
            string[] parameters = new string[] {
            "--application-id", appID,
            "--bootstrap-servers", cluster.bootstrapServers(),
            "--input-topics", NON_EXISTING_TOPIC,
            "--execute"
        };
            Properties cleanUpConfig = new Properties();
            cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
            cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

            streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

            // RUN
            streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
            streams.start();

            int exitCode = new StreamsResetter().run(parameters, cleanUpConfig);
            Assert.Equal(1, exitCode);

            streams.close();
        }

        public void shouldNotAllowToResetWhenInputTopicAbsent()
        {// throws Exception
            appID = testId + "-not-reset-without-input-topic";
            string[] parameters = new string[] {
            "--application-id", appID,
            "--bootstrap-servers", cluster.bootstrapServers(),
            "--input-topics", NON_EXISTING_TOPIC,
            "--execute"
        };
            Properties cleanUpConfig = new Properties();
            cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
            cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

            int exitCode = new StreamsResetter().run(parameters, cleanUpConfig);
            Assert.Equal(1, exitCode);
        }

        public void shouldNotAllowToResetWhenIntermediateTopicAbsent()
        {// throws Exception
            appID = testId + "-not-reset-without-intermediate-topic";
            string[] parameters = new string[] {
            "--application-id", appID,
            "--bootstrap-servers", cluster.bootstrapServers(),
            "--intermediate-topics", NON_EXISTING_TOPIC,
            "--execute"
        };
            Properties cleanUpConfig = new Properties();
            cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
            cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

            int exitCode = new StreamsResetter().run(parameters, cleanUpConfig);
            Assert.Equal(1, exitCode);
        }

        void testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic()
        {// throws Exception
            appID = testId + "-from-scratch";
            streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

            // RUN
            streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
            streams.start();
            List<KeyValuePair<long, long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

            streams.close();
            TestUtils.waitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
                "Streams Application consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

            // RESET
            streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
            streams.cleanUp();
            cleanGlobal(false, null, null);
            TestUtils.waitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                    "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

            assertInternalTopicsGotDeleted(null);

            // RE-RUN
            streams.start();
            List<KeyValuePair<long, long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
            streams.close();

            Assert.Equal(resultRerun, (result));

            TestUtils.waitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                    "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
            cleanGlobal(false, null, null);
        }

        void testReprocessingFromScratchAfterResetWithIntermediateUserTopic()
        {// throws Exception
            cluster.createTopic(INTERMEDIATE_USER_TOPIC);

            appID = testId + "-from-scratch-with-intermediate-topic";
            streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

            // RUN
            streams = new KafkaStreams(setupTopologyWithIntermediateUserTopic(OUTPUT_TOPIC_2), streamsConfig);
            streams.start();
            List<KeyValuePair<long, long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
            // receive only first values to make sure intermediate user topic is not consumed completely
            // => required to test "seekToEnd" for intermediate topics
            List<KeyValuePair<long, long>> result2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC_2, 40);

            streams.close();
            TestUtils.waitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
                "Streams Application consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

            // insert bad record to make sure intermediate user topic gets seekToEnd()
            mockTime.sleep(1);
            KeyValuePair<long, string> badMessage = new KeyValuePair<>(-1L, "badRecord-ShouldBeSkipped");
            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                INTERMEDIATE_USER_TOPIC,
                Collections.singleton(badMessage),
                    producerConfig,
                mockTime.milliseconds());

            // RESET
            streams = new KafkaStreams(setupTopologyWithIntermediateUserTopic(OUTPUT_TOPIC_2_RERUN), streamsConfig);
            streams.cleanUp();
            cleanGlobal(true, null, null);
            TestUtils.waitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                    "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

            assertInternalTopicsGotDeleted(INTERMEDIATE_USER_TOPIC);

            // RE-RUN
            streams.start();
            List<KeyValuePair<long, long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
            List<KeyValuePair<long, long>> resultRerun2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC_2_RERUN, 40);
            streams.close();

            Assert.Equal(resultRerun, (result));
            Assert.Equal(resultRerun2, (result2));

            Properties props = TestUtils.consumerConfig(cluster.bootstrapServers(), testId + "-result-consumer", LongDeserializer, StringDeserializer, commonClientConfig);
            List<KeyValuePair<long, string>> resultIntermediate = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(props, INTERMEDIATE_USER_TOPIC, 21);

            for (int i = 0; i < 10; i++)
            {
                Assert.Equal(resultIntermediate.get(i), (resultIntermediate.get(i + 11)));
            }
            Assert.Equal(resultIntermediate.get(10), (badMessage));

            TestUtils.waitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                    "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
            cleanGlobal(true, null, null);

            cluster.deleteTopicAndWait(INTERMEDIATE_USER_TOPIC);
        }

        void testReprocessingFromFileAfterResetWithoutIntermediateUserTopic()
        {// throws Exception
            appID = testId + "-from-file";
            streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

            // RUN
            streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
            streams.start();
            List<KeyValuePair<long, long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

            streams.close();
            TestUtils.waitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
                "Streams Application consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

            // RESET
            File resetFile = File.createTempFile("reset", ".csv");
            try
            {
                (BufferedWriter writer = new BufferedWriter(new FileWriter(resetFile)));
                writer.write(INPUT_TOPIC + ",0,1");
            }

        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
            streams.cleanUp();

            cleanGlobal(false, "--from-file", resetFile.getAbsolutePath());
            TestUtils.waitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                    "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

            assertInternalTopicsGotDeleted(null);

            resetFile.deleteOnExit();

            // RE-RUN
            streams.start();
            List<KeyValuePair<long, long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 5);
            streams.close();

            result.remove(0);
            Assert.Equal(resultRerun, (result));

            TestUtils.waitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                    "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
            cleanGlobal(false, null, null);
        }

        void testReprocessingFromDateTimeAfterResetWithoutIntermediateUserTopic()
        {// throws Exception
            appID = testId + "-from-datetime";
            streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

            // RUN
            streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
            streams.start();
            List<KeyValuePair<long, long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

            streams.close();
            TestUtils.waitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
                "Streams Application consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

            // RESET
            File resetFile = File.createTempFile("reset", ".csv");
            try
            {
                BufferedWriter writer = new BufferedWriter(new FileWriter(resetFile));
                writer.write(INPUT_TOPIC + ",0,1");
            }

        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
            streams.cleanUp();


            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.DATE, -1);

            cleanGlobal(false, "--to-datetime", format.format(calendar.getTime()));
            TestUtils.waitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                    "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

            assertInternalTopicsGotDeleted(null);

            resetFile.deleteOnExit();

            // RE-RUN
            streams.start();
            List<KeyValuePair<long, long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
            streams.close();

            Assert.Equal(resultRerun, (result));

            TestUtils.waitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                    "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
            cleanGlobal(false, null, null);
        }

        void testReprocessingByDurationAfterResetWithoutIntermediateUserTopic()
        {// throws Exception
            appID = testId + "-from-duration";
            streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

            // RUN
            streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
            streams.start();
            List<KeyValuePair<long, long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

            streams.close();
            TestUtils.waitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
                "Streams Application consumer group " + appID + "  did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

            // RESET
            File resetFile = File.createTempFile("reset", ".csv");
            try
            {
                (BufferedWriter writer = new BufferedWriter(new FileWriter(resetFile)));
                writer.write(INPUT_TOPIC + ",0,1");
            }

        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
            streams.cleanUp();
            cleanGlobal(false, "--by-duration", "PT1M");

            TestUtils.waitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

            assertInternalTopicsGotDeleted(null);

            resetFile.deleteOnExit();

            // RE-RUN
            streams.start();
            List<KeyValuePair<long, long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
            streams.close();

            Assert.Equal(resultRerun, (result));

            TestUtils.waitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
            cleanGlobal(false, null, null);
        }

        private Topology setupTopologyWithIntermediateUserTopic(string outputTopic2)
        {
            StreamsBuilder builder = new StreamsBuilder();

            KStream<long, string> input = builder.stream(INPUT_TOPIC);

            // use map to trigger internal re-partitioning before groupByKey
            input.map(KeyValuePair)
                .groupByKey()
                .count()
                .toStream()
                .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.Long()));

            input.through(INTERMEDIATE_USER_TOPIC)
                .groupByKey()
                .windowedBy(TimeWindows.of(ofMillis(35)).advanceBy(ofMillis(10)))
                .count()
                .toStream()
                .map((key, value) => new KeyValuePair<>(key.window().start() + key.window().end(), value))
                .to(outputTopic2, Produced.with(Serdes.Long(), Serdes.Long()));

            return builder.build();
        }

        private Topology setupTopologyWithoutIntermediateUserTopic()
        {
            StreamsBuilder builder = new StreamsBuilder();

            KStream<long, string> input = builder.stream(INPUT_TOPIC);

            // use map to trigger internal re-partitioning before groupByKey
            input.map((key, value) => new KeyValuePair<>(key, key))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.Long()));

            return builder.build();
        }

        private void cleanGlobal(bool withIntermediateTopics,
                                 string resetScenario,
                                 string resetScenarioArg)
        {// throws Exception
         // leaving --zookeeper arg here to ensure tool works if users add it
            List<string> parameterList = new ArrayList<>(
                Array.asList("--application-id", appID,
                        "--bootstrap-servers", cluster.bootstrapServers(),
                        "--input-topics", INPUT_TOPIC,
                        "--execute"));
            if (withIntermediateTopics)
            {
                parameterList.add("--intermediate-topics");
                parameterList.add(INTERMEDIATE_USER_TOPIC);
            }

            Dictionary<string, object> sslConfig = getClientSslConfig();
            if (sslConfig != null)
            {
                File configFile = TestUtils.tempFile();
                BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
                writer.write(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG + "=SSL\n");
                writer.write(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG + "=" + sslConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG) + "\n");
                writer.write(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG + "=" + ((Password)sslConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).Value + "\n");
                writer.close();

                parameterList.add("--config-file");
                parameterList.add(configFile.getAbsolutePath());
            }
            if (resetScenario != null)
            {
                parameterList.add(resetScenario);
            }
            if (resetScenarioArg != null)
            {
                parameterList.add(resetScenarioArg);
            }

            string[] parameters = parameterList.toArray(new string[0]);

            Properties cleanUpConfig = new Properties();
            cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
            cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

            int exitCode = new StreamsResetter().run(parameters, cleanUpConfig);
            Assert.Equal(0, exitCode);
        }

        private void assertInternalTopicsGotDeleted(string intermediateUserTopic)
        {// throws Exception
         // do not use list topics request, but read from the embedded cluster's zookeeper path directly to confirm
            if (intermediateUserTopic != null)
            {
                cluster.waitForRemainingTopics(30000, INPUT_TOPIC, OUTPUT_TOPIC, OUTPUT_TOPIC_2, OUTPUT_TOPIC_2_RERUN,
                        Topic.GROUP_METADATA_TOPIC_NAME, intermediateUserTopic);
            }
            else
            {
                cluster.waitForRemainingTopics(30000, INPUT_TOPIC, OUTPUT_TOPIC, OUTPUT_TOPIC_2, OUTPUT_TOPIC_2_RERUN,
                        Topic.GROUP_METADATA_TOPIC_NAME);
            }
        }
    }
}
