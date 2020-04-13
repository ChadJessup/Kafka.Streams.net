using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Topologies;
using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Kafka.Streams.Tests.Integration
{
    public abstract class AbstractResetIntegrationTest
    {
        static readonly string testId;
        static EmbeddedKafkaCluster cluster;

        private static MockTime mockTime;
        private static KafkaStreams streams;
        private static Admin adminClient = null;

        public abstract Dictionary<string, object> GetClientSslConfig();

        public static void AfterClassCleanup()
        {
            if (adminClient != null)
            {
                adminClient.Close(TimeSpan.FromSeconds(10));
                adminClient = null;
            }
        }

        private string appID = "abstract-reset-integration-test";
        private StreamsConfig commonClientConfig;
        private StreamsConfig streamsConfig;
        private StreamsConfig producerConfig;
        private StreamsConfig resultConsumerConfig;

        private void PrepareEnvironment()
        {
            if (adminClient == null)
            {
                adminClient = Admin.Create(commonClientConfig);
            }

            bool timeSet = false;
            while (!timeSet)
            {
                timeSet = SetCurrentTime();
            }
        }

        private bool SetCurrentTime()
        {
            bool currentTimeSet = false;
            try
            {
                mockTime = cluster.time;
                // we align time to seconds to get clean window boundaries and thus ensure the same result for each run
                // otherwise, input records could fall into different windows for different runs depending on the initial mock time
                long alignedTime = (System.currentTimeMillis() / 1000 + 1) * 1000;
                mockTime.SetCurrentTimeMs(alignedTime);
                currentTimeSet = true;
            }
            catch (ArgumentException e)
            {
                // don't care will retry until set
            }
            return currentTimeSet;
        }

        private void PrepareConfigs()
        {
            commonClientConfig = new StreamsConfig();
            commonClientConfig.Set(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            Dictionary<string, object> sslConfig = GetClientSslConfig();
            if (sslConfig != null)
            {
                commonClientConfig.Set(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslConfig.Get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
                commonClientConfig.Set(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ((Password)sslConfig.Get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).Value);
                commonClientConfig.Set(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            }

            producerConfig = new StreamsConfig();
            producerConfig.Set(ProducerConfig.ACKS_CONFIG, "All");
            producerConfig.Set(ProducerConfig.RETRIES_CONFIG, 0);
            producerConfig.Set(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.Long().Serializer);
            producerConfig.Set(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);
            producerConfig.SetAll(commonClientConfig);

            resultConsumerConfig = new StreamsConfig();
            resultConsumerConfig.Set(ConsumerConfig.GROUP_ID_CONFIG, testId + "-result-consumer");
            resultConsumerConfig.Set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            resultConsumerConfig.Set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer);
            resultConsumerConfig.Set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer);
            resultConsumerConfig.SetAll(commonClientConfig);

            streamsConfig = new StreamsConfig();
            streamsConfig.Set(StreamsConfig.STATE_DIR_CONFIG, testFolder.getRoot().getPath());
            streamsConfig.Set(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().GetType());
            streamsConfig.Set(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().GetType());
            streamsConfig.Set(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
            streamsConfig.Set(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
            streamsConfig.Set(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
            streamsConfig.Set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            streamsConfig.Set(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + STREAMS_CONSUMER_TIMEOUT);
            streamsConfig.SetAll(commonClientConfig);
        }

        public TemporaryFolder testFolder = new TemporaryFolder(TestUtils.GetTempDirectory());

        private const string INPUT_TOPIC = "inputTopic";
        private const string OUTPUT_TOPIC = "outputTopic";
        private const string OUTPUT_TOPIC_2 = "outputTopic2";
        private const string OUTPUT_TOPIC_2_RERUN = "outputTopic2_rerun";
        private const string INTERMEDIATE_USER_TOPIC = "userTopic";
        private const string NON_EXISTING_TOPIC = "nonExistingTopic";

        private const long STREAMS_CONSUMER_TIMEOUT = 2000L;
        private const long CLEANUP_CONSUMER_TIMEOUT = 2000L;
        private const int TIMEOUT_MULTIPLIER = 15;

        private class ConsumerGroupInactiveCondition // : TestCondition
        {
            public bool ConditionMet()
            {
                //try
                //{
                ConsumerGroupDescription groupDescription = adminClient.describeConsumerGroups(Collections.singletonList(appID)).describedGroups().Get(appID).Get();
                return groupDescription.members().IsEmpty();
                //}
                //catch (ExecutionException | InterruptedException e) {
                //    return false;
                //}
                //}
            }

            void PrepareTest()
            {// throws Exception
                PrepareConfigs();
                PrepareEnvironment();

                // busy wait until cluster (ie, ConsumerGroupCoordinator) is available
                TestUtils.WaitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                        "Test consumer group " + appID + " still active even after waiting " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

                cluster.deleteAndRecreateTopics(INPUT_TOPIC, OUTPUT_TOPIC, OUTPUT_TOPIC_2, OUTPUT_TOPIC_2_RERUN);

                Add10InputElements();
            }

            void CleanupTest()
            {// throws Exception
                if (streams != null)
                {
                    streams.Close(TimeSpan.FromSeconds(30));
                }
                IntegrationTestUtils.PurgeLocalStreamsState(streamsConfig);
            }

            private void Add10InputElements()
            {// throws java.util.concurrent.ExecutionException, InterruptedException
                List<KeyValuePair<long, string>> records = Arrays.asList(
                    KeyValuePair.Create(0L, "aaa"),
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
                    mockTime.Sleep(10);
                    IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC, Collections.singleton(record), producerConfig, mockTime.NowAsEpochMilliseconds);
                }
            }

            void ShouldNotAllowToResetWhileStreamsIsRunning()
            {
                appID = testId + "-not-reset-during-runtime";
                string[] parameters = new string[] {
            "--application-id", appID,
            "--bootstrap-servers", cluster.bootstrapServers(),
            "--input-topics", NON_EXISTING_TOPIC,
            "--execute"
        };
                StreamsConfig cleanUpConfig = new StreamsConfig();
                cleanUpConfig.Put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
                cleanUpConfig.Put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

                streamsConfig.Put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

                // RUN
                streams = new KafkaStreams(SetupTopologyWithoutIntermediateUserTopic(), streamsConfig);
                streams.start();

                int exitCode = new StreamsResetter().run(parameters, cleanUpConfig);
                Assert.Equal(1, exitCode);

                streams.Close();
            }

            public void ShouldNotAllowToResetWhenInputTopicAbsent()
            {// throws Exception
                appID = testId + "-not-reset-without-input-topic";
                string[] parameters = new string[] {
            "--application-id", appID,
            "--bootstrap-servers", cluster.bootstrapServers(),
            "--input-topics", NON_EXISTING_TOPIC,
            "--execute"
        };
                StreamsConfig cleanUpConfig = new StreamsConfig();
                cleanUpConfig.Put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
                cleanUpConfig.Put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

                int exitCode = new StreamsResetter().run(parameters, cleanUpConfig);
                Assert.Equal(1, exitCode);
            }

            public void ShouldNotAllowToResetWhenIntermediateTopicAbsent()
            {// throws Exception
                appID = testId + "-not-reset-without-intermediate-topic";
                string[] parameters = new string[] {
            "--application-id", appID,
            "--bootstrap-servers", cluster.bootstrapServers(),
            "--intermediate-topics", NON_EXISTING_TOPIC,
            "--execute"
        };
                StreamsConfig cleanUpConfig = new StreamsConfig();
                cleanUpConfig.Put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
                cleanUpConfig.Put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

                int exitCode = new StreamsResetter().run(parameters, cleanUpConfig);
                Assert.Equal(1, exitCode);
            }

            void TestReprocessingFromScratchAfterResetWithoutIntermediateUserTopic()
            {// throws Exception
                appID = testId + "-from-scratch";
                streamsConfig.Put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

                // RUN
                streams = new KafkaStreams(SetupTopologyWithoutIntermediateUserTopic(), streamsConfig);
                streams.start();
                List<KeyValuePair<long, long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

                streams.Close();
                TestUtils.WaitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
                    "Streams Application consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

                // RESET
                streams = new KafkaStreams(SetupTopologyWithoutIntermediateUserTopic(), streamsConfig);
                streams.cleanUp();
                CleanGlobal(false, null, null);
                TestUtils.WaitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                        "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

                AssertInternalTopicsGotDeleted(null);

                // RE-RUN
                streams.start();
                List<KeyValuePair<long, long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
                streams.Close();

                Assert.Equal(resultRerun, result);

                TestUtils.WaitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                        "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
                CleanGlobal(false, null, null);
            }

            void TestReprocessingFromScratchAfterResetWithIntermediateUserTopic()
            {// throws Exception
                cluster.createTopic(INTERMEDIATE_USER_TOPIC);

                appID = testId + "-from-scratch-with-intermediate-topic";
                streamsConfig.Put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

                // RUN
                streams = new KafkaStreams(SetupTopologyWithIntermediateUserTopic(OUTPUT_TOPIC_2), streamsConfig);
                streams.start();
                List<KeyValuePair<long, long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
                // receive only first values to make sure intermediate user topic is not consumed completely
                // => required to test "seekToEnd" for intermediate topics
                List<KeyValuePair<long, long>> result2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC_2, 40);

                streams.Close();
                TestUtils.WaitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
                    "Streams Application consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

                // insert bad record to make sure intermediate user topic gets seekToEnd()
                mockTime.Sleep(1);
                KeyValuePair<long, string> badMessage = KeyValuePair.Create(-1L, "badRecord-ShouldBeSkipped");
                IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
                    INTERMEDIATE_USER_TOPIC,
                    Collections.singleton(badMessage),
                        producerConfig,
                    mockTime.NowAsEpochMilliseconds);

                // RESET
                streams = new KafkaStreams(SetupTopologyWithIntermediateUserTopic(OUTPUT_TOPIC_2_RERUN), streamsConfig);
                streams.cleanUp();
                CleanGlobal(true, null, null);
                TestUtils.WaitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                        "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

                AssertInternalTopicsGotDeleted(INTERMEDIATE_USER_TOPIC);

                // RE-RUN
                streams.start();
                List<KeyValuePair<long, long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
                List<KeyValuePair<long, long>> resultRerun2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC_2_RERUN, 40);
                streams.Close();

                Assert.Equal(resultRerun, result);
                Assert.Equal(resultRerun2, result2);

                StreamsConfig props = TestUtils.consumerConfig(cluster.bootstrapServers(), testId + "-result-consumer", LongDeserializer, Serdes.String().Deserializer, commonClientConfig);
                List<KeyValuePair<long, string>> resultIntermediate = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(props, INTERMEDIATE_USER_TOPIC, 21);

                for (int i = 0; i < 10; i++)
                {
                    Assert.Equal(resultIntermediate.Get(i), resultIntermediate.Get(i + 11));
                }
                Assert.Equal(resultIntermediate.Get(10), badMessage);

                TestUtils.WaitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                        "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
                CleanGlobal(true, null, null);

                cluster.deleteTopicAndWait(INTERMEDIATE_USER_TOPIC);
            }

            void TestReprocessingFromFileAfterResetWithoutIntermediateUserTopic()
            {// throws Exception
                appID = testId + "-from-file";
                streamsConfig.Put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

                // RUN
                streams = new KafkaStreams(SetupTopologyWithoutIntermediateUserTopic(), streamsConfig);
                streams.start();
                List<KeyValuePair<long, long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

                streams.Close();
                TestUtils.WaitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
                    "Streams Application consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

                // RESET
                FileInfo resetFile = File.createTempFile("reset", ".csv");
                BufferedWriter writer = new BufferedWriter(new FileWriter(resetFile));
                writer.write(INPUT_TOPIC + ",0,1");

                streams = new KafkaStreams(SetupTopologyWithoutIntermediateUserTopic(), streamsConfig);
                streams.cleanUp();

                cleanGlobal(false, "--from-file", resetFile.FullName);
                TestUtils.WaitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                        "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

                AssertInternalTopicsGotDeleted(null);

                resetFile.deleteOnExit();

                // RE-RUN
                streams.start();
                List<KeyValuePair<long, long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 5);
                streams.Close();

                result.remove(0);
                Assert.Equal(resultRerun, result);

                TestUtils.WaitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                        "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
                CleanGlobal(false, null, null);
            }

            void TestReprocessingFromDateTimeAfterResetWithoutIntermediateUserTopic()
            {// throws Exception
                appID = testId + "-from-datetime";
                streamsConfig.Put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

                // RUN
                streams = new KafkaStreams(SetupTopologyWithoutIntermediateUserTopic(), streamsConfig);
                streams.start();
                List<KeyValuePair<long, long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

                streams.Close();
                TestUtils.WaitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
                    "Streams Application consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

                // RESET
                var resetFile = File.createTempFile("reset", ".csv");
                BufferedWriter writer = new BufferedWriter(new FileWriter(resetFile));
                writer.write(INPUT_TOPIC + ",0,1");

                streams = new KafkaStreams(SetupTopologyWithoutIntermediateUserTopic(), streamsConfig);
                streams.cleanUp();


                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                Calendar calendar = Calendar.getInstance();
                calendar.Add(Calendar.DATE, -1);

                cleanGlobal(false, "--to-datetime", format.format(calendar.getTime()));
                TestUtils.WaitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                        "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

                AssertInternalTopicsGotDeleted(null);

                resetFile.deleteOnExit();

                // RE-RUN
                streams.start();
                List<KeyValuePair<long, long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
                streams.Close();

                Assert.Equal(resultRerun, result);

                TestUtils.WaitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                        "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
                CleanGlobal(false, null, null);
            }

            void TestReprocessingByDurationAfterResetWithoutIntermediateUserTopic()
            {// throws Exception
                appID = testId + "-from-duration";
                streamsConfig.Put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

                // RUN
                streams = new KafkaStreams(SetupTopologyWithoutIntermediateUserTopic(), streamsConfig);
                streams.start();
                List<KeyValuePair<long, long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

                streams.Close();
                TestUtils.WaitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT,
                    "Streams Application consumer group " + appID + "  did not time out after " + (TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT) + " ms.");

                // RESET
                var resetFile = File.createTempFile("reset", ".csv");
                BufferedWriter writer = new BufferedWriter(new FileWriter(resetFile));
                writer.write(INPUT_TOPIC + ",0,1");

                streams = new KafkaStreams(SetupTopologyWithoutIntermediateUserTopic(), streamsConfig);
                streams.cleanUp();
                CleanGlobal(false, "--by-duration", "PT1M");

                TestUtils.WaitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                    "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");

                AssertInternalTopicsGotDeleted(null);

                resetFile.deleteOnExit();

                // RE-RUN
                streams.start();
                List<KeyValuePair<long, long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
                streams.Close();

                Assert.Equal(resultRerun, result);

                TestUtils.WaitForCondition(new ConsumerGroupInactiveCondition(), TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT,
                    "Reset Tool consumer group " + appID + " did not time out after " + (TIMEOUT_MULTIPLIER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
                CleanGlobal(false, null, null);
            }

            private Topology SetupTopologyWithIntermediateUserTopic(string outputTopic2)
            {
                StreamsBuilder builder = new StreamsBuilder();

                IKStream<K, V>(INPUT_TOPIC);

                // use map to trigger internal re-partitioning before groupByKey
                input.map(KeyValuePair)
                    .GroupByKey()
                    .Count()
                    .ToStream()
                    .To(OUTPUT_TOPIC, Produced.With(Serdes.Long(), Serdes.Long()));

                input.Through(INTERMEDIATE_USER_TOPIC)
                    .GroupByKey()
                    .WindowedBy(TimeWindow.Of(TimeSpan.FromMilliseconds(35)).advanceBy(TimeSpan.FromMilliseconds(10)))
                    .Count()
                    .ToStream()
                    .map((key, value) => KeyValuePair.Create(key.window().start() + key.window().end(), value))
                    .To(outputTopic2, Produced.With(Serdes.Long(), Serdes.Long()));

                return builder.Build();
            }

            private Topology SetupTopologyWithoutIntermediateUserTopic()
            {
                StreamsBuilder builder = new StreamsBuilder();

                IKStream<K, V>(INPUT_TOPIC);

                // use map to trigger internal re-partitioning before groupByKey
                input.map((key, value) => KeyValuePair.Create(key, key))
                    .To(OUTPUT_TOPIC, Produced.With(Serdes.Long(), Serdes.Long()));

                return builder.Build();
            }

            private void CleanGlobal(bool withIntermediateTopics,
                                     string resetScenario,
                                     string resetScenarioArg)
            {// throws Exception
             // leaving --zookeeper arg here to ensure tool works if users add it
                List<string> parameterList = new List<>(
                    Arrays.asList("--application-id", appID,
                            "--bootstrap-servers", cluster.bootstrapServers(),
                            "--input-topics", INPUT_TOPIC,
                            "--execute"));
                if (withIntermediateTopics)
                {
                    parameterList.Add("--intermediate-topics");
                    parameterList.Add(INTERMEDIATE_USER_TOPIC);
                }

                Dictionary<string, object> sslConfig = GetClientSslConfig();
                if (sslConfig != null)
                {
                    FileInfo configFile = TestUtils.tempFile();
                    BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
                    writer.write(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG + "=SSL\n");
                    writer.write(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG + "=" + sslConfig.Get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG) + "\n");
                    writer.write(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG + "=" + ((Password)sslConfig.Get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).Value + "\n");
                    writer.Close();

                    parameterList.Add("--config-file");
                    parameterList.Add(configFile.FullName);
                }
                if (resetScenario != null)
                {
                    parameterList.Add(resetScenario);
                }
                if (resetScenarioArg != null)
                {
                    parameterList.Add(resetScenarioArg);
                }

                string[] parameters = parameterList.toArray(System.Array.Empty<string>());

                StreamsConfig cleanUpConfig = new StreamsConfig();
                cleanUpConfig.Put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
                cleanUpConfig.Put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + CLEANUP_CONSUMER_TIMEOUT);

                int exitCode = new StreamsResetter().run(parameters, cleanUpConfig);
                Assert.Equal(0, exitCode);
            }

            private void AssertInternalTopicsGotDeleted(string intermediateUserTopic)
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
}