using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.Tests.Helpers;

using System;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Integration
{
    public class PurgeRepartitionTopicIntegrationTest
    {

        private const int NUM_BROKERS = 1;

        private const string INPUT_TOPIC = "input-stream";
        private const string APPLICATION_ID = "restore-test";
        private const string REPARTITION_TOPIC = APPLICATION_ID + "-KSTREAM-AGGREGATE-STATE-STORE-0000000002-repartition";

        private static Admin adminClient;
        private static KafkaStreams kafkaStreams;
        private const int PURGE_INTERVAL_MS = 10;
        private const int PURGE_SEGMENT_BYTES = 2000;


        //        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS, new StreamsConfig() {
        //        {
        //            Put("log.retention.check.interval.ms", PURGE_INTERVAL_MS);
        //        Put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, 0);
        //    }
        //    });

        private Time time = CLUSTER.time;

        private class RepartitionTopicCreatedWithExpectedConfigs : TestCondition
        {

            public bool ConditionMet()
            {
                try
                {
                    HashSet<string> topics = adminClient.listTopics().names().Get();

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
                        .Get(resource)
                        .Get();
                    return config.Get(TopicConfig.CLEANUP_POLICY_CONFIG).Value.Equals(TopicConfig.CLEANUP_POLICY_DELETE)
                            && config.Get(TopicConfig.SEGMENT_MS_CONFIG).Value.Equals(PURGE_INTERVAL_MS.ToString())
                            && config.Get(TopicConfig.SEGMENT_BYTES_CONFIG).Value.Equals(PURGE_SEGMENT_BYTES.ToString());
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

            public RepartitionTopicVerified(TopicSizeVerifier verifier)
            {
                this.verifier = verifier;
            }

            public bool ConditionMet()
            {
                time.Sleep(PURGE_INTERVAL_MS);

                try
                {
                    Collection<DescribeLogDirsResponse.LogDirInfo> logDirInfo =
                        adminClient.describeLogDirs(Collections.singleton(0)).values().Get(0).Get().values();

                    foreach (DescribeLogDirsResponse.LogDirInfo partitionInfo in logDirInfo)
                    {
                        DescribeLogDirsResponse.ReplicaInfo replicaInfo =
                            partitionInfo.replicaInfos.Get(new TopicPartition(REPARTITION_TOPIC, 0));
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
            // Create admin client for verification
            StreamsConfig adminConfig = new StreamsConfig();
            adminConfig.Set(StreamsConfig.BootstrapServers, CLUSTER.bootstrapServers());
            adminClient = Admin.Create(adminConfig);

            StreamsConfig streamsConfiguration = new StreamsConfig();
            streamsConfiguration.Set(StreamsConfig.ApplicationId, APPLICATION_ID);
            streamsConfiguration.Set(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, PURGE_INTERVAL_MS);
            streamsConfiguration.Set(StreamsConfig.BootstrapServers, CLUSTER.bootstrapServers());
            streamsConfiguration.Set(StreamsConfig.DefaultKeySerdeClass, Serdes.Int().GetType().FullName);
            streamsConfiguration.Set(StreamsConfig.DefaultValueSerdeClass, Serdes.Int().GetType().FullName);
            streamsConfiguration.Set(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory(APPLICATION_ID).getPath());
            streamsConfiguration.Set(StreamsConfig.TopicPrefix(TopicConfig.SEGMENT_MS_CONFIG), PURGE_INTERVAL_MS);
            streamsConfiguration.Set(StreamsConfig.TopicPrefix(TopicConfig.SEGMENT_BYTES_CONFIG), PURGE_SEGMENT_BYTES);
            streamsConfiguration.Set(StreamsConfig.ProducerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), PURGE_SEGMENT_BYTES / 2);    // we cannot allow batch size larger than segment size

            StreamsBuilder builder = new StreamsBuilder();
            builder.Stream(INPUT_TOPIC)
                   .GroupBy(MockMapper.selectKeyKeyValueMapper())
                   .Count();

            kafkaStreams = new KafkaStreams(builder.Build(), streamsConfiguration, time);
        }

        public void Shutdown()
        {
            if (kafkaStreams != null)
            {
                kafkaStreams.Close(TimeSpan.FromSeconds(30));
            }
        }

        [Fact]
        public void ShouldRestoreState()
        {// throws Exception
         // produce some data to input topic
            List<KeyValuePair<int, int>> messages = new List<KeyValuePair<int, int>>();
            for (int i = 0; i < 1000; i++)
            {
                messages.Add(KeyValuePair.Create(i, i));
            }

            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC,
                    messages,
                    TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                            IntegerSerializer,
                            IntegerSerializer),
                    time.NowAsEpochMilliseconds);

            kafkaStreams.start();

            TestUtils.WaitForCondition(new RepartitionTopicCreatedWithExpectedConfigs(), 60000,
                    "Repartition topic " + REPARTITION_TOPIC + " not created with the expected configs after 60000 ms.");

            TestUtils.WaitForCondition(
                new RepartitionTopicVerified(currentSize => currentSize > 0),
                60000,
                "Repartition topic " + REPARTITION_TOPIC + " not received data after 60000 ms."
            );

            // we need long enough timeout to by-pass the log manager's InitialTaskDelayMs, which is hard-coded on server side
            TestUtils.WaitForCondition(
                new RepartitionTopicVerified(currentSize => currentSize <= PURGE_SEGMENT_BYTES),
                60000,
                "Repartition topic " + REPARTITION_TOPIC + " not purged data after 60000 ms."
            );
        }
    }
}
