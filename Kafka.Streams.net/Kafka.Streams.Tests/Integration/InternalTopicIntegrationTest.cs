//using Confluent.Kafka;
//using Confluent.Kafka.Admin;
//using Kafka.Common;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State.Windowed;
//using Kafka.Streams.Temporary;
//using Kafka.Streams.Tests.Helpers;
//using Kafka.Streams.Tests.Mocks;
//using Kafka.Streams.Threads.KafkaStreams;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Integration
//{
//    /**
//     * Tests related to internal topics in streams
//     */
//    public class InternalTopicIntegrationTest
//    {
//        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

//        private const string APP_ID = "internal-topics-integration-test";
//        private const string DEFAULT_INPUT_TOPIC = "inputTopic";

//        private MockTime mockTime = CLUSTER.time;

//        private StreamsConfig streamsProp;

//        public static void StartKafkaCluster()
//        {// throws InterruptedException
//            CLUSTER.CreateTopics(DEFAULT_INPUT_TOPIC);
//        }

//        public InternalTopicIntegrationTest()
//        {
//            streamsProp = new StreamsConfig
//            {
//                BootstrapServers = CLUSTER.bootstrapServers(),
//                DefaultKeySerdeType = Serdes.String().GetType(),
//                DefaultValueSerdeType = Serdes.String().GetType(),
//                StateStoreDirectory = TestUtils.GetTempDirectory(),
//                CommitIntervalMs = 100,
//                CacheMaxBytesBuffering = 0,
//                AutoOffsetReset = AutoOffsetReset.Earliest
//            };
//        }

//        public void After()
//        { //throws IOException
//          // Remove any state from previous test runs
//            IntegrationTestUtils.PurgeLocalStreamsState(streamsProp);
//        }

//        private void ProduceData(List<string> inputValues)
//        {// throws Exception
//            StreamsConfig producerProp = new StreamsConfig
//            {
//                BootstrapServers = CLUSTER.bootstrapServers(),
//                Retries = 0,
//                DefaultKeySerdeType = Serdes.String().Serializer.GetType(),
//                DefaultValueSerdeType = Serdes.String().Serializer.GetType(),
//            };

//            //producerProp.Put(ProducerConfig.ACKS_CONFIG, "All");

//            IntegrationTestUtils.ProduceValuesSynchronously(DEFAULT_INPUT_TOPIC, inputValues, producerProp, mockTime);
//        }

//        private StreamsConfig GetTopicProperties(string changelog)
//        {
//            var adminClient = CreateAdminClient();
//            ConfigResource configResource = new ConfigResource();// ConfigResource.Type.TOPIC, changelog);

//            Config config = adminClient.DescribeConfigsAsync(Collections.singletonList(configResource)).Result.Get(configResource).Get();
//            StreamsConfig properties = new StreamsConfig();
//            foreach (var configEntry in config)
//            {
//                if (configEntry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
//                {
//                    properties.Put(configEntry.Key, configEntry.Value);
//                }
//            }
//            return properties;
//            // }
//            // catch (InterruptedException | ExecutionException e) {
//            //     throw new RuntimeException(e);
//            // }
//            // }
//        }

//        private IAdminClient CreateAdminClient()
//        {
//            var adminClientConfig = new StreamsConfig
//            {
//                BootstrapServers = CLUSTER.bootstrapServers()
//            };

//            return new AdminClientBuilder(adminClientConfig).Build();
//        }

//        [Fact]
//        public void ShouldCompactTopicsForKeyValueStoreChangelogs()
//        {// throws Exception
//            string appID = APP_ID + "-compact";
//            streamsProp.ApplicationId = appID;

//            //
//            // Step 1: Configure and start a simple word count topology
//            //
//            StreamsBuilder builder = new StreamsBuilder();
//            IKStream<string, string> textLines = builder.Stream<string, string>(DEFAULT_INPUT_TOPIC);

//            textLines.FlatMapValues(value => Arrays.asList(value.ToLower().Split("\\W+")))
//                .GroupBy(MockMapper.GetSelectValueMapper<string, string>())
//                .Count(Materialized.As("Counts"));

//            IKafkaStreamsThread streams = new KafkaStreamsThread(builder.Build(), streamsProp);
//            streams.Start();

//            //
//            // Step 2: Produce some input data to the input topic.
//            //
//            ProduceData(Arrays.asList("hello", "world", "world", "hello world"));

//            //
//            // Step 3: Verify the state changelog topics are compact
//            //
//            WaitForCompletion(streams, 2, 30000);
//            streams.Close();

//            StreamsConfig changelogProps = GetTopicProperties(ProcessorStateManager.StoreChangelogTopic(appID, "Counts"));
//            Assert.Equal(LogConfig.Compact(), changelogProps.Get(LogConfig.CleanupPolicyProp()));

//            StreamsConfig repartitionProps = GetTopicProperties(appID + "-Counts-repartition");
//            Assert.Equal(LogConfig.Delete(), repartitionProps.Get(LogConfig.CleanupPolicyProp()));
//            Assert.Equal(3, repartitionProps.Count());
//        }

//        [Fact]
//        public void ShouldCompactAndDeleteTopicsForWindowStoreChangelogs()
//        {// throws Exception
//            string appID = APP_ID + "-compact-delete";
//            streamsProp.Put(StreamsConfig.ApplicationIdConfig, appID);

//            //
//            // Step 1: Configure and start a simple word count topology
//            //
//            StreamsBuilder builder = new StreamsBuilder();
//            IKStream<string, string> textLines = builder.Stream<string, string>(DEFAULT_INPUT_TOPIC);

//            int durationMs = 2000;

//            textLines.FlatMapValues(value => Arrays.asList(value.ToLower().Split("\\W+")))
//                .GroupBy(MockMapper.GetSelectValueMapper<string, string>())
//                .WindowedBy(TimeWindows.Of(TimeSpan.FromSeconds(1L)).Grace(TimeSpan.FromMilliseconds(0L)))
//                .Count(Materialized.As<string, long, IWindowStore<Bytes, byte[]>>("CountWindows").WithRetention(TimeSpan.FromSeconds(2L)));

//            IKafkaStreamsThread streams = new KafkaStreamsThread(builder.Build(), streamsProp);
//            streams.Start();

//            //
//            // Step 2: Produce some input data to the input topic.
//            //
//            ProduceData(Arrays.asList("hello", "world", "world", "hello world"));

//            //
//            // Step 3: Verify the state changelog topics are compact
//            //
//            WaitForCompletion(streams, 2, 30000);
//            streams.Close();
//            StreamsConfig properties = GetTopicProperties(ProcessorStateManager.StoreChangelogTopic(appID, "CountWindows"));
//            List<string> policies = Arrays.asList(properties.Get(LogConfig.CleanupPolicyProp()).Split(","));
//            Assert.Equal(2, policies.Count);
//            Assert.True(policies.Contains(LogConfig.Compact()));
//            Assert.True(policies.Contains(LogConfig.Delete()));
//            // retention should be 1 day + the window duration
//            long retention = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS) + durationMs;
//            Assert.Equal(retention, long.Parse(properties.Get(LogConfig.RetentionMsProp())));

//            StreamsConfig repartitionProps = GetTopicProperties(appID + "-CountWindows-repartition");
//            Assert.Equal(LogConfig.Delete(), repartitionProps.Get(LogConfig.CleanupPolicyProp()));
//            Assert.Equal(3, repartitionProps.Count());
//        }
//    }
//}
