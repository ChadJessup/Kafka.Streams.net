using Kafka.Streams.Configs;
using Kafka.Streams.KStream;
using Kafka.Streams.State.Windowed;
using Kafka.Streams.Temporary;
using Kafka.Streams.Tests.Mocks;
using Kafka.Streams.Threads.KafkaStreams;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Integration
{
    /**
     * Tests related to internal topics in streams
     */

    public class InternalTopicIntegrationTest
    {

        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

        private const string APP_ID = "internal-topics-integration-test";
        private const string DEFAULT_INPUT_TOPIC = "inputTopic";

        private MockTime mockTime = CLUSTER.time;

        private StreamsConfig streamsProp;


        public static void StartKafkaCluster()
        {// throws InterruptedException
            CLUSTER.createTopics(DEFAULT_INPUT_TOPIC);
        }


        public void Before()
        {
            streamsProp = new StreamsConfig();
            streamsProp.Put(StreamsConfig.BootstrapServersConfig, CLUSTER.bootstrapServers());
            streamsProp.Put(StreamsConfig.DefaultKeySerdeClassConfig, Serdes.String().GetType().FullName);
            streamsProp.Put(StreamsConfig.DefaultValueSerdeClassConfig, Serdes.String().GetType().FullName);
            streamsProp.Put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath());
            streamsProp.Put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
            streamsProp.Put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
            streamsProp.Put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }


        public void After()
        { //throws IOException
          // Remove any state from previous test runs
            IntegrationTestUtils.PurgeLocalStreamsState(streamsProp);
        }

        private void ProduceData(List<string> inputValues)
        {// throws Exception
            StreamsConfig producerProp = new StreamsConfig();
            producerProp.Put(ProducerConfig.BootstrapServersConfig, CLUSTER.bootstrapServers());
            producerProp.Put(ProducerConfig.ACKS_CONFIG, "All");
            producerProp.Put(ProducerConfig.RETRIES_CONFIG, 0);
            producerProp.Put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);
            producerProp.Put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);

            IntegrationTestUtils.produceValuesSynchronously(DEFAULT_INPUT_TOPIC, inputValues, producerProp, mockTime);
        }

        private StreamsConfig GetTopicProperties(string changelog)
        {
            //try
            //{
            Admin adminClient = createAdminClient();
            ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, changelog);
            //try
            //{
            Config config = adminClient.describeConfigs(Collections.singletonList(configResource)).values().Get(configResource).Get();
            StreamsConfig properties = new StreamsConfig();
            foreach (ConfigEntry configEntry in config.entries())
            {
                if (configEntry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
                {
                    properties.Put(configEntry.Name(), configEntry.Value);
                }
            }
            return properties;
            // }
            // catch (InterruptedException | ExecutionException e) {
            //     throw new RuntimeException(e);
            // }
            // }
        }

        private Admin createAdminClient()
        {
            StreamsConfig adminClientConfig = new StreamsConfig();
            adminClientConfig.Put(AdminClientConfig.BootstrapServersConfig, CLUSTER.bootstrapServers());
            return Admin.Create(adminClientConfig);
        }

        [Fact]
        public void ShouldCompactTopicsForKeyValueStoreChangelogs()
        {// throws Exception
            string appID = APP_ID + "-compact";
            streamsProp.Put(StreamsConfig.ApplicationIdConfig, appID);

            //
            // Step 1: Configure and start a simple word count topology
            //
            StreamsBuilder builder = new StreamsBuilder();
            IKStream<K, V> textLines = builder.Stream(DEFAULT_INPUT_TOPIC);

            textLines.flatMapValues(value => Arrays.asList(value.toLowerCase(Locale.getDefault()).Split("\\W+")))
                .GroupBy(MockMapper.selectValueMapper())
                .Count(Materialized.As("Counts"));

            KafkaStreamsThread streams = new KafkaStreamsThread(builder.Build(), streamsProp);
            streams.Start();

            //
            // Step 2: Produce some input data to the input topic.
            //
            produceData(Arrays.asList("hello", "world", "world", "hello world"));

            //
            // Step 3: Verify the state changelog topics are compact
            //
            waitForCompletion(streams, 2, 30000);
            streams.Close();

            StreamsConfig changelogProps = getTopicProperties(ProcessorStateManager.storeChangelogTopic(appID, "Counts"));
            Assert.Equal(LogConfig.Compact(), changelogProps.getProperty(LogConfig.CleanupPolicyProp()));

            StreamsConfig repartitionProps = GetTopicProperties(appID + "-Counts-repartition");
            Assert.Equal(LogConfig.Delete(), repartitionProps.getProperty(LogConfig.CleanupPolicyProp()));
            Assert.Equal(3, repartitionProps.Count);
        }

        [Fact]
        public void ShouldCompactAndDeleteTopicsForWindowStoreChangelogs()
        {// throws Exception
            string appID = APP_ID + "-compact-delete";
            streamsProp.Put(StreamsConfig.ApplicationIdConfig, appID);

            //
            // Step 1: Configure and start a simple word count topology
            //
            StreamsBuilder builder = new StreamsBuilder();
            IKStream<K, V> textLines = builder.Stream(DEFAULT_INPUT_TOPIC);

            int durationMs = 2000;

            textLines.flatMapValues(value => Arrays.asList(value.toLowerCase(Locale.getDefault()).Split("\\W+")))
                .GroupBy(MockMapper.GetSelectValueMapper())
                .WindowedBy(TimeWindows.Of(TimeSpan.FromSeconds(1L)).Grace(TimeSpan.FromMilliseconds(0L)))
                .Count(Materialized.As<string, long, IWindowStore<Bytes, byte[]>>("CountWindows").withRetention(TimeSpan.FromSeconds(2L)));

            KafkaStreamsThread streams = new KafkaStreamsThread(builder.Build(), streamsProp);
            streams.Start();

            //
            // Step 2: Produce some input data to the input topic.
            //
            produceData(Arrays.asList("hello", "world", "world", "hello world"));

            //
            // Step 3: Verify the state changelog topics are compact
            //
            waitForCompletion(streams, 2, 30000);
            streams.Close();
            StreamsConfig properties = getTopicProperties(ProcessorStateManager.storeChangelogTopic(appID, "CountWindows"));
            List<string> policies = Arrays.asList(properties.getProperty(LogConfig.CleanupPolicyProp()).Split(","));
            Assert.Equal(2, policies.Count);
            Assert.True(policies.Contains(LogConfig.Compact()));
            Assert.True(policies.Contains(LogConfig.Delete()));
            // retention should be 1 day + the window duration
            long retention = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS) + durationMs;
            Assert.Equal(retention, long.parseLong(properties.getProperty(LogConfig.RetentionMsProp())));

            StreamsConfig repartitionProps = GetTopicProperties(appID + "-CountWindows-repartition");
            Assert.Equal(LogConfig.Delete(), repartitionProps.getProperty(LogConfig.CleanupPolicyProp()));
            Assert.Equal(3, repartitionProps.Count);
        }
    }
}
