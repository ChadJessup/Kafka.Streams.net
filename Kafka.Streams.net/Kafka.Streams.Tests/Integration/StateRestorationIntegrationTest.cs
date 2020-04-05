//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.State;
//using System.Collections.Generic;

//namespace Kafka.Streams.Tests.Integration
//{
//    public class StateRestorationIntegrationTest
//    {
//        private StreamsBuilder builder = new StreamsBuilder();

//        private const string APPLICATION_ID = "restoration-test-app";
//        private const string STATE_STORE_NAME = "stateStore";
//        private const string INPUT_TOPIC = "input";
//        private const string OUTPUT_TOPIC = "output";

//        private StreamsConfig streamsConfiguration;


//        //        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);
//        private MockTime mockTime = CLUSTER.time;

//        public void SetUp()
//        {// throws Exception
//            StreamsConfig props = new StreamsConfig();

//            streamsConfiguration = StreamsTestUtils.getStreamsConfig(
//                    APPLICATION_ID,
//                    CLUSTER.bootstrapServers(),
//                    Serdes.Int().GetType().FullName,
//                    Serdes.ByteArray().GetType().FullName,
//                    props);

//            CLUSTER.createTopics(INPUT_TOPIC);
//            CLUSTER.createTopics(OUTPUT_TOPIC);

//            IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
//        }

//        [Xunit.Fact]
//        public void ShouldRestoreNullRecord()
//        {// throws InterruptedException, ExecutionException
//            builder.Table<int, byte[]>(INPUT_TOPIC, Materialized.As<int, byte[]>(
//                    Stores.PersistentTimestampedKeyValueStore(STATE_STORE_NAME))
//                    .WithKeySerde(Serdes.Int())
//                    .WithValueSerde(Serdes.ByteArray())
//                    .WithCachingDisabled()).ToStream().To(OUTPUT_TOPIC);

//            StreamsConfig producerConfig = TestUtils.producerConfig(
//                    CLUSTER.bootstrapServers(), IntegerSerializer, BytesSerializer);

//            var initialKeyValues = new List<KeyValuePair<int, byte[]>>
//            {
//                    KeyValuePair.Create(3, new byte[] { 3 }),
//                    KeyValuePair.Create<int, byte[]>(3, null),
//                    KeyValuePair.Create(1, new byte[] { 1 })
//            };

//            IntegrationTestUtils.produceKeyValuesSynchronously(
//                    INPUT_TOPIC, initialKeyValues, producerConfig, mockTime);

//            KafkaStreams streams = new KafkaStreams(builder.Build(streamsConfiguration), streamsConfiguration);
//            streams.start();

//            StreamsConfig consumerConfig = TestUtils.consumerConfig(
//                    CLUSTER.bootstrapServers(), Serdes.Int().Deserializer, Serdes.ByteArray().Deserializer);

//            IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(
//                    consumerConfig, OUTPUT_TOPIC, initialKeyValues);

//            // wipe out state store to trigger restore process on restart
//            streams.close();
//            streams.cleanUp();

//            // Restart the stream instance. There should not be exception handling the null value within changelog topic.
//            List<KeyValuePair<int, Bytes>> newKeyValues =
//                    Collections.singletonList(KeyValuePair.Create(2, new Bytes(new byte[3])));
//            IntegrationTestUtils.produceKeyValuesSynchronously(
//                    INPUT_TOPIC, newKeyValues, producerConfig, mockTime);
//            streams = new KafkaStreams(builder.Build(streamsConfiguration), streamsConfiguration);
//            streams.start();
//            IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(
//                    consumerConfig, OUTPUT_TOPIC, newKeyValues);
//            streams.close();
//        }
//    }
//}
