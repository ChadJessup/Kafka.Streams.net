namespace Kafka.Streams.Tests.Integration
{
    /*






    *

    *





    */































    public class StateRestorationIntegrationTest
    {
        private StreamsBuilder builder = new StreamsBuilder();

        private static readonly string APPLICATION_ID = "restoration-test-app";
        private static readonly string STATE_STORE_NAME = "stateStore";
        private static readonly string INPUT_TOPIC = "input";
        private static readonly string OUTPUT_TOPIC = "output";

        private Properties streamsConfiguration;


        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);
        private MockTime mockTime = CLUSTER.time;


        public void SetUp()
        {// throws Exception
            Properties props = new Properties();

            streamsConfiguration = StreamsTestUtils.getStreamsConfig(
                    APPLICATION_ID,
                    CLUSTER.bootstrapServers(),
                    Serdes.Int().getClass().getName(),
                    Serdes.ByteArray().getClass().getName(),
                    props);

            CLUSTER.createTopics(INPUT_TOPIC);
            CLUSTER.createTopics(OUTPUT_TOPIC);

            IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
        }

        [Xunit.Fact]
        public void ShouldRestoreNullRecord()
        {// throws InterruptedException, ExecutionException
            builder.table(INPUT_TOPIC, Materialized<int, Bytes>.As(
                    Stores.persistentTimestampedKeyValueStore(STATE_STORE_NAME))
                    .withKeySerde(Serdes.Int())
                    .withValueSerde(Serdes.Bytes())
                    .withCachingDisabled()).toStream().to(OUTPUT_TOPIC);

            Properties producerConfig = TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(), IntegerSerializer, BytesSerializer);

            List<KeyValuePair<int, Bytes>> initialKeyValues = Array.asList(
                    KeyValuePair.Create(3, new Bytes(new byte[] { 3 })),
                    KeyValuePair.Create(3, null),
                    KeyValuePair.Create(1, new Bytes(new byte[] { 1 })));

            IntegrationTestUtils.produceKeyValuesSynchronously(
                    INPUT_TOPIC, initialKeyValues, producerConfig, mockTime);

            KafkaStreams streams = new KafkaStreams(builder.build(streamsConfiguration), streamsConfiguration);
            streams.start();

            Properties consumerConfig = TestUtils.consumerConfig(
                    CLUSTER.bootstrapServers(), IntegerDeserializer, BytesDeserializer);

            IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(
                    consumerConfig, OUTPUT_TOPIC, initialKeyValues);

            // wipe out state store to trigger restore process on restart
            streams.close();
            streams.cleanUp();

            // Restart the stream instance. There should not be exception handling the null value within changelog topic.
            List<KeyValuePair<int, Bytes>> newKeyValues =
                    Collections.singletonList(KeyValuePair.Create(2, new Bytes(new byte[3])));
            IntegrationTestUtils.produceKeyValuesSynchronously(
                    INPUT_TOPIC, newKeyValues, producerConfig, mockTime);
            streams = new KafkaStreams(builder.build(streamsConfiguration), streamsConfiguration);
            streams.start();
            IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(
                    consumerConfig, OUTPUT_TOPIC, newKeyValues);
            streams.close();
        }
    }
}
/*






*

*





*/































