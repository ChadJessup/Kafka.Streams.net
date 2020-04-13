using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;

using System;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Integration
{
    public class GlobalThreadShutDownOrderTest
    {

        private const int NUM_BROKERS = 1;
        private static StreamsConfig BROKER_CONFIG;

        //static {
        //    BROKER_CONFIG = new StreamsConfig();
        //BROKER_CONFIG.Put("transaction.state.log.replication.factor", (short) 1);
        //    BROKER_CONFIG.Put("transaction.state.log.min.isr", 1);
        //}


        //public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS, BROKER_CONFIG);

        private MockTime mockTime = null;// CLUSTER.time;
        private string globalStore = "globalStore";
        private StreamsBuilder builder;
        private StreamsConfig streamsConfiguration;
        private KafkaStreams kafkaStreams;
        private string globalStoreTopic;
        private string streamTopic;
        private List<long> retrievedValuesList = new List<long>();
        private bool firstRecordProcessed;


        public void Before()
        {// throws Exception
            builder = new StreamsBuilder();
            createTopics();
            streamsConfiguration = new StreamsConfig();
            string applicationId = "global-thread-Shutdown-test";
            streamsConfiguration.Set(StreamsConfig.ApplicationId, applicationId);
            streamsConfiguration.Set(StreamsConfig.BootstrapServers, null);// CLUSTER.bootstrapServers());
            streamsConfiguration.Set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            streamsConfiguration.Set(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory());
            streamsConfiguration.Set(StreamsConfig.CacheMaxBytesBuffering, 0.ToString());
            streamsConfiguration.Set(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100.ToString());

            Consumed<string, long> stringLongConsumed = Consumed.With(Serdes.String(), Serdes.Long());

            KeyValueStoreBuilder<string, long> storeBuilder = new KeyValueStoreBuilder<string, long>(
                Stores.PersistentKeyValueStore(globalStore),
                Serdes.String(),
                Serdes.Long(),
                mockTime);

            builder.AddGlobalStore(
                storeBuilder,
                globalStoreTopic,
                Consumed.With(Serdes.String(), Serdes.Long()),
                new MockProcessorSupplier());

            builder
                .Stream(streamTopic, stringLongConsumed)
                .Process(new GlobalStoreProcessor(globalStore));
        }


        public void WhenShuttingDown()
        {// throws Exception
            if (kafkaStreams != null)
            {
                kafkaStreams.Close();
            }
            IntegrationTestUtils.PurgeLocalStreamsState(streamsConfiguration);
        }

        [Fact]
        public void ShouldFinishGlobalStoreOperationOnShutDown()
        {// throws Exception
            kafkaStreams = new KafkaStreams(builder.Build(), streamsConfiguration);
            populateTopics(globalStoreTopic);
            populateTopics(streamTopic);

            kafkaStreams.start();

            TestUtils.WaitForCondition(
                () => firstRecordProcessed,
                30000,
                "Has not processed record within 30 seconds");

            kafkaStreams.Close(TimeSpan.FromSeconds(30));

            List<long> expectedRetrievedValues = new List<long> { 1L, 2L, 3L, 4L };
            Assert.Equal(expectedRetrievedValues, retrievedValuesList);
        }


        private void CreateTopics()
        {// throws Exception
            streamTopic = "stream-topic";
            globalStoreTopic = "global-store-topic";
            CLUSTER.createTopics(streamTopic);
            CLUSTER.createTopic(globalStoreTopic);
        }


        private void PopulateTopics(string topicName)
        {// throws Exception
            IntegrationTestUtils.ProduceKeyValuesSynchronously(
                topicName,
                Arrays.asList(
                    new KeyValuePair<string, long>("A", 1L),
                    new KeyValuePair<string, long>("B", 2L),
                    new KeyValuePair<string, long>("C", 3L),
                    new KeyValuePair<string, long>("D", 4L)),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    Serdes.String().Serializer,
                    Serdes.Long().Serializer,
                    new StreamsConfig()),
                mockTime);
        }


        private class GlobalStoreProcessor : AbstractProcessor<string, long>
        {

            private IKeyValueStore<string, long> store;
            private readonly string storeName;

            public GlobalStoreProcessor(string storeName)
            {
                this.storeName = storeName;
            }

            public override void Init(IProcessorContext context)
            {
                base.Init(context);
                store = (IKeyValueStore<string, long>)context.GetStateStore(storeName);
            }

            public override void Process(string key, long value)
            {
                firstRecordProcessed = true;
            }

            public override void Close()
            {
                List<string> keys = new List<string> { "A", "B", "C", "D" };
                foreach (string key in keys)
                {
                    // need to simulate thread slow in closing
                    Utils.Sleep(1000);
                    retrievedValuesList.Add(store.Get(key));
                }
            }
        }
    }
}
