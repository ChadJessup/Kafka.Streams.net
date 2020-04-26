using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Queryable;
using Kafka.Streams.State.ReadOnly;
using Kafka.Streams.Temporary;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Threads.KafkaStreams;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Integration
{
    public class GlobalKTableEOSIntegrationTest
    {
        private const int NUM_BROKERS = 1;
        private static StreamsConfig BROKER_CONFIG;
        //    static {
        //    BROKER_CONFIG = new StreamsConfig();
        //    BROKER_CONFIG.Put("transaction.state.log.replication.factor", (short) 1);
        //    BROKER_CONFIG.Put("transaction.state.log.min.isr", 1);
        //}


        public static EmbeddedKafkaCluster CLUSTER =
                new EmbeddedKafkaCluster(NUM_BROKERS, BROKER_CONFIG);

        private static volatile int testNo = 0;
        private MockTime mockTime = CLUSTER.time;
        private KeyValueMapper<string, long, long> keyMapper = (key, value) => value;
        private ValueJoiner<long, string, string> joiner = (value1, value2) => value1 + "+" + value2;
        private string globalStore = "globalStore";
        private Dictionary<string, string> results = new Dictionary<string, string>();
        private StreamsBuilder builder;
        private StreamsConfig streamsConfiguration;
        private IKafkaStreamsThread kafkaStreams;
        private string globalTableTopic;
        private string streamTopic;
        private IGlobalKTable<long, string> globalTable;
        private IKStream<long, string> stream;
        private Action<long, string> foreachAction;


        public void Before()
        {// throws Exception
            builder = new StreamsBuilder();
            CreateTopics();
            streamsConfiguration = new StreamsConfig();
            string applicationId = "globalTableTopic-table-eos-test-" + ++testNo;
            streamsConfiguration.Put(StreamsConfig.ApplicationIdConfig, applicationId);
            streamsConfiguration.Put(StreamsConfig.BootstrapServersConfig, CLUSTER.bootstrapServers());
            streamsConfiguration.Put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            streamsConfiguration.Put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory());
            streamsConfiguration.Put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
            streamsConfiguration.Put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
            streamsConfiguration.Put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
            globalTable = builder.GlobalTable(
                globalTableTopic, Consumed.With(Serdes.Long(), Serdes.String()),
                Materialized.As<long, string, IKeyValueStore<Bytes, byte[]>>(globalStore)
                    .WithKeySerde(Serdes.Long())
                    .WithValueSerde(Serdes.String()));
            Consumed<string, long> stringLongConsumed = Consumed.With(Serdes.String(), Serdes.Long());
            stream = builder.Stream(streamTopic, stringLongConsumed);
            foreachAction = results.Put;
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
        public void ShouldKStreamGlobalKTableLeftJoin()
        {// throws Exception
            IKStream<long, string> streamTableJoin = stream.LeftJoin(globalTable, keyMapper, joiner);
            streamTableJoin.ForEach(foreachAction);
            ProduceInitialGlobalTableValues();
            StartStreams();
            ProduceTopicValues(streamTopic);

            Dictionary<string, string> expected = new Dictionary<string, string>();
            expected.Put("a", "1+A");
            expected.Put("b", "2+B");
            expected.Put("c", "3+C");
            expected.Put("d", "4+D");
            expected.Put("e", "5+null");

            TestUtils.WaitForCondition(
                () => results.Equals(expected),
                30000L,
                "waiting for initial values");


            ProduceGlobalTableValues();

            IReadOnlyKeyValueStore<long, string> replicatedStore =
                kafkaStreams.Store(globalStore, QueryableStoreTypes.KeyValueStore<long, string>());

            TestUtils.WaitForCondition(
                () => "J".Equals(replicatedStore.Get(5L)),
                30000,
                "waiting for data in replicated store");

            ProduceTopicValues(streamTopic);

            expected.Put("a", "1+F");
            expected.Put("b", "2+G");
            expected.Put("c", "3+H");
            expected.Put("d", "4+I");
            expected.Put("e", "5+J");

            TestUtils.WaitForCondition(
                () => results.Equals(expected),
                30000L,
                "waiting for values");
        }

        [Fact]
        public void ShouldKStreamGlobalKTableJoin()
        {// throws Exception
            IKStream<long, string> streamTableJoin = stream.Join(globalTable, keyMapper, joiner);
            streamTableJoin.ForEach(foreachAction);
            ProduceInitialGlobalTableValues();
            StartStreams();
            ProduceTopicValues(streamTopic);

            Dictionary<string, string> expected = new Dictionary<string, string>();
            expected.Put("a", "1+A");
            expected.Put("b", "2+B");
            expected.Put("c", "3+C");
            expected.Put("d", "4+D");

            TestUtils.WaitForCondition(
                () => results.Equals(expected),
                30000L,
                "waiting for initial values");


            ProduceGlobalTableValues();

            IReadOnlyKeyValueStore<long, string> replicatedStore =
                kafkaStreams.Store(globalStore, QueryableStoreTypes.KeyValueStore<long, string>());

            TestUtils.WaitForCondition(
                () => "J".Equals(replicatedStore.Get(5L)),
                30000,
                "waiting for data in replicated store");

            ProduceTopicValues(streamTopic);

            expected.Put("a", "1+F");
            expected.Put("b", "2+G");
            expected.Put("c", "3+H");
            expected.Put("d", "4+I");
            expected.Put("e", "5+J");

            TestUtils.WaitForCondition(
                () => results.Equals(expected),
                30000L,
                "waiting for values");
        }

        [Fact]
        public void ShouldRestoreTransactionalMessages()
        {// throws Exception
            ProduceInitialGlobalTableValues();

            StartStreams();

            Dictionary<long, string> expected = new Dictionary<long, string>();
            expected.Put(1L, "A");
            expected.Put(2L, "B");
            expected.Put(3L, "C");
            expected.Put(4L, "D");

            TestUtils.WaitForCondition(
                () =>
                {
                    IReadOnlyKeyValueStore<long, string> store;
                    try
                    {
                        store = kafkaStreams.Store(globalStore, QueryableStoreTypes.KeyValueStore<long, string>());
                    }
                    catch (InvalidStateStoreException ex)
                    {
                        return false;
                    }

                    Dictionary<long, string> result = new Dictionary<long, string>();
                    var it = store.All();

                    while (it.MoveNext())
                    {
                        KeyValuePair<long, string> kv = it.Current;
                        result.Put(kv.Key, kv.Value);
                    }
                    return result.Equals(expected);
                },
                30000L,
                "waiting for initial values");
        }

        [Fact]
        public void ShouldNotRestoreAbortedMessages()
        {// throws Exception
            ProduceAbortedMessages();
            ProduceInitialGlobalTableValues();
            ProduceAbortedMessages();

            StartStreams();

            Dictionary<long, string> expected = new Dictionary<long, string>();
            expected.Add(1L, "A");
            expected.Add(2L, "B");
            expected.Add(3L, "C");
            expected.Add(4L, "D");

            TestUtils.WaitForCondition(
                () =>
                {
                    IReadOnlyKeyValueStore<long, string> store;
                    try
                    {
                        store = kafkaStreams.Store(globalStore, QueryableStoreTypes.KeyValueStore<long, string>());
                    }
                    catch (InvalidStateStoreException ex)
                    {
                        return false;
                    }

                    Dictionary<long, string> result = new Dictionary<long, string>();
                    var it = store.All();
                    while (it.MoveNext())
                    {
                        KeyValuePair<long, string> kv = it.Current;
                        result.Put(kv.Key, kv.Value);
                    }
                    return result.Equals(expected);
                },
                30000L,
                "waiting for initial values");
        }

        private void CreateTopics()
        {// throws Exception
            streamTopic = "stream-" + testNo;
            globalTableTopic = "globalTable-" + testNo;
            CLUSTER.CreateTopics(streamTopic);
            CLUSTER.CreateTopic(globalTableTopic, 2, 1);
        }

        private void StartStreams()
        {
            kafkaStreams = new KafkaStreamsThread(builder.Build(), streamsConfiguration);
            kafkaStreams.Start();
        }

        private void ProduceTopicValues(string topic)
        {// throws Exception
            IntegrationTestUtils.ProduceKeyValuesSynchronously(
                    topic,
                    Arrays.asList(
                            KeyValuePair.Create("a", 1L),
                            KeyValuePair.Create("b", 2L),
                            KeyValuePair.Create("c", 3L),
                            KeyValuePair.Create("d", 4L),
                            KeyValuePair.Create("e", 5L)),
                    TestUtils.ProducerConfig(
                            CLUSTER.bootstrapServers(),
                            Serdes.String().Serializer,
                            Serdes.Long().Serializer,
                            new StreamsConfig()),
                    mockTime);
        }

        private void ProduceAbortedMessages()
        {// throws Exception
            StreamsConfig properties = new StreamsConfig();
            properties.Set(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "someid");
            properties.Set(ProducerConfig.RETRIES_CONFIG, 1);
            IntegrationTestUtils.ProduceAbortedKeyValuesSynchronouslyWithTimestamp(
                    globalTableTopic, Arrays.asList(
                            KeyValuePair.Create(1L, "A"),
                            KeyValuePair.Create(2L, "B"),
                            KeyValuePair.Create(3L, "C"),
                            KeyValuePair.Create(4L, "D")
                            ),
                    TestUtils.ProducerConfig(
                                    CLUSTER.bootstrapServers(),
                                    Serdes.Long().Serializer,
                                    Serdes.String().Serializer,
                                    properties),
                    mockTime.NowAsEpochMilliseconds);
        }

        private void ProduceInitialGlobalTableValues()
        {// throws Exception
            ProduceInitialGlobalTableValues(true);
        }

        private void ProduceInitialGlobalTableValues(bool enableTransactions)
        {// throws Exception
            StreamsConfig properties = new StreamsConfig();
            if (enableTransactions)
            {
                properties.Put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "someid");
                properties.Put(ProducerConfig.RETRIES_CONFIG, 1);
            }

            IntegrationTestUtils.ProduceKeyValuesSynchronously(
                    globalTableTopic,
                    Arrays.asList(
                            KeyValuePair.Create(1L, "A"),
                            KeyValuePair.Create(2L, "B"),
                            KeyValuePair.Create(3L, "C"),
                            KeyValuePair.Create(4L, "D")),
                    TestUtils.ProducerConfig(
                            CLUSTER.bootstrapServers(),
                            Serdes.Long().Serializer,
                            Serdes.String().Serializer,
                            properties),
                    mockTime,
                    enableTransactions);
        }

        private void ProduceGlobalTableValues()
        {// throws Exception
            IntegrationTestUtils.ProduceKeyValuesSynchronously(
                    globalTableTopic,
                    Arrays.asList(
                            KeyValuePair.Create(1L, "F"),
                            KeyValuePair.Create(2L, "G"),
                            KeyValuePair.Create(3L, "H"),
                            KeyValuePair.Create(4L, "I"),
                            KeyValuePair.Create(5L, "J")),
                    TestUtils.ProducerConfig(
                            CLUSTER.bootstrapServers(),
                            Serdes.Long().Serializer,
                            Serdes.String().Serializer,
                            new StreamsConfig()),
                    mockTime);
        }
    }
}
