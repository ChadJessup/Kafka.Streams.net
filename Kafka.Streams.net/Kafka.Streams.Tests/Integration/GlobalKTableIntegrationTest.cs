using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Mappers;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Queryable;
using Kafka.Streams.State.ReadOnly;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Integration
{
    public class GlobalKTableIntegrationTest
    {
        private const int NUM_BROKERS = 1;


        //public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

        private static volatile int testNo = 0;
        private MockTime mockTime = CLUSTER.time;
        private IKeyValueMapper<string, long, long> keyMapper = new KeyValueMapper<string, long, long>((key, value) => value);
        private IValueJoiner<long, string, string> joiner = (value1, value2) => value1 + "+" + value2;
        private readonly string globalStore = "globalStore";
        private StreamsBuilder builder;
        private StreamsConfig streamsConfiguration;
        private KafkaStreams kafkaStreams;
        private string globalTableTopic;
        private string streamTopic;
        private IGlobalKTable<long, string> globalTable;
        private IKStream<K, V> stream;
        private MockProcessorSupplier<string, string> supplier;

        public void Before()
        {// throws Exception
            builder = new StreamsBuilder();
            CreateTopics();
            streamsConfiguration = new StreamsConfig();
            string applicationId = "globalTableTopic-table-test-" + ++testNo;
            streamsConfiguration.Set(StreamsConfig.ApplicationId, applicationId);
            streamsConfiguration.Set(StreamsConfig.BootstrapServers, null);// CLUSTER.bootstrapServers());
            streamsConfiguration.Set(StreamsConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            streamsConfiguration.Set(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory());
            streamsConfiguration.Set(StreamsConfig.CacheMaxBytesBuffering, 0.ToString());
            streamsConfiguration.Set(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100.ToString());
            globalTable = builder.GlobalTable(
                globalTableTopic,
                Consumed.With(Serdes.Long(), Serdes.String()),
                Materialized.As<long, string, IKeyValueStore<Bytes, byte[]>>(globalStore)
                    .WithKeySerde(Serdes.Long())
                    .WithValueSerde(Serdes.String()));

            Consumed<string, long> stringLongConsumed = Consumed.With(Serdes.String(), Serdes.Long());
            stream = builder.Stream(streamTopic, stringLongConsumed);
            supplier = new MockProcessorSupplier<string, string>();
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
            IKStream<K, V> streamTableJoin = stream.LeftJoin(globalTable, keyMapper, joiner);
            streamTableJoin.Process(supplier);
            ProduceInitialGlobalTableValues();
            StartStreams();
            long firstTimestamp = mockTime.NowAsEpochMilliseconds;
            ProduceTopicValues(streamTopic);

            var expected = new Dictionary<string, IValueAndTimestamp<string>?>
            {
                { "a", ValueAndTimestamp.Make("1+A", firstTimestamp) },
                { "b", ValueAndTimestamp.Make("2+B", firstTimestamp + 1L) },
                { "c", ValueAndTimestamp.Make("3+C", firstTimestamp + 2L) },
                { "d", ValueAndTimestamp.Make("4+D", firstTimestamp + 3L) },
                { "e", ValueAndTimestamp.Make("5+null", firstTimestamp + 4L) }
            };

            TestUtils.WaitForCondition(
                () =>
                {
                    if (supplier.CapturedProcessorsCount() < 2)
                    {
                        return false;
                    }

                    var result = new Dictionary<string, IValueAndTimestamp<string>>();
                    result.AddRange(supplier.CapturedProcessors(2)[0].lastValueAndTimestampPerKey);
                    result.AddRange(supplier.CapturedProcessors(2)[1].lastValueAndTimestampPerKey);
                    return result.Equals(expected);
                },
                30000L,
                "waiting for initial values");


            firstTimestamp = mockTime.NowAsEpochMilliseconds; ;
            ProduceGlobalTableValues();

            IReadOnlyKeyValueStore<long, string> replicatedStore =
                kafkaStreams.store(globalStore, QueryableStoreTypes.KeyValueStore);

            TestUtils.WaitForCondition(
                () => "J".Equals(replicatedStore.Get(5L)),
                30000,
                "waiting for data in replicated store");

            IReadOnlyKeyValueStore<long, IValueAndTimestamp<string>> replicatedStoreWithTimestamp =
                kafkaStreams.store(globalStore, QueryableStoreTypes.TimestampedKeyValueStore);
            Assert.Equal(replicatedStoreWithTimestamp.Get(5L), ValueAndTimestamp.Make("J", firstTimestamp + 4L));

            firstTimestamp = mockTime.NowAsEpochMilliseconds; ;
            ProduceTopicValues(streamTopic);

            expected.Add("a", ValueAndTimestamp.Make("1+F", firstTimestamp));
            expected.Add("b", ValueAndTimestamp.Make("2+G", firstTimestamp + 1L));
            expected.Add("c", ValueAndTimestamp.Make("3+H", firstTimestamp + 2L));
            expected.Add("d", ValueAndTimestamp.Make("4+I", firstTimestamp + 3L));
            expected.Add("e", ValueAndTimestamp.Make("5+J", firstTimestamp + 4L));

            TestUtils.WaitForCondition(
                () =>
                {
                    if (supplier.CapturedProcessorsCount() < 2)
                    {
                        return false;
                    }

                    var result = new Dictionary<string, IValueAndTimestamp<string>>();
                    result.AddRange(supplier.CapturedProcessors(2)[0].lastValueAndTimestampPerKey);
                    result.AddRange(supplier.CapturedProcessors(2)[1].lastValueAndTimestampPerKey);
                    return result.Equals(expected);
                },
                30000L,
                "waiting for values");
        }

        [Fact]
        public void ShouldKStreamGlobalKTableJoin()
        {// throws Exception
            IKStream<K, V> streamTableJoin = stream.Join(globalTable, keyMapper, joiner);
            streamTableJoin.Process(supplier);
            ProduceInitialGlobalTableValues();
            StartStreams();
            long firstTimestamp = mockTime.NowAsEpochMilliseconds; ;
            ProduceTopicValues(streamTopic);

            var expected = new Dictionary<string, IValueAndTimestamp<string>?>
            {
                { "a", ValueAndTimestamp.Make("1+A", firstTimestamp) },
                { "b", ValueAndTimestamp.Make("2+B", firstTimestamp + 1L) },
                { "c", ValueAndTimestamp.Make("3+C", firstTimestamp + 2L) },
                { "d", ValueAndTimestamp.Make("4+D", firstTimestamp + 3L) }
            };

            TestUtils.WaitForCondition(
                () =>
                {
                    if (supplier.CapturedProcessorsCount() < 2)
                    {
                        return false;
                    }

                    var result = new Dictionary<string, IValueAndTimestamp<string>?>();
                    result.AddRange(supplier.CapturedProcessors(2)[0].lastValueAndTimestampPerKey);
                    result.AddRange(supplier.CapturedProcessors(2)[1].lastValueAndTimestampPerKey);

                    return result.Equals(expected);
                },
                30000L,
                "waiting for initial values");


            firstTimestamp = mockTime.NowAsEpochMilliseconds; ;
            ProduceGlobalTableValues();

            IReadOnlyKeyValueStore<long, string> replicatedStore =
                kafkaStreams.store(globalStore, QueryableStoreTypes.KeyValueStore);

            TestUtils.WaitForCondition(
                () => "J".Equals(replicatedStore.Get(5L)),
                30000,
                "waiting for data in replicated store");

            IReadOnlyKeyValueStore<long, IValueAndTimestamp<string>> replicatedStoreWithTimestamp =
                kafkaStreams.store(globalStore, QueryableStoreTypes.TimestampedKeyValueStore);
            Assert.Equal(replicatedStoreWithTimestamp.Get(5L), ValueAndTimestamp.Make("J", firstTimestamp + 4L));

            firstTimestamp = mockTime.NowAsEpochMilliseconds;
            ProduceTopicValues(streamTopic);

            expected.Add("a", ValueAndTimestamp.Make("1+F", firstTimestamp));
            expected.Add("b", ValueAndTimestamp.Make("2+G", firstTimestamp + 1L));
            expected.Add("c", ValueAndTimestamp.Make("3+H", firstTimestamp + 2L));
            expected.Add("d", ValueAndTimestamp.Make("4+I", firstTimestamp + 3L));
            expected.Add("e", ValueAndTimestamp.Make("5+J", firstTimestamp + 4L));

            TestUtils.WaitForCondition(
                () =>
                {
                    if (supplier.CapturedProcessorsCount() < 2)
                    {
                        return false;
                    }

                    var result = new Dictionary<string, IValueAndTimestamp<string>>();
                    result.AddRange(supplier.CapturedProcessors(2)[0].lastValueAndTimestampPerKey);
                    result.AddRange(supplier.CapturedProcessors(2)[1].lastValueAndTimestampPerKey);

                    return result.Equals(expected);
                },
                30000L,
                "waiting for values");
        }

        [Fact]
        public void ShouldRestoreGlobalInMemoryKTableOnRestart()
        {// throws Exception
            builder = new StreamsBuilder();
            globalTable = builder.GlobalTable(
                globalTableTopic,
                Consumed.With(Serdes.Long(), Serdes.String()),
                Materialized.As(Stores.InMemoryKeyValueStore(globalStore)));

            ProduceInitialGlobalTableValues();

            StartStreams();
            IReadOnlyKeyValueStore<long, string> store = kafkaStreams.store(globalStore, QueryableStoreTypes.KeyValueStore);
            Assert.Equal(4L, store.approximateNumEntries);
            IReadOnlyKeyValueStore<long, IValueAndTimestamp<string>> timestampedStore =
                kafkaStreams.store(globalStore, QueryableStoreTypes.TimestampedKeyValueStore);

            Assert.Equal(4L, timestampedStore.approximateNumEntries);
            kafkaStreams.Close();

            StartStreams();
            store = kafkaStreams.store(globalStore, QueryableStoreTypes.KeyValueStore);
            Assert.Equal(4L, store.approximateNumEntries);
            timestampedStore = kafkaStreams.store(globalStore, QueryableStoreTypes.TimestampedKeyValueStore);
            Assert.Equal(4L, timestampedStore.approximateNumEntries);
        }

        private void CreateTopics()
        {// throws Exception
            streamTopic = "stream-" + testNo;
            globalTableTopic = "globalTable-" + testNo;
            CLUSTER.createTopics(streamTopic);
            CLUSTER.createTopic(globalTableTopic, 2, 1);
        }

        private void StartStreams()
        {
            kafkaStreams = new KafkaStreams(builder.Build(), streamsConfiguration);
            kafkaStreams.start();
        }

        private void ProduceTopicValues(string topic)
        {// throws Exception
            IntegrationTestUtils.ProduceKeyValuesSynchronously(
                    topic,
                    Arrays.asList(
                            new KeyValuePair<string, long>("a", 1L),
                            new KeyValuePair<string, long>("b", 2L),
                            new KeyValuePair<string, long>("c", 3L),
                            new KeyValuePair<string, long>("d", 4L),
                            new KeyValuePair<string, long>("e", 5L)),
                    TestUtils.producerConfig(
                            CLUSTER.bootstrapServers(),
                            Serdes.String().Serializer,
                            Serdes.Long().Serializer,
                            new StreamsConfig()),
                    mockTime);
        }

        private void ProduceInitialGlobalTableValues()
        {// throws Exception
            IntegrationTestUtils.ProduceKeyValuesSynchronously(
                    globalTableTopic,
                    Arrays.asList(
                            new KeyValuePair<long, string>(1L, "A"),
                            new KeyValuePair<long, string>(2L, "B"),
                            new KeyValuePair<long, string>(3L, "C"),
                            new KeyValuePair<long, string>(4L, "D")
                            ),
                    TestUtils.producerConfig(
                            CLUSTER.bootstrapServers(),
                            Serdes.Long().Serializer,
                            Serdes.String().Serializer
                            ),
                    mockTime);
        }

        private void ProduceGlobalTableValues()
        {// throws Exception
            IntegrationTestUtils.ProduceKeyValuesSynchronously(
                    globalTableTopic,
                    Arrays.asList(
                            new KeyValuePair<long, string>(1L, "F"),
                            new KeyValuePair<long, string>(2L, "G"),
                            new KeyValuePair<long, string>(3L, "H"),
                            new KeyValuePair<long, string>(4L, "I"),
                            new KeyValuePair<long, string>(5L, "J")),
                    TestUtils.producerConfig(
                            //CLUSTER.bootstrapServers(),
                            Serdes.Long().Serializer,
                            Serdes.String().Serializer,
                            new StreamsConfig()),
                    mockTime);
        }
    }
}
