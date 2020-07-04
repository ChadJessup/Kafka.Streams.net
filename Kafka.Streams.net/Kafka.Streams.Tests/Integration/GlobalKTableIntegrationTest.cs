using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Queryable;
using Kafka.Streams.State.ReadOnly;
using Kafka.Streams.Temporary;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;
using Kafka.Streams.Threads.KafkaStreams;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Integration
{
    public class GlobalKTableIntegrationTest
    {
        private const int NUM_BROKERS = 1;

        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

        private static volatile int testNo = 0;
        private readonly IClock mockClock;
        private readonly KeyValueMapper<string, long, long> keyMapper = new KeyValueMapper<string, long, long>((key, value) => value);
        private readonly ValueJoiner<long, string, string> joiner = (value1, value2) => value1 + "+" + value2;
        private readonly string globalStore = "globalStore";
        private readonly StreamsBuilder builder;
        private readonly StreamsConfig streamsConfiguration;
        private string globalTableTopic;
        private string streamTopic;
        private readonly IGlobalKTable<long, string> globalTable;
        private readonly IKStream<string, long> stream;
        private readonly MockProcessorSupplier<string, string> supplier;

        private IKafkaStreamsThread kafkaStreams;

        public GlobalKTableIntegrationTest()
        {
            var sc = new ServiceCollection();
            sc.TryAddSingleton<IClock, MockTime>();

            builder = new StreamsBuilder(sc);
            this.mockClock = builder.Context.Clock;

            CreateTopics();
            string applicationId = "globalTableTopic-table-test-" + ++testNo;
            streamsConfiguration = new StreamsConfig
            {
                ApplicationId = applicationId,
                BootstrapServers = CLUSTER.bootstrapServers(),
                AutoOffsetReset = AutoOffsetReset.Earliest,
                StateStoreDirectory = TestUtils.GetTempDirectory(),
                CacheMaxBytesBuffering = 0,
                CommitIntervalMs = 100
            };

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

        private void WhenShuttingDown()
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
            IKStream<string, string> streamTableJoin = stream.LeftJoin(globalTable, keyMapper, joiner);
            streamTableJoin.Process(supplier);
            ProduceInitialGlobalTableValues();
            StartStreams();
            long firstTimestamp = mockClock.NowAsEpochMilliseconds;
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
                    result.AddRange(supplier.CapturedProcessors(2)[0].LastValueAndTimestampPerKey);
                    result.AddRange(supplier.CapturedProcessors(2)[1].LastValueAndTimestampPerKey);
                    return result.Equals(expected);
                },
                30000L,
                "waiting for initial values");


            firstTimestamp = mockClock.NowAsEpochMilliseconds;
            ProduceGlobalTableValues();

            IReadOnlyKeyValueStore<long, string> replicatedStore =
                kafkaStreams.Store(globalStore, QueryableStoreTypes.KeyValueStore<long, string>());

            TestUtils.WaitForCondition(
                () => "J".Equals(replicatedStore.Get(5L)),
                30000,
                "waiting for data in replicated store");

            IReadOnlyKeyValueStore<long, IValueAndTimestamp<string>> replicatedStoreWithTimestamp =
                kafkaStreams.Store(globalStore, QueryableStoreTypes.TimestampedKeyValueStore<long, string>());

            Assert.Equal(replicatedStoreWithTimestamp.Get(5L), ValueAndTimestamp.Make("J", firstTimestamp + 4L));

            firstTimestamp = mockClock.NowAsEpochMilliseconds;
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
                    result.AddRange(supplier.CapturedProcessors(2)[0].LastValueAndTimestampPerKey);
                    result.AddRange(supplier.CapturedProcessors(2)[1].LastValueAndTimestampPerKey);
                    return result.Equals(expected);
                },
                30000L,
                "waiting for values");
        }

        [Fact]
        public void ShouldKStreamGlobalKTableJoin()
        {// throws Exception
            IKStream<string, string> streamTableJoin = stream.Join(globalTable, keyMapper, joiner);
            streamTableJoin.Process(supplier);
            ProduceInitialGlobalTableValues();
            StartStreams();
            long firstTimestamp = mockClock.NowAsEpochMilliseconds;
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

                    var result = new Dictionary<string, IValueAndTimestamp<string>>();
                    result.AddRange(supplier.CapturedProcessors(2)[0].LastValueAndTimestampPerKey);
                    result.AddRange(supplier.CapturedProcessors(2)[1].LastValueAndTimestampPerKey);

                    return result.Equals(expected);
                },
                30000L,
                "waiting for initial values");


            firstTimestamp = mockClock.NowAsEpochMilliseconds;
            ProduceGlobalTableValues();

            IReadOnlyKeyValueStore<long, string> replicatedStore =
                kafkaStreams.Store(globalStore, QueryableStoreTypes.KeyValueStore<long, string>());

            TestUtils.WaitForCondition(
                () => "J".Equals(replicatedStore.Get(5L)),
                30000,
                "waiting for data in replicated store");

            IReadOnlyKeyValueStore<long, IValueAndTimestamp<string>> replicatedStoreWithTimestamp =
                kafkaStreams.Store(globalStore, QueryableStoreTypes.TimestampedKeyValueStore<long, string>());
            Assert.Equal(replicatedStoreWithTimestamp.Get(5L), ValueAndTimestamp.Make("J", firstTimestamp + 4L));

            firstTimestamp = mockClock.NowAsEpochMilliseconds;
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
                    result.AddRange(supplier.CapturedProcessors(2)[0].LastValueAndTimestampPerKey);
                    result.AddRange(supplier.CapturedProcessors(2)[1].LastValueAndTimestampPerKey);

                    return result.Equals(expected);
                },
                30000L,
                "waiting for values");
        }

        // [Fact]
        // public void ShouldRestoreGlobalInMemoryKTableOnRestart()
        // {// throws Exception
        //     builder = new StreamsBuilder();
        //     globalTable = builder.GlobalTable(
        //         globalTableTopic,
        //         Consumed.With(Serdes.Long(), Serdes.String()),
        //         Materialized.As(Stores.InMemoryKeyValueStore(globalStore)));
        // 
        //     ProduceInitialGlobalTableValues();
        // 
        //     StartStreams();
        //     IReadOnlyKeyValueStore<long, string> store = kafkaStreams.Store(globalStore, QueryableStoreTypes.KeyValueStore<long, string>());
        //     Assert.Equal(4L, store.approximateNumEntries);
        //     IReadOnlyKeyValueStore<long, IValueAndTimestamp<string>> timestampedStore =
        //         kafkaStreams.Store(globalStore, QueryableStoreTypes.TimestampedKeyValueStore<long, string>());
        // 
        //     Assert.Equal(4L, timestampedStore.approximateNumEntries);
        //     kafkaStreams.Close();
        // 
        //     StartStreams();
        //     store = kafkaStreams.Store(globalStore, QueryableStoreTypes.KeyValueStore<long, string>());
        //     Assert.Equal(4L, store.approximateNumEntries);
        //     timestampedStore = kafkaStreams.Store(globalStore, QueryableStoreTypes.TimestampedKeyValueStore<long, string>());
        //     Assert.Equal(4L, timestampedStore.approximateNumEntries);
        // }

        private void CreateTopics()
        {// throws Exception
            streamTopic = "stream-" + testNo;
            globalTableTopic = "globalTable-" + testNo;
            CLUSTER.CreateTopics(streamTopic);
            CLUSTER.CreateTopic(globalTableTopic, 2, 1);
        }

        private void StartStreams()
        {
            kafkaStreams = this.builder.Context.GetRequiredService<IKafkaStreamsThread>();
            //kafkaStreams = new KafkaStreamsThread(builder.Build(), streamsConfiguration);
            kafkaStreams.Start();
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
                    TestUtils.ProducerConfig(
                            CLUSTER.bootstrapServers(),
                            Serdes.String().Serializer,
                            Serdes.Long().Serializer,
                            new StreamsConfig()),
                    mockClock);
        }

        private void ProduceInitialGlobalTableValues()
        {// throws Exception
            IntegrationTestUtils.ProduceKeyValuesSynchronously(
                    globalTableTopic,
                    Arrays.asList(
                            new KeyValuePair<long, string>(1L, "A"),
                            new KeyValuePair<long, string>(2L, "B"),
                            new KeyValuePair<long, string>(3L, "C"),
                            new KeyValuePair<long, string>(4L, "D")),
                    TestUtils.ProducerConfig(
                            CLUSTER.bootstrapServers(),
                            Serdes.Long().Serializer,
                            Serdes.String().Serializer),
                    mockClock);
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
                    TestUtils.ProducerConfig(
                            CLUSTER.bootstrapServers(),
                            Serdes.Long().Serializer,
                            Serdes.String().Serializer,
                            new StreamsConfig()),
                    mockClock);
        }
    }
}
