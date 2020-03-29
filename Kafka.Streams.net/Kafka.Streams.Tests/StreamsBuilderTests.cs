using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class StreamsBuilderTest
    {

        private static string STREAM_TOPIC = "stream-topic";

        private static string STREAM_OPERATION_NAME = "stream-operation";

        private static string STREAM_TOPIC_TWO = "stream-topic-two";

        private static string TABLE_TOPIC = "table-topic";

        private StreamsBuilder builder = new StreamsBuilder();

        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());

        [Fact]
        public void ShouldNotThrowNullPointerIfOptimizationsNotSpecified()
        {
            var properties = new StreamsConfig();

            var builder = new StreamsBuilder();
            builder.Build(properties);
        }

        [Fact]
        public void ShouldAllowJoinUnmaterializedFilteredKTable()
        {
            IKTable<Bytes, string> filteredKTable = builder
                .Table<Bytes, string>(TABLE_TOPIC)
                .filter(MockPredicate.AllGoodPredicate<Bytes, string>());

            builder
                .Stream<Bytes, string>(STREAM_TOPIC)
                .Join(filteredKTable, MockValueJoiner.TOSTRING_JOINER<string, string>());

            builder.Build();

            ProcessorTopology topology =
                builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();

            Assert.Single(topology.StateStores);

            Assert.Equal(
                topology.processorConnectedStateStores("KSTREAM-JOIN-0000000005"),
                new HashSet<string> { topology.StateStores[0].name });

            Assert.False(topology.processorConnectedStateStores("KTABLE-FILTER-0000000003").Any());
        }

        /*
                [Fact]
                public void shouldAllowJoinMaterializedFilteredKTable()
                {
                    IKTable<Bytes, string> filteredKTable = builder
                        .< Bytes, string> table(TABLE_TOPIC)
                         .filter(MockPredicate.allGoodPredicate(), Materialized.As("store"));
                    builder
                        .< Bytes, string > stream(STREAM_TOPIC)
                         .join(filteredKTable, MockValueJoiner.TOSTRING_JOINER);
                    builder.Build();

                    ProcessorTopology topology =
                        builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();

                    Assert.Equal(
                        topology.StateStores.Count,
                        1);
                    Assert.Equal(
                        topology.processorConnectedStateStores("KSTREAM-JOIN-0000000005"),
                        Collections.singleton("store"));
                    Assert.Equal(
                        topology.processorConnectedStateStores("KTABLE-FILTER-0000000003"),
                        Collections.singleton("store"));
                }

                [Fact]
                public void shouldAllowJoinUnmaterializedMapValuedKTable()
                {
                    IKTable<Bytes, string> mappedKTable = builder
                        .< Bytes, string> table(TABLE_TOPIC)
                         .mapValues(MockMapper.noOpValueMapper());
                    builder
                        .< Bytes, string > stream(STREAM_TOPIC)
                         .join(mappedKTable, MockValueJoiner.TOSTRING_JOINER);
                    builder.Build();

                    ProcessorTopology topology =
                        builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();

                    Assert.Equal(
                        topology.StateStores.Count,
                        1);
                    Assert.Equal(
                        topology.processorConnectedStateStores("KSTREAM-JOIN-0000000005"),
                        Collections.singleton(topology.StateStores.get(0).name()));
                    Assert.True(
                        topology.processorConnectedStateStores("KTABLE-MAPVALUES-0000000003").isEmpty());
                }

                [Fact]
                public void shouldAllowJoinMaterializedMapValuedKTable()
                {
                    IKTable<Bytes, string> mappedKTable = builder
                        .< Bytes, string> table(TABLE_TOPIC)
                         .mapValues(MockMapper.noOpValueMapper(), Materialized.As("store"));
                    builder
                        .< Bytes, string > stream(STREAM_TOPIC)
                         .join(mappedKTable, MockValueJoiner.TOSTRING_JOINER);
                    builder.Build();

                    ProcessorTopology topology =
                        builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();

                    Assert.Equal(
                        topology.StateStores.Count,
                        1);
                    Assert.Equal(
                        topology.processorConnectedStateStores("KSTREAM-JOIN-0000000005"),
                        Collections.singleton("store"));
                    Assert.Equal(
                        topology.processorConnectedStateStores("KTABLE-MAPVALUES-0000000003"),
                        Collections.singleton("store"));
                }

                [Fact]
                public void shouldAllowJoinUnmaterializedJoinedKTable()
                {
                    IKTable<Bytes, string> table1 = builder.Table("table-topic1");
                    IKTable<Bytes, string> table2 = builder.Table("table-topic2");
                    builder
                        .< Bytes, string > stream(STREAM_TOPIC)
                         .join(table1.join(table2, MockValueJoiner.TOSTRING_JOINER), MockValueJoiner.TOSTRING_JOINER);
                    builder.Build();

                    ProcessorTopology topology =
                        builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();

                    Assert.Equal(
                        topology.StateStores.Count,
                        2);
                    Assert.Equal(
                        topology.processorConnectedStateStores("KSTREAM-JOIN-0000000010"),
                        Utils.mkSet(topology.StateStores.get(0).name(), topology.StateStores.get(1).name()));
                    Assert.True(
                        topology.processorConnectedStateStores("KTABLE-MERGE-0000000007").isEmpty());
                }

                [Fact]
                public void shouldAllowJoinMaterializedJoinedKTable()
                {
                    IKTable<Bytes, string> table1 = builder.Table("table-topic1");
                    IKTable<Bytes, string> table2 = builder.Table("table-topic2");
                    builder
                        .< Bytes, string > stream(STREAM_TOPIC)
                         .join(
                             table1.join(table2, MockValueJoiner.TOSTRING_JOINER, Materialized.As("store")),
                        MockValueJoiner.TOSTRING_JOINER);
                    builder.Build();

                    ProcessorTopology topology =
                        builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();

                    Assert.Equal(
                        topology.StateStores.Count,
                        3);
                    Assert.Equal(
                        topology.processorConnectedStateStores("KSTREAM-JOIN-0000000010"),
                        Collections.singleton("store"));
                    Assert.Equal(
                        topology.processorConnectedStateStores("KTABLE-MERGE-0000000007"),
                        Collections.singleton("store"));
                }

                [Fact]
                public void shouldAllowJoinMaterializedSourceKTable()
                {
                    IKTable<Bytes, string> table = builder.Table(TABLE_TOPIC);
                    builder.< Bytes, string > stream(STREAM_TOPIC).join(table, MockValueJoiner.TOSTRING_JOINER);
                    builder.Build();

                    ProcessorTopology topology =
                        builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();

                    Assert.Equal(
                        topology.StateStores.Count,
                        1);
                    Assert.Equal(
                        topology.processorConnectedStateStores("KTABLE-SOURCE-0000000002"),
                        Collections.singleton(topology.StateStores.get(0).name()));
                    Assert.Equal(
                        topology.processorConnectedStateStores("KSTREAM-JOIN-0000000004"),
                        Collections.singleton(topology.StateStores.get(0).name()));
                }

                [Fact]
                public void shouldProcessingFromSinkTopic()
                {
                    IKStream<string, string> source = builder.Stream("topic-source");
                    source.to("topic-sink");

                    MockProcessorSupplier<string, string> processorSupplier = new MockProcessorSupplier<>();
                    source.process(processorSupplier);

                    ConsumerRecordFactory<string, string> recordFactory =
                        new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L);

                    try
                    {
                        var driver = new TopologyTestDriver(builder.Build(), props)) {
                            driver.pipeInput(recordFactory.create("topic-source", "A", "aa"));
                        }

                        // no exception .As thrown
                        Assert.Equal(Collections.singletonList(new KeyValueTimestamp<>("A", "aa", 0)),
                                 processorSupplier.theCapturedProcessor().processed);
                    }

            [Fact]
                public void shouldProcessViaThroughTopic()
                {
                    IKStream<string, string> source = builder.Stream("topic-source");
                    IKStream<string, string> through = source.through("topic-sink");

                    MockProcessorSupplier<string, string> sourceProcessorSupplier = new MockProcessorSupplier<>();
                    source.process(sourceProcessorSupplier);

                    MockProcessorSupplier<string, string> throughProcessorSupplier = new MockProcessorSupplier<>();
                    through.process(throughProcessorSupplier);

                    ConsumerRecordFactory<string, string> recordFactory =
                        new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L);

                    try {
var driver = new TopologyTestDriver(builder.Build(), props);
                        driver.pipeInput(recordFactory.create("topic-source", "A", "aa"));
                    }

                    Assert.Equal(Collections.singletonList(new KeyValueTimestamp<>("A", "aa", 0)), sourceProcessorSupplier.theCapturedProcessor().processed);
                    Assert.Equal(Collections.singletonList(new KeyValueTimestamp<>("A", "aa", 0)), throughProcessorSupplier.theCapturedProcessor().processed);
                    }

                [Fact]
                public void shouldMergeStreams()
                {
                    string topic1 = "topic-1";
                    string topic2 = "topic-2";

                    IKStream<string, string> source1 = builder.Stream(topic1);
                    IKStream<string, string> source2 = builder.Stream(topic2);
                    IKStream<string, string> merged = source1.merge(source2);

                    MockProcessorSupplier<string, string> processorSupplier = new MockProcessorSupplier<>();
                    merged.process(processorSupplier);

                    ConsumerRecordFactory<string, string> recordFactory =
                        new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L);

                    try {
var driver = new TopologyTestDriver(builder.Build(), props);
                        driver.pipeInput(recordFactory.create(topic1, "A", "aa"));
                        driver.pipeInput(recordFactory.create(topic2, "B", "bb"));
                        driver.pipeInput(recordFactory.create(topic2, "C", "cc"));
                        driver.pipeInput(recordFactory.create(topic1, "D", "dd"));
                    }

                    Assert.EqualsasList(new KeyValueTimestamp<>("A", "aa", 0),
                            new KeyValueTimestamp<>("B", "bb", 0),
                            new KeyValueTimestamp<>("C", "cc", 0),
                            new KeyValueTimestamp<>("D", "dd", 0)), processorSupplier.theCapturedProcessor().processed);
                    }

            [Fact]
                public void shouldUseSerdesDefinedInMaterializedToConsumeTable()
                {
                    Dictionary<long, string> results = new HashMap<>();
                    string topic = "topic";
                    ForeachAction<long, string> action = results.Add;
                    builder.Table(topic, Materialized.< long, string, IKeyValueStore<Bytes, byte[]> > As("store")
                            .withKeySerde(Serdes.Long())
                            .withValueSerde(Serdes.String()))
                    .toStream().ForEach (action) ;

                    ConsumerRecordFactory<long, string> recordFactory =
                        new ConsumerRecordFactory<>(new LongSerializer(), Serdes.String());

                    try {
var driver = new TopologyTestDriver(builder.Build(), props);
                        driver.pipeInput(recordFactory.create(topic, 1L, "value1"));
                        driver.pipeInput(recordFactory.create(topic, 2L, "value2"));

                        IKeyValueStore<long, string> store = driver.getKeyValueStore("store");
                        Assert.Equal("value1", store.get(1L));
                        Assert.Equal("value2", store.get(2L));
                        Assert.Equal("value1", results.get(1L));
                        Assert.Equal("value2", results.get(2L));
                    }
                    }

            [Fact]
                public void shouldUseSerdesDefinedInMaterializedToConsumeGlobalTable()
                {
                    string topic = "topic";
                    builder.globalTable(topic, Materialized.< long, string, IKeyValueStore<Bytes, byte[]> > As("store")
                            .withKeySerde(Serdes.Long())
                            .withValueSerde(Serdes.String()));

                    ConsumerRecordFactory<long, string> recordFactory =
                        new ConsumerRecordFactory<>(new LongSerializer(), Serdes.String());

                    try
                    {
                        var driver = new TopologyTestDriver(builder.Build(), props);
                        driver.pipeInput(recordFactory.create(topic, 1L, "value1"));
                        driver.pipeInput(recordFactory.create(topic, 2L, "value2"));
                        IKeyValueStore<long, string> store = driver.getKeyValueStore("store");

                        Assert.Equal("value1", store.get(1L));
                        Assert.Equal("value2", store.get(2L));
                    }
                    }

                [Fact]
                public void shouldNotMaterializeStoresIfNotRequired()
                {
                    string topic = "topic";
                    builder.Table(topic, Materialized.with(Serdes.Long(), Serdes.String()));

                    ProcessorTopology topology =
                        builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();

                    Assert.Equal(0, topology.StateStores.Count);
                }

                [Fact]
                public void shouldReuseSourceTopicAsChangelogsWithOptimization20()
                {
                    string topic = "topic";
                    builder.Table(topic, Materialized.< long, string, IKeyValueStore<Bytes, byte[]> > As("store"));
                    StreamsConfig props = StreamsTestConfigs.GetStandardConfig();
                    props.Add(StreamsConfigPropertyNames.TOPOLOGY_OPTIMIZATION, StreamsConfigPropertyNames.OPTIMIZE);
                    Topology topology = builder.Build(props);

                    InternalTopologyBuilder InternalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
                    InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props));

                    Assert.Equal(
                        InternalTopologyBuilder.Build().storeToChangelogTopic(),
                        Collections.singletonMap("store", "topic"));
                    Assert.Equal(
                        InternalTopologyBuilder.getStateStores().keySet(),
                        Collections.singleton("store"));
                    Assert.Equal(
                        InternalTopologyBuilder.getStateStores().get("store").loggingEnabled(),
                        false);
                    Assert.Equal(
                        InternalTopologyBuilder.topicGroups().get(0).stateChangelogTopics.isEmpty(),
                        true);
                }

                [Fact]
                public void shouldNotReuseSourceTopicAsChangelogsByDefault()
                {
                    string topic = "topic";
                    builder.Table(topic, Materialized<long, string, IKeyValueStore<Bytes, byte[]>>.As("store"));

                    InternalTopologyBuilder InternalTopologyBuilder = TopologyWrapper.getInternalTopologyBuilder(builder.Build());
                    InternalTopologyBuilder.SetApplicationId("appId");

                    Assert.Equal(
                        InternalTopologyBuilder.Build().storeToChangelogTopic(),
                        Collections.singletonMap("store", "appId-store-changelog"));
                    Assert.Equal(
                        InternalTopologyBuilder.getStateStores().keySet(),
                        Collections.singleton("store"));
                    Assert.Equal(
                        InternalTopologyBuilder.getStateStores().get("store").loggingEnabled(),
                        true);
                    Assert.Equal(
                        InternalTopologyBuilder.topicGroups().get(0).stateChangelogTopics.keySet(),
                        Collections.singleton("appId-store-changelog"));
                }

        [Fact]// // (expected = TopologyException))
                public void shouldThrowExceptionWhenNoTopicPresent()
                {
                    builder.Stream(Collections.emptyList());
                    builder.Build();
                }

        [Fact]// // (expected = NullPointerException))
                public void shouldThrowExceptionWhenTopicNamesAreNull()
                {
                    builder.Stream(new List<string> { null, null });
                    builder.Build();
                }

                [Fact]
                public void shouldUseSpecifiedNameForStreamSourceProcessor()
                {
                    string expected = "source-node";
                    builder.Stream(STREAM_TOPIC, Consumed.As(expected));
                    builder.Stream(STREAM_TOPIC_TWO);
                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology, expected, "KSTREAM-SOURCE-0000000001");
                }

                [Fact]
                public void shouldUseSpecifiedNameForTableSourceProcessor()
                {
                    string expected = "source-node";
                    builder.Table(STREAM_TOPIC, Consumed.As(expected));
                    builder.Table(STREAM_TOPIC_TWO);
                    builder.Build();

                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();

                   .AssertSpecifiedNameForOperation(
                            topology,
                            expected,
                            expected + "-table-source",
                            "KSTREAM-SOURCE-0000000004",
                            "KTABLE-SOURCE-0000000005");
                }

                [Fact]
                public void shouldUseSpecifiedNameForGlobalTableSourceProcessor()
                {
                    string expected = "source-processor";
                    builder.globalTable(STREAM_TOPIC, Consumed.As(expected));
                    builder.globalTable(STREAM_TOPIC_TWO);
                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();

                   .AssertSpecifiedNameForStateStore(
                            topology.globalStateStores(),
                            "stream-topic-STATE-STORE-0000000000",
                            "stream-topic-two-STATE-STORE-0000000003"
                    );
                }

                [Fact]
                public void shouldUseSpecifiedNameForSinkProcessor()
                {
                    string expected = "sink-processor";
                    IKStream<object, object> stream = builder.Stream(STREAM_TOPIC);
                    stream.to(STREAM_TOPIC_TWO, Produced.As(expected));
                    stream.to(STREAM_TOPIC_TWO);
                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology, "KSTREAM-SOURCE-0000000000", expected, "KSTREAM-SINK-0000000002");
                }

                [Fact]
                public void shouldUseSpecifiedNameForMapOperation()
                {
                    builder.Stream(STREAM_TOPIC).map(KeyValuePair::pair, Named.As(STREAM_OPERATION_NAME));
                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology, "KSTREAM-SOURCE-0000000000", STREAM_OPERATION_NAME);
                }

                [Fact]
                public void shouldUseSpecifiedNameForMapValuesOperation()
                {
                    builder.Stream(STREAM_TOPIC).mapValues(v => v, Named.As(STREAM_OPERATION_NAME));
                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology, "KSTREAM-SOURCE-0000000000", STREAM_OPERATION_NAME);
                }

                [Fact]
                public void shouldUseSpecifiedNameForMapValuesWithKeyOperation()
                {
                    builder.Stream(STREAM_TOPIC).mapValues((k, v) => v, Named.As(STREAM_OPERATION_NAME));
                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology, "KSTREAM-SOURCE-0000000000", STREAM_OPERATION_NAME);
                }

                [Fact]
                public void shouldUseSpecifiedNameForFilterOperation()
                {
                    builder.Stream(STREAM_TOPIC).filter((k, v) => true, Named.As(STREAM_OPERATION_NAME));
                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology, "KSTREAM-SOURCE-0000000000", STREAM_OPERATION_NAME);
                }

                [Fact]
                public void shouldUseSpecifiedNameForForEachOperation()
                {
                    builder.Stream(STREAM_TOPIC).ForEach ((k, v) => { }, Named.As(STREAM_OPERATION_NAME));
                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology, "KSTREAM-SOURCE-0000000000", STREAM_OPERATION_NAME);
                }

                [Fact]
                public void shouldUseSpecifiedNameForTransform()
                {
                    builder.Stream(STREAM_TOPIC).transform(() => null, Named.As(STREAM_OPERATION_NAME));
                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology, "KSTREAM-SOURCE-0000000000", STREAM_OPERATION_NAME);
                }

                [Fact]
                // 
                public void shouldUseSpecifiedNameForTransformValues()
                {
                    builder.Stream(STREAM_TOPIC).transformValues(() => (ValueTransformer)null, Named.As(STREAM_OPERATION_NAME));
                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology, "KSTREAM-SOURCE-0000000000", STREAM_OPERATION_NAME);
                }

                [Fact]
                // 
            public void shouldUseSpecifiedNameForTransformValuesWithKey()
                {
                    builder.Stream(STREAM_TOPIC).transformValues(() => (ValueTransformerWithKey)null, Named.As(STREAM_OPERATION_NAME));
                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology, "KSTREAM-SOURCE-0000000000", STREAM_OPERATION_NAME);
                }

                [Fact]
                // 
            public void shouldUseSpecifiedNameForBranchOperation()
                {
                    builder.Stream(STREAM_TOPIC)
                            .branch(Named.As("branch-processor"), (k, v) => true, (k, v) => false);

                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology,
                            "KSTREAM-SOURCE-0000000000",
                            "branch-processor",
                            "branch-processor-predicate-0",
                            "branch-processor-predicate-1");
                }

                [Fact]
                public void shouldUseSpecifiedNameForJoinOperationBetweenKStreamAndKTable()
                {
                    IKStream<string, string> streamOne = builder.Stream(STREAM_TOPIC);
                    IKTable<string, string> streamTwo = builder.Table("table-topic");
                    streamOne.join(streamTwo, (value1, value2) => value1, Joined.As(STREAM_OPERATION_NAME));
                    builder.Build();

                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology,
                            "KSTREAM-SOURCE-0000000000",
                            "KSTREAM-SOURCE-0000000002",
                            "KTABLE-SOURCE-0000000003",
                            STREAM_OPERATION_NAME);
                }

                [Fact]
                public void shouldUseSpecifiedNameForLeftJoinOperationBetweenKStreamAndKTable()
                {
                    IKStream<string, string> streamOne = builder.Stream(STREAM_TOPIC);
                    IKTable<string, string> streamTwo = builder.Table(STREAM_TOPIC_TWO);
                    streamOne.leftJoin(streamTwo, (value1, value2) => value1, Joined.As(STREAM_OPERATION_NAME));
                    builder.Build();

                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology,
                            "KSTREAM-SOURCE-0000000000",
                            "KSTREAM-SOURCE-0000000002",
                            "KTABLE-SOURCE-0000000003",
                            STREAM_OPERATION_NAME);
                }

                [Fact]
                public void shouldUseSpecifiedNameForLeftJoinOperationBetweenKStreamAndKStream()
                {
                    IKStream<string, string> streamOne = builder.Stream(STREAM_TOPIC);
                    IKStream<string, string> streamTwo = builder.Stream(STREAM_TOPIC_TWO);

                    streamOne.leftJoin(streamTwo, (value1, value2) => value1, JoinWindows.of(Duration.ofHours(1)), Joined.As(STREAM_OPERATION_NAME));
                    builder.Build();

                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForStateStore(topology.StateStores,
                            STREAM_OPERATION_NAME + "-this-join-store", STREAM_OPERATION_NAME + "-outer-other-join-store"
                    );
                   .AssertSpecifiedNameForOperation(topology,
                            "KSTREAM-SOURCE-0000000000",
                            "KSTREAM-SOURCE-0000000001",
                            STREAM_OPERATION_NAME + "-this-windowed",
                            STREAM_OPERATION_NAME + "-other-windowed",
                            STREAM_OPERATION_NAME + "-this-join",
                            STREAM_OPERATION_NAME + "-outer-other-join",
                            STREAM_OPERATION_NAME + "-merge");
                }

                [Fact]
                public void shouldUseSpecifiedNameForJoinOperationBetweenKStreamAndKStream()
                {
                    IKStream<string, string> streamOne = builder.Stream(STREAM_TOPIC);
                    IKStream<string, string> streamTwo = builder.Stream(STREAM_TOPIC_TWO);

                    streamOne.join(streamTwo, (value1, value2) => value1, JoinWindows.of(Duration.ofHours(1)), Joined.As(STREAM_OPERATION_NAME));
                    builder.Build();

                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForStateStore(topology.StateStores,
                            STREAM_OPERATION_NAME + "-this-join-store",
                            STREAM_OPERATION_NAME + "-other-join-store"
                    );
                   .AssertSpecifiedNameForOperation(topology,
                            "KSTREAM-SOURCE-0000000000",
                            "KSTREAM-SOURCE-0000000001",
                            STREAM_OPERATION_NAME + "-this-windowed",
                            STREAM_OPERATION_NAME + "-other-windowed",
                            STREAM_OPERATION_NAME + "-this-join",
                            STREAM_OPERATION_NAME + "-other-join",
                            STREAM_OPERATION_NAME + "-merge");
                }

                [Fact]
                public void shouldUseSpecifiedNameForOuterJoinOperationBetweenKStreamAndKStream()
                {
                    IKStream<string, string> streamOne = builder.Stream(STREAM_TOPIC);
                    IKStream<string, string> streamTwo = builder.Stream(STREAM_TOPIC_TWO);

                    streamOne.outerJoin(streamTwo, (value1, value2) => value1, JoinWindows.of(Duration.ofHours(1)), Joined.As(STREAM_OPERATION_NAME));
                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForStateStore(topology.StateStores,
                            STREAM_OPERATION_NAME + "-outer-this-join-store",
                            STREAM_OPERATION_NAME + "-outer-other-join-store");
                   .AssertSpecifiedNameForOperation(topology,
                            "KSTREAM-SOURCE-0000000000",
                            "KSTREAM-SOURCE-0000000001",
                            STREAM_OPERATION_NAME + "-this-windowed",
                            STREAM_OPERATION_NAME + "-other-windowed",
                            STREAM_OPERATION_NAME + "-outer-this-join",
                            STREAM_OPERATION_NAME + "-outer-other-join",
                            STREAM_OPERATION_NAME + "-merge");

                }

                [Fact]
                public void shouldUseSpecifiedNameForMergeOperation()
                {
                    string topic1 = "topic-1";
                    string topic2 = "topic-2";

                    IKStream<string, string> source1 = builder.Stream(topic1);
                    IKStream<string, string> source2 = builder.Stream(topic2);
                    source1.merge(source2, Named.As("merge-processor"));
                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology, "KSTREAM-SOURCE-0000000000", "KSTREAM-SOURCE-0000000001", "merge-processor");
                }

                [Fact]
                public void shouldUseSpecifiedNameForProcessOperation()
                {
                    builder.Stream(STREAM_TOPIC)
                            .process(() => null, Named.As("test-processor"));

                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology, "KSTREAM-SOURCE-0000000000", "test-processor");
                }

                [Fact]
                public void shouldUseSpecifiedNameForPrintOperation()
                {
                    builder.Stream(STREAM_TOPIC).print(Printed.toSysOut().withName("print-processor"));
                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology, "KSTREAM-SOURCE-0000000000", "print-processor");
                }

                [Fact]
                // 
            public void shouldUseSpecifiedNameForFlatTransformValueOperation()
                {
                    builder.Stream(STREAM_TOPIC).flatTransformValues(() => (ValueTransformer)null, Named.As(STREAM_OPERATION_NAME));
                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology, "KSTREAM-SOURCE-0000000000", STREAM_OPERATION_NAME);
                }

                [Fact]
                // 
            public void shouldUseSpecifiedNameForFlatTransformValueWithKeyOperation()
                {
                    builder.Stream(STREAM_TOPIC).flatTransformValues(() => (ValueTransformerWithKey)null, Named.As(STREAM_OPERATION_NAME));
                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology, "KSTREAM-SOURCE-0000000000", STREAM_OPERATION_NAME);
                }

                [Fact]
                // 
                public void shouldUseSpecifiedNameForToStream()
                {
                    builder.Table(STREAM_TOPIC)
                            .toStream(Named.As("to-stream"));

                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology,
                            "KSTREAM-SOURCE-0000000001",
                            "KTABLE-SOURCE-0000000002",
                            "to-stream");
                }

                [Fact]
                // 
                public void shouldUseSpecifiedNameForToStreamWithMapper()
                {
                    builder.Table(STREAM_TOPIC)
                            .toStream(KeyValuePair::pair, Named.As("to-stream"));

                    builder.Build();
                    ProcessorTopology topology = builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(props)).Build();
                   .AssertSpecifiedNameForOperation(topology,
                            "KSTREAM-SOURCE-0000000001",
                            "KTABLE-SOURCE-0000000002",
                            "to-stream",
                            "KSTREAM-KEY-SELECT-0000000004");
                }

                private static void assertSpecifiedNameForOperation(ProcessorTopology topology, params string[] expected)
                {
                    List<ProcessorNode> processors = topology.processors();
                    Assert.Equal("Invalid number of expected processors", expected.Length, processors.Count);
                    for (int i = 0; i < expected.Length; i++)
                    {
                        Assert.Equal(expected[i], processors.get(i).name());
                    }
                }

                private static void assertSpecifiedNameForStateStore(List<IStateStore> stores, params string[] expected)
                {
                    Assert.Equal("Invalid number of expected state stores", expected.Length, stores.Count);
                    for (int i = 0; i < expected.Length; i++)
                    {
                        Assert.Equal(expected[i], stores.get(i).name());
                    }
                }
                */
    }
}
