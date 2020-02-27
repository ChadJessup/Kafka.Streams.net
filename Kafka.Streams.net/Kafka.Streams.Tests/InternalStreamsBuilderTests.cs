using Confluent.Kafka;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State.KeyValue;
using Kafka.Streams.Topologies;
using System.Text.RegularExpressions;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class InternalStreamsBuilderTest
    {
        private static readonly string APP_ID = "app-id";
        private readonly StreamsBuilder streamsBuilder = new StreamsBuilder();
        private InternalStreamsBuilder builder => streamsBuilder.InternalStreamsBuilder;

        private readonly string storePrefix = "prefix-";
        private readonly ConsumedInternal<string, string> consumed = new ConsumedInternal<string, string>();

        private MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> materialized =>
            new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(
                Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As("test-store"),
                builder,
                storePrefix);

        [Fact]
        public void TestNewName()
        {
            Assert.Equal("X-0000000000", builder.NewProcessorName("X-"));
            Assert.Equal("Y-0000000001", builder.NewProcessorName("Y-"));
            Assert.Equal("Z-0000000002", builder.NewProcessorName("Z-"));

            InternalStreamsBuilder newBuilder = new StreamsBuilder().InternalStreamsBuilder;

            Assert.Equal("X-0000000000", newBuilder.NewProcessorName("X-"));
            Assert.Equal("Y-0000000001", newBuilder.NewProcessorName("Y-"));
            Assert.Equal("Z-0000000002", newBuilder.NewProcessorName("Z-"));
        }

        [Fact]
        public void TestNewStoreName()
        {
            Assert.Equal("X-STATE-STORE-0000000000", builder.NewStoreName("X-"));
            Assert.Equal("Y-STATE-STORE-0000000001", builder.NewStoreName("Y-"));
            Assert.Equal("Z-STATE-STORE-0000000002", builder.NewStoreName("Z-"));

            InternalStreamsBuilder newBuilder = new StreamsBuilder().InternalStreamsBuilder;

            Assert.Equal("X-STATE-STORE-0000000000", newBuilder.NewStoreName("X-"));
            Assert.Equal("Y-STATE-STORE-0000000001", newBuilder.NewStoreName("Y-"));
            Assert.Equal("Z-STATE-STORE-0000000002", newBuilder.NewStoreName("Z-"));
        }

        //    [Fact]
        //    public void shouldHaveCorrectSourceTopicsForTableFromMergedStream()
        //    {
        //        string topic1 = "topic-1";
        //        string topic2 = "topic-2";
        //        string topic3 = "topic-3";
        //        KStream<string, string> source1 = builder.Stream(Collections.singleton(topic1), consumed);
        //        KStream<string, string> source2 = builder.Stream(Collections.singleton(topic2), consumed);
        //        KStream<string, string> source3 = builder.Stream(Collections.singleton(topic3), consumed);
        //        KStream<string, string> processedSource1 =
        //                source1.mapValues(new ValueMapper<string, string>()
        //                {
        //                    public string apply(string value)
        //        {
        //            return value;
        //        }
        //    }).filter(new Predicate<string, string>() {
        //                    public bool test(string key, string value)
        //    {
        //        return true;
        //    }
        //});
        //        KStream<string, string> processedSource2 = source2.filter(new Predicate<string, string>()
        //        {
        //            public bool test(string key, string value)
        //{
        //    return true;
        //}
        //        });

        //        KStream<string, string> merged = processedSource1.merge(processedSource2).merge(source3);
        //merged.groupByKey().count(Materialized.<string, long, IKeyValueStore<Bytes, byte[]>>as("my-table"));
        //        builder.BuildAndOptimizeTopology();
        //        Dictionary<string, List<string>> actual = builder.InternalTopologyBuilder.stateStoreNameToSourceTopics();
        //Assert.Equal(asList("topic-1", "topic-2", "topic-3"), actual.get("my-table"));
        //    }

        //[Fact]
        //public void ShouldNotMaterializeSourceKTableIfNotRequired()
        //{
        //    var materializedInternal = new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(
        //        Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.With(null, null), builder, storePrefix);
        //
        //    IKTable<string, string> table1 = builder.table("topic2", consumed, materializedInternal);
        //
        //    builder.BuildAndOptimizeTopology();
        //    ProcessorTopology topology = builder.InternalTopologyBuilder
        //        .RewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)))
        //        .build(null);
        //
        //    Assert.Empty(topology.StateStores);
        //    Assert.Empty(topology.StoreToChangelogTopic);
        //    Assert.Null(table1.QueryableStoreName);
        //}
        //
        //[Fact]
        //public void shouldBuildGlobalTableWithNonQueryableStoreName()
        //{
        //    var materializedInternal =
        //         new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.With(null, null), builder, storePrefix);
        //
        //    IGlobalKTable<string, string> table1 = builder.globalTable("topic2", consumed, materializedInternal);
        //
        //    Assert.Null(table1.QueryableStoreName);
        //}
        //
        //[Fact]
        //public void shouldBuildGlobalTableWithQueryaIbleStoreName()
        //{
        //    MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> materializedInternal =
        //         new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As("globalTable"), builder, storePrefix);
        //    IGlobalKTable<string, string> table1 = builder.globalTable("topic2", consumed, materializedInternal);
        //
        //    Assert.Equal("globalTable", table1.QueryableStoreName);
        //}
        //
        //[Fact]
        //public void shouldBuildSimpleGlobalTableTopology()
        //{
        //    MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> materializedInternal =
        //         new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As("globalTable"), builder, storePrefix);
        //    builder.globalTable("table",
        //                        consumed,
        //        materializedInternal);
        //
        //    builder.BuildAndOptimizeTopology();
        //    ProcessorTopology topology = builder.InternalTopologyBuilder
        //        .RewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)))
        //        .buildGlobalStateTopology();
        //    List<IStateStore> stateStores = topology.globalStateStores;
        //
        //    Assert.Single(stateStores);
        //    Assert.Equal("globalTable", stateStores[0].name);
        //}
        //
        //private void doBuildGlobalTopologyWithAllGlobalTables()
        //{
        //    ProcessorTopology topology = builder.InternalTopologyBuilder
        //        .RewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)))
        //        .buildGlobalStateTopology();
        //
        //    List<IStateStore> stateStores = topology.globalStateStores;
        //    List<string> sourceTopics = topology.sourceTopics();
        //
        //    Assert.Equal(Utils.mkSet("table", "table2"), sourceTopics);
        //    Assert.Equal(2, stateStores.Count);
        //}
        //
        //[Fact]
        //public void shouldBuildGlobalTopologyWithAllGlobalTables()
        //{
        //    {
        //        MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> materializedInternal =
        //             new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As("global1"), builder, storePrefix);
        //        builder.globalTable("table", consumed, materializedInternal);
        //    }
        //    {
        //        MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> materializedInternal =
        //             new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As("global2"), builder, storePrefix);
        //        builder.globalTable("table2", consumed, materializedInternal);
        //    }
        //
        //    builder.BuildAndOptimizeTopology();
        //    doBuildGlobalTopologyWithAllGlobalTables();
        //}
        //
        //[Fact]
        //public void shouldAddGlobalTablesToEachGroup()
        //{
        //    string one = "globalTable";
        //    string two = "globalTable2";
        //
        //    MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> materializedInternal =
        //         new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As(one), builder, storePrefix);
        //    IGlobalKTable<string, string> globalTable = builder.globalTable("table", consumed, materializedInternal);
        //
        //    MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> materializedInternal2 =
        //         new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As(two), builder, storePrefix);
        //    IGlobalKTable<string, string> globalTable2 = builder.globalTable("table2", consumed, materializedInternal2);
        //
        //    MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> materializedInternalNotGlobal =
        //         new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As("not-global"), builder, storePrefix);
        //    builder.table("not-global", consumed, materializedInternalNotGlobal);
        //
        //    var kvMapper = new KeyValueMapper<string, string, string>((key, value) => value);
        //
        //    KStream<string, string> stream = builder.Stream(Collections.singleton("t1"), consumed);
        //    stream.leftJoin(globalTable, kvMapper, MockValueJoiner.TOSTRING_JOINER);
        //    KStream<string, string> stream2 = builder.Stream(Collections.singleton("t2"), consumed);
        //    stream2.leftJoin(globalTable2, kvMapper, MockValueJoiner.TOSTRING_JOINER);
        //
        //    var nodeGroups = builder.InternalTopologyBuilder.GetNodeGroups();
        //    foreach (int groupId in nodeGroups.Keys)
        //    {
        //        ProcessorTopology topology = builder.InternalTopologyBuilder.build(groupId);
        //        List<IStateStore> stateStores = topology.globalStateStores;
        //        HashSet<string> names = new HashSet<string>();
        //        foreach (var stateStore in stateStores)
        //        {
        //            names.Add(stateStore.name);
        //        }
        //
        //        Assert.Equal(2, stateStores.Count);
        //        Assert.Contains(one, names);
        //        Assert.Contains(two, names);
        //    }
        //}
        //
        //[Fact]
        //public void shouldMapStateStoresToCorrectSourceTopics()
        //{
        //    IKStream<string, string> playEvents = builder.Stream(new[] { "events" }, consumed);
        //
        //    var materializedInternal =
        //         new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As("table-store"), builder, storePrefix);
        //    IKTable<string, string> table = builder.table("table-topic", consumed, materializedInternal);
        //
        //
        //    KStream<string, string> mapped = playEvents.map(MockMapper<string, string> selectValueKeyValueMapper());
        //    mapped.leftJoin(table, MockValueJoiner.TOSTRING_JOINER).groupByKey().count(Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As("count"));
        //    builder.BuildAndOptimizeTopology();
        //    builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)));
        //    Assert.Equal(new[] { "table-topic" }, builder.InternalTopologyBuilder.StateStoreNameToSourceTopics()["table-store"]);
        //    Assert.Equal(new[] { APP_ID + "-KSTREAM-MAP-0000000003-repartition" }, builder.InternalTopologyBuilder.StateStoreNameToSourceTopics()["count"]);
        //}

        [Fact]
        public void ShouldAddTopicToEarliestAutoOffsetResetList()
        {
            string topicName = "topic-1";
            var consumed = new ConsumedInternal<string, string>(Consumed<string, string>.with(AutoOffsetReset.Earliest));
            builder.Stream(new[] { topicName }, consumed);
            builder.BuildAndOptimizeTopology();

            Assert.Matches(builder.InternalTopologyBuilder.earliestResetTopicsPattern(), topicName);
            Assert.DoesNotMatch(builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicName);
        }

        [Fact]
        public void ShouldAddTopicToLatestAutoOffsetResetList()
        {
            string topicName = "topic-1";

            var consumed = new ConsumedInternal<string, string>(Consumed<string, string>.with(AutoOffsetReset.Latest));
            builder.Stream(new[] { topicName }, consumed);
            builder.BuildAndOptimizeTopology();

            Assert.Matches(builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicName);
            Assert.DoesNotMatch(builder.InternalTopologyBuilder.earliestResetTopicsPattern(), topicName);
        }

        [Fact]
        public void shouldAddTableToEarliestAutoOffsetResetList()
        {
            string topicName = "topic-1";
            builder.table(topicName, new ConsumedInternal<string, string>(Consumed<string, string>.with(AutoOffsetReset.Earliest)), materialized);
            builder.BuildAndOptimizeTopology();
            Assert.Matches(builder.InternalTopologyBuilder.earliestResetTopicsPattern(), topicName);
            Assert.DoesNotMatch(builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicName);
        }

        [Fact]
        public void shouldAddTableToLatestAutoOffsetResetList()
        {
            string topicName = "topic-1";
            // builder.table(topicName, new ConsumedInternal<string, string>(Consumed<string, string>.with(AutoOffsetReset.Latest)), materialized);
            builder.BuildAndOptimizeTopology();
            Assert.Matches(builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicName);
            Assert.DoesNotMatch(builder.InternalTopologyBuilder.earliestResetTopicsPattern(), topicName);
        }

        [Fact]
        public void shouldNotAddTableToOffsetResetLists()
        {
            string topicName = "topic-1";
            //builder.table(topicName, consumed, materialized);
            Assert.DoesNotMatch(builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicName);
            Assert.DoesNotMatch(builder.InternalTopologyBuilder.earliestResetTopicsPattern(), topicName);
        }

        [Fact]
        public void shouldNotAddRegexTopicsToOffsetResetLists()
        {
            Regex topicPattern = new Regex("topic-\\d", RegexOptions.Compiled);
            string topic = "topic-5";

            builder.Stream(topicPattern, consumed);

            Assert.DoesNotMatch(builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topic);
            Assert.DoesNotMatch(builder.InternalTopologyBuilder.earliestResetTopicsPattern(), topic);

        }

        [Fact]
        public void shouldAddRegexTopicToEarliestAutoOffsetResetList()
        {
            Regex topicPattern = new Regex("topic-\\d+", RegexOptions.Compiled);
            string topicTwo = "topic-500000";

            builder.Stream(topicPattern, new ConsumedInternal<string, string>(Consumed<string, string>.with(AutoOffsetReset.Earliest)));
            builder.BuildAndOptimizeTopology();

            Assert.Matches(builder.InternalTopologyBuilder.earliestResetTopicsPattern(), topicTwo);
            Assert.DoesNotMatch(builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicTwo);
        }

        [Fact]
        public void shouldAddRegexTopicToLatestAutoOffsetResetList()
        {
            Regex topicPattern = new Regex("topic-\\d+", RegexOptions.Compiled);
            string topicTwo = "topic-1000000";

            builder.Stream(topicPattern, new ConsumedInternal<string, string>(Consumed<string, string>.with(AutoOffsetReset.Latest)));
            builder.BuildAndOptimizeTopology();

            Assert.Matches(builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicTwo);
            Assert.DoesNotMatch(builder.InternalTopologyBuilder.earliestResetTopicsPattern(), topicTwo);
        }

        // [Fact]
        // public void shouldHaveNullTimestampExtractorWhenNoneSupplied()
        // {
        //     builder.Stream(new[] { "topic" }, consumed);
        //     builder.BuildAndOptimizeTopology();
        //     builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)));
        //     ProcessorTopology processorTopology = builder.InternalTopologyBuilder.build(null);
        //     Assert.Null(processorTopology.Source("topic").getTimestampExtractor());
        // }
        // 
        // [Fact]
        // public void shouldUseProvidedTimestampExtractor()
        // {
        //     var consumed = new ConsumedInternal<string, string>(Consumed<string, string>.with(new MockTimestampExtractor()));
        //     builder.Stream(new[] { "topic" }, consumed);
        //     builder.BuildAndOptimizeTopology();
        //     ProcessorTopology processorTopology = builder.InternalTopologyBuilder
        //         .RewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)))
        //         .build(null);
        //     Assert.True(processorTopology.Source("topic").getTimestampExtractor() is MockTimestampExtractor);
        // }
        // 
        // [Fact]
        // public void ktableShouldHaveNullTimestampExtractorWhenNoneSupplied()
        // {
        //     builder.table("topic", consumed, materialized);
        //     builder.BuildAndOptimizeTopology();
        //     ProcessorTopology processorTopology = builder.InternalTopologyBuilder
        //         .RewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)))
        //         .build(null);
        // 
        //     Assert.Null(processorTopology.Source("topic").getTimestampExtractor());
        // }
        // 
        // [Fact]
        // public void ktableShouldUseProvidedTimestampExtractor()
        // {
        //     var consumed = new ConsumedInternal<string, string>(
        //         Consumed<string, string>.with(new MockTimestampExtractor()));
        // 
        //     builder.table("topic", consumed, materialized);
        //     builder.BuildAndOptimizeTopology();
        //     ProcessorTopology processorTopology = builder.InternalTopologyBuilder
        //         .RewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)))
        //         .build(null);
        //     Assert.True(processorTopology.Source("topic").getTimestampExtractor() is MockTimestampExtractor);
        // }
    }
}
