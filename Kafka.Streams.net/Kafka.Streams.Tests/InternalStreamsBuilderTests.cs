using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class InternalStreamsBuilderTest
    {
        private const string APP_ID = "app-id";
        private readonly StreamsBuilder streamsBuilder = new StreamsBuilder();
        private InternalStreamsBuilder Builder => this.streamsBuilder.Context.InternalStreamsBuilder;

        private readonly string storePrefix = "prefix-";
        private readonly ConsumedInternal<string, string> consumed = new ConsumedInternal<string, string>();

        private MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> MaterializedTest =>
            new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(
                Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("test-store"),
                this.Builder,
                this.storePrefix);

        [Fact]
        public void TestNewName()
        {
            Assert.Equal("X-0000000000", this.Builder.NewProcessorName("X-"));
            Assert.Equal("Y-0000000001", this.Builder.NewProcessorName("Y-"));
            Assert.Equal("Z-0000000002", this.Builder.NewProcessorName("Z-"));

            InternalStreamsBuilder newBuilder = new StreamsBuilder().Context.InternalStreamsBuilder;

            Assert.Equal("X-0000000000", newBuilder.NewProcessorName("X-"));
            Assert.Equal("Y-0000000001", newBuilder.NewProcessorName("Y-"));
            Assert.Equal("Z-0000000002", newBuilder.NewProcessorName("Z-"));
        }

        [Fact]
        public void TestNewStoreName()
        {
            Assert.Equal("X-STATE-STORE-0000000000", this.Builder.NewStoreName("X-"));
            Assert.Equal("Y-STATE-STORE-0000000001", this.Builder.NewStoreName("Y-"));
            Assert.Equal("Z-STATE-STORE-0000000002", this.Builder.NewStoreName("Z-"));

            InternalStreamsBuilder newBuilder = new StreamsBuilder().Context.InternalStreamsBuilder;

            Assert.Equal("X-STATE-STORE-0000000000", newBuilder.NewStoreName("X-"));
            Assert.Equal("Y-STATE-STORE-0000000001", newBuilder.NewStoreName("Y-"));
            Assert.Equal("Z-STATE-STORE-0000000002", newBuilder.NewStoreName("Z-"));
        }

        [Fact]
        public void ShouldHaveCorrectSourceTopicsForTableFromMergedStream()
        {
            var topic1 = "topic-1";
            var topic2 = "topic-2";
            var topic3 = "topic-3";
            var source1 = this.Builder.Stream(new List<string> { topic1 }, this.consumed);
            var source2 = this.Builder.Stream(new List<string> { topic2 }, this.consumed);
            var source3 = this.Builder.Stream(new List<string> { topic3 }, this.consumed);

            var processedSource1 = source1
                .MapValues((value) => value)
                .Filter((k, v) => true);

            var processedSource2 = source2
                .Filter((k, v) => true);

            var merged = processedSource1
                .Merge(processedSource2)
                .Merge(source3);

            merged
                .GroupByKey()
                .Count(KStream.Materialized.As<string, long, IKeyValueStore<Bytes, byte[]>>("my-table"));

            this.Builder.BuildAndOptimizeTopology();

            var actual = this.Builder.InternalTopologyBuilder.StateStoreNameToSourceTopics();
            Assert.Equal(new List<string> { "topic-1", "topic-2", "topic-3" }, actual["my-table"]);
        }

        [Fact]
        public void ShouldNotMaterializeSourceKTableIfNotRequired()
        {
            var materializedInternal = new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(
                KStream.Materialized.With<string, string, IKeyValueStore<Bytes, byte[]>>(null, null), this.Builder, this.storePrefix);

            IKTable<string, string> table1 = this.Builder.Table("topic2", this.consumed, materializedInternal);

            this.Builder.BuildAndOptimizeTopology();
            ProcessorTopology topology = this.Builder.InternalTopologyBuilder
                .RewriteTopology(StreamsTestConfigs.GetStandardConfig(APP_ID))
                .Build(null);

            Assert.Empty(topology.StateStores);
            Assert.Empty(topology.StoreToChangelogTopic);
            Assert.Null(table1.QueryableStoreName);
        }

        [Fact]
        public void ShouldBuildGlobalTableWithNonQueryableStoreName()
        {
            var materializedInternal =
                 new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(KStream.Materialized.With<string, string, IKeyValueStore<Bytes, byte[]>>(null, null), this.Builder, this.storePrefix);

            IGlobalKTable<string, string> table1 = this.Builder.GlobalTable("topic2", this.consumed, materializedInternal);

            Assert.Null(table1.QueryableStoreName);
        }

        [Fact]
        public void ShouldBuildGlobalTableWithQueryaIbleStoreName()
        {
            var materializedInternal =
                 new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(KStream.Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("globalTable"), this.Builder, this.storePrefix);
            IGlobalKTable<string, string> table1 = this.Builder.GlobalTable("topic2", this.consumed, materializedInternal);

            Assert.Equal("globalTable", table1.QueryableStoreName);
        }

        [Fact]
        public void ShouldBuildSimpleGlobalTableTopology()
        {
            var materializedInternal = new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(
                KStream.Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("globalTable"),
                this.Builder,
                this.storePrefix);

            this.Builder.GlobalTable("table", this.consumed, materializedInternal);

            this.Builder.BuildAndOptimizeTopology();
            ProcessorTopology topology = this.Builder.InternalTopologyBuilder
                .RewriteTopology(new StreamsConfig(StreamsTestConfigs.GetStandardConfig(APP_ID)))
                .BuildGlobalStateTopology();

            List<IStateStore> stateStores = topology.globalStateStores;

            Assert.Single(stateStores);
            Assert.Equal("globalTable", stateStores[0].Name);
        }

        private void DoBuildGlobalTopologyWithAllGlobalTables()
        {
            ProcessorTopology topology = this.Builder.InternalTopologyBuilder
                .RewriteTopology(new StreamsConfig(StreamsTestConfigs.GetStandardConfig(APP_ID)))
                .BuildGlobalStateTopology();

            List<IStateStore> stateStores = topology.globalStateStores;
            List<string> sourceTopics = topology.SourceTopics;

            Assert.Equal(new List<string> { "table", "table2" }, sourceTopics);
            Assert.Equal(2, stateStores.Count);
        }

        [Fact]
        public void ShouldBuildGlobalTopologyWithAllGlobalTables()
        {
            var materializedInternal =
                 new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(KStream.Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("global1"), this.Builder, this.storePrefix);
            this.Builder.GlobalTable("table", this.consumed, materializedInternal);

            var materializedInternal2 =
                     new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(KStream.Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("global2"), this.Builder, this.storePrefix);
            this.Builder.GlobalTable("table2", this.consumed, materializedInternal2);

            this.Builder.BuildAndOptimizeTopology();
            this.DoBuildGlobalTopologyWithAllGlobalTables();
        }

        [Fact]
        public void ShouldAddGlobalTablesToEachGroup()
        {
            var one = "globalTable";
            var two = "globalTable2";

            var materializedInternal =
                 new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(KStream.Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>(one), this.Builder, this.storePrefix);
            IGlobalKTable<string, string> globalTable = this.Builder.GlobalTable("table", this.consumed, materializedInternal);

            var materializedInternal2 =
                 new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(KStream.Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>(two), this.Builder, this.storePrefix);
            IGlobalKTable<string, string> globalTable2 = this.Builder.GlobalTable("table2", this.consumed, materializedInternal2);

            var materializedInternalNotGlobal =
                 new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(KStream.Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("not-global"), this.Builder, this.storePrefix);
            this.Builder.Table("not-global", this.consumed, materializedInternalNotGlobal);

            KeyValueMapper<string, string, string> kvMapper = (key, value) => value;

            IKStream<string, string> stream = this.Builder.Stream(new[] { "t1" }, this.consumed);
            stream.LeftJoin(globalTable, kvMapper, MockValueJoiner.TOSTRING_JOINER());
            IKStream<string, string> stream2 = this.Builder.Stream(new[] { "t2" }, this.consumed);
            stream2.LeftJoin(globalTable2, kvMapper, MockValueJoiner.TOSTRING_JOINER());

            var nodeGroups = this.Builder.InternalTopologyBuilder.GetNodeGroups();
            foreach (var groupId in nodeGroups.Keys)
            {
                ProcessorTopology topology = this.Builder.InternalTopologyBuilder.Build(groupId);
                List<IStateStore> stateStores = topology.globalStateStores;
                var names = new HashSet<string>();
                foreach (var stateStore in stateStores)
                {
                    names.Add(stateStore.Name);
                }

                Assert.Equal(2, stateStores.Count);
                Assert.Contains(one, names);
                Assert.Contains(two, names);
            }
        }

        [Fact]
        public void ShouldMapStateStoresToCorrectSourceTopics()
        {
            IKStream<string, string> playEvents = this.Builder.Stream(new[] { "events" }, this.consumed);

            var materializedInternal = new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(
                Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("table-store"),
                    this.Builder,
                    this.storePrefix);

            IKTable<string, string> table = this.Builder.Table("table-topic", this.consumed, materializedInternal);

            var mapper = MockMapper.GetSelectValueKeyValueMapper<string, string>();
            IKStream<string, string> mapped = playEvents.Map<string, string>(mapper);
            var leftJoined = mapped.LeftJoin(table, MockValueJoiner.TOSTRING_JOINER());
            var groupedByKey = leftJoined.GroupByKey();

            var countMaterialized = Materialized.As<string, long, IKeyValueStore<Bytes, byte[]>>("count");
            var count = groupedByKey.Count(countMaterialized);

            this.Builder.BuildAndOptimizeTopology();
            this.Builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(StreamsTestConfigs.GetStandardConfig(APP_ID)));

            Assert.Equal(new[] { "table-topic" }, this.Builder.InternalTopologyBuilder.StateStoreNameToSourceTopics()["table-store"]);
            Assert.Equal(new[] { APP_ID + "-KSTREAM-MAP-0000000003-repartition" }, this.Builder.InternalTopologyBuilder.StateStoreNameToSourceTopics()["count"]);
        }

        [Fact]
        public void ShouldAddTopicToEarliestAutoOffsetResetList()
        {
            var topicName = "topic-1";
            var consumed = new ConsumedInternal<string, string>(Consumed.With<string, string>(AutoOffsetReset.Earliest));
            this.Builder.Stream(new[] { topicName }, consumed);
            this.Builder.BuildAndOptimizeTopology();

            Assert.Matches(this.Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topicName);
            Assert.DoesNotMatch(this.Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicName);
        }

        [Fact]
        public void ShouldAddTopicToLatestAutoOffsetResetList()
        {
            var topicName = "topic-1";

            var consumed = new ConsumedInternal<string, string>(Consumed.With<string, string>(AutoOffsetReset.Latest));
            this.Builder.Stream(new[] { topicName }, consumed);
            this.Builder.BuildAndOptimizeTopology();

            Assert.Matches(this.Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicName);
            Assert.DoesNotMatch(this.Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topicName);
        }

        [Fact]
        public void ShouldAddTableToEarliestAutoOffsetResetList()
        {
            var topicName = "topic-1";
            this.Builder.Table(topicName, new ConsumedInternal<string, string>(Consumed.With<string, string>(AutoOffsetReset.Earliest)), this.MaterializedTest);
            this.Builder.BuildAndOptimizeTopology();

            Assert.Matches(this.Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topicName);
            Assert.DoesNotMatch(this.Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicName);
        }

        [Fact]
        public void ShouldAddTableToLatestAutoOffsetResetList()
        {
            var topicName = "topic-1";
            this.Builder.Table(topicName, new ConsumedInternal<string, string>(Consumed.With<string, string>(AutoOffsetReset.Latest)), this.MaterializedTest);
            this.Builder.BuildAndOptimizeTopology();

            Assert.Matches(this.Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicName);
            Assert.DoesNotMatch(this.Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topicName);
        }

        [Fact]
        public void ShouldNotAddTableToOffsetResetLists()
        {
            var topicName = "topic-1";
            this.Builder.Table(topicName, this.consumed, this.MaterializedTest);
            Assert.DoesNotMatch(this.Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicName);
            Assert.DoesNotMatch(this.Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topicName);
        }

        [Fact]
        public void ShouldNotAddRegexTopicsToOffsetResetLists()
        {
            var topicPattern = new Regex("topic-\\d", RegexOptions.Compiled);
            var topic = "topic-5";

            this.Builder.Stream(topicPattern, this.consumed);

            Assert.DoesNotMatch(this.Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topic);
            Assert.DoesNotMatch(this.Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topic);
        }

        [Fact]
        public void ShouldAddRegexTopicToEarliestAutoOffsetResetList()
        {
            var topicPattern = new Regex("topic-\\d+", RegexOptions.Compiled);
            var topicTwo = "topic-500000";

            this.Builder.Stream(topicPattern, new ConsumedInternal<string, string>(Consumed.With<string, string>(AutoOffsetReset.Earliest)));
            this.Builder.BuildAndOptimizeTopology();

            Assert.Matches(this.Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topicTwo);
            Assert.DoesNotMatch(this.Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicTwo);
        }

        [Fact]
        public void ShouldAddRegexTopicToLatestAutoOffsetResetList()
        {
            var topicPattern = new Regex("topic-\\d+", RegexOptions.Compiled);
            var topicTwo = "topic-1000000";

            this.Builder.Stream(topicPattern, new ConsumedInternal<string, string>(Consumed.With<string, string>(AutoOffsetReset.Latest)));
            this.Builder.BuildAndOptimizeTopology();

            Assert.Matches(this.Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicTwo);
            Assert.DoesNotMatch(this.Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topicTwo);
        }

        [Fact]
        public void ShouldHaveNullTimestampExtractorWhenNoneSupplied()
        {
            this.Builder.Stream(new[] { "topic" }, this.consumed);
            this.Builder.BuildAndOptimizeTopology();
            this.Builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(StreamsTestConfigs.GetStandardConfig(APP_ID)));
            ProcessorTopology processorTopology = this.Builder.InternalTopologyBuilder.Build(null);
            Assert.Null(processorTopology.Source("topic").TimestampExtractor);
        }

        [Fact]
        public void ShouldUseProvidedTimestampExtractor()
        {
            var consumed = new ConsumedInternal<string, string>(Consumed.With<string, string>(new MockTimestampExtractor()));
            this.Builder.Stream(new[] { "topic" }, consumed);
            this.Builder.BuildAndOptimizeTopology();
            ProcessorTopology processorTopology = this.Builder.InternalTopologyBuilder
                .RewriteTopology(new StreamsConfig(StreamsTestConfigs.GetStandardConfig(APP_ID)))
                .Build(null);

            Assert.True(processorTopology.Source("topic").TimestampExtractor is MockTimestampExtractor);
        }

        [Fact]
        public void KtableShouldHaveNullTimestampExtractorWhenNoneSupplied()
        {
            this.Builder.Table("topic", this.consumed, this.MaterializedTest);
            this.Builder.BuildAndOptimizeTopology();
            ProcessorTopology processorTopology = this.Builder.InternalTopologyBuilder
                .RewriteTopology(new StreamsConfig(StreamsTestConfigs.GetStandardConfig(APP_ID)))
                .Build(null);

            Assert.Null(processorTopology.Source("topic").TimestampExtractor);
        }

        [Fact]
        public void KtableShouldUseProvidedTimestampExtractor()
        {
            var consumed = new ConsumedInternal<string, string>(
                Consumed.With<string, string>(new MockTimestampExtractor()));

            this.Builder.Table("topic", consumed, this.MaterializedTest);
            this.Builder.BuildAndOptimizeTopology();
            ProcessorTopology processorTopology = this.Builder.InternalTopologyBuilder
                .RewriteTopology(new StreamsConfig(StreamsTestConfigs.GetStandardConfig(APP_ID)))
                .Build(null);

            Assert.True(processorTopology.Source("topic").TimestampExtractor is MockTimestampExtractor);
        }
    }
}
