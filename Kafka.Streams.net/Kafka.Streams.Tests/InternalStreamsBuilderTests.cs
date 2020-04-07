using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.KStream.Mappers;
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
        private InternalStreamsBuilder Builder => streamsBuilder.Context.InternalStreamsBuilder;

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
            Assert.Equal("X-0000000000", Builder.NewProcessorName("X-"));
            Assert.Equal("Y-0000000001", Builder.NewProcessorName("Y-"));
            Assert.Equal("Z-0000000002", Builder.NewProcessorName("Z-"));

            InternalStreamsBuilder newBuilder = new StreamsBuilder().Context.InternalStreamsBuilder;

            Assert.Equal("X-0000000000", newBuilder.NewProcessorName("X-"));
            Assert.Equal("Y-0000000001", newBuilder.NewProcessorName("Y-"));
            Assert.Equal("Z-0000000002", newBuilder.NewProcessorName("Z-"));
        }

        [Fact]
        public void TestNewStoreName()
        {
            Assert.Equal("X-STATE-STORE-0000000000", Builder.NewStoreName("X-"));
            Assert.Equal("Y-STATE-STORE-0000000001", Builder.NewStoreName("Y-"));
            Assert.Equal("Z-STATE-STORE-0000000002", Builder.NewStoreName("Z-"));

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
            var source1 = Builder.Stream(new List<string> { topic1 }, consumed);
            var source2 = Builder.Stream(new List<string> { topic2 }, consumed);
            var source3 = Builder.Stream(new List<string> { topic3 }, consumed);

            var processedSource1 = source1
                .MapValues(new ValueMapper<string, string>((value) => value))
                .Filter((k, v) => true);

            var processedSource2 = source2
                .Filter((k, v) => true);

            var merged = processedSource1
                .Merge(processedSource2)
                .Merge(source3);

            merged
                .GroupByKey()
                .Count(KStream.Materialized.As<string, long, IKeyValueStore<Bytes, byte[]>>("my-table"));

            Builder.BuildAndOptimizeTopology();

            var actual = Builder.InternalTopologyBuilder.StateStoreNameToSourceTopics();
            Assert.Equal(new List<string> { "topic-1", "topic-2", "topic-3" }, actual["my-table"]);
        }

        [Fact]
        public void ShouldNotMaterializeSourceKTableIfNotRequired()
        {
            var materializedInternal = new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(
                KStream.Materialized.With<string, string, IKeyValueStore<Bytes, byte[]>>(null, null), this.Builder, this.storePrefix);

            IKTable<string, string> table1 = Builder.Table("topic2", consumed, materializedInternal);

            Builder.BuildAndOptimizeTopology();
            ProcessorTopology topology = Builder.InternalTopologyBuilder
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

            IGlobalKTable<string, string> table1 = Builder.GlobalTable("topic2", consumed, materializedInternal);

            Assert.Null(table1.QueryableStoreName);
        }

        [Fact]
        public void ShouldBuildGlobalTableWithQueryaIbleStoreName()
        {
            var materializedInternal =
                 new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(KStream.Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("globalTable"), this.Builder, this.storePrefix);
            IGlobalKTable<string, string> table1 = Builder.GlobalTable("topic2", consumed, materializedInternal);

            Assert.Equal("globalTable", table1.QueryableStoreName);
        }

        [Fact]
        public void ShouldBuildSimpleGlobalTableTopology()
        {
            var materializedInternal = new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(
                KStream.Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("globalTable"),
                this.Builder,
                this.storePrefix);

            Builder.GlobalTable("table", consumed, materializedInternal);

            Builder.BuildAndOptimizeTopology();
            ProcessorTopology topology = Builder.InternalTopologyBuilder
                .RewriteTopology(new StreamsConfig(StreamsTestConfigs.GetStandardConfig(APP_ID)))
                .BuildGlobalStateTopology();

            List<IStateStore> stateStores = topology.globalStateStores;

            Assert.Single(stateStores);
            Assert.Equal("globalTable", stateStores[0].Name);
        }

        private void DoBuildGlobalTopologyWithAllGlobalTables()
        {
            ProcessorTopology topology = Builder.InternalTopologyBuilder
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
            Builder.GlobalTable("table", consumed, materializedInternal);

            var materializedInternal2 =
                     new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(KStream.Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("global2"), this.Builder, this.storePrefix);
            Builder.GlobalTable("table2", consumed, materializedInternal2);

            Builder.BuildAndOptimizeTopology();
            DoBuildGlobalTopologyWithAllGlobalTables();
        }

        [Fact]
        public void ShouldAddGlobalTablesToEachGroup()
        {
            var one = "globalTable";
            var two = "globalTable2";

            var materializedInternal =
                 new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(KStream.Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>(one), this.Builder, this.storePrefix);
            IGlobalKTable<string, string> globalTable = Builder.GlobalTable("table", consumed, materializedInternal);

            var materializedInternal2 =
                 new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(KStream.Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>(two), this.Builder, this.storePrefix);
            IGlobalKTable<string, string> globalTable2 = Builder.GlobalTable("table2", consumed, materializedInternal2);

            var materializedInternalNotGlobal =
                 new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(KStream.Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("not-global"), this.Builder, this.storePrefix);
            Builder.Table("not-global", consumed, materializedInternalNotGlobal);

            var kvMapper = new KeyValueMapper<string, string, string>((key, value) => value);

            IKStream<string, string> stream = Builder.Stream(new[] { "t1" }, consumed);
            stream.LeftJoin(globalTable, kvMapper, MockValueJoiner.TOSTRING_JOINER<string, string>());
            IKStream<string, string> stream2 = Builder.Stream(new[] { "t2" }, consumed);
            stream2.LeftJoin(globalTable2, kvMapper, MockValueJoiner.TOSTRING_JOINER<string, string>());

            var nodeGroups = Builder.InternalTopologyBuilder.GetNodeGroups();
            foreach (var groupId in nodeGroups.Keys)
            {
                ProcessorTopology topology = Builder.InternalTopologyBuilder.Build(groupId);
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
            IKStream<string, string> playEvents = Builder.Stream(new[] { "events" }, consumed);

            var materializedInternal = new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(
                Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("table-store"),
                    this.Builder,
                    this.storePrefix);

            IKTable<string, string> table = Builder.Table("table-topic", consumed, materializedInternal);

            var mapper = new MockMapper.SelectValueKeyValueMapper<string, string>();
            IKStream<string, string> mapped = playEvents.Map(mapper);
            var leftJoined = mapped.LeftJoin(table, MockValueJoiner.TOSTRING_JOINER<string, string>());
            var groupedByKey = leftJoined.GroupByKey();

            var countMaterialized = Materialized.As<string, long, IKeyValueStore<Bytes, byte[]>>("count");
            var count = groupedByKey.Count(countMaterialized);

            Builder.BuildAndOptimizeTopology();
            Builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(StreamsTestConfigs.GetStandardConfig(APP_ID)));

            Assert.Equal(new[] { "table-topic" }, Builder.InternalTopologyBuilder.StateStoreNameToSourceTopics()["table-store"]);
            Assert.Equal(new[] { APP_ID + "-KSTREAM-MAP-0000000003-repartition" }, Builder.InternalTopologyBuilder.StateStoreNameToSourceTopics()["count"]);
        }

        [Fact]
        public void ShouldAddTopicToEarliestAutoOffsetResetList()
        {
            var topicName = "topic-1";
            var consumed = new ConsumedInternal<string, string>(Consumed.With<string, string>(AutoOffsetReset.Earliest));
            Builder.Stream(new[] { topicName }, consumed);
            Builder.BuildAndOptimizeTopology();

            Assert.Matches(Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topicName);
            Assert.DoesNotMatch(Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicName);
        }

        [Fact]
        public void ShouldAddTopicToLatestAutoOffsetResetList()
        {
            var topicName = "topic-1";

            var consumed = new ConsumedInternal<string, string>(Consumed.With<string, string>(AutoOffsetReset.Latest));
            Builder.Stream(new[] { topicName }, consumed);
            Builder.BuildAndOptimizeTopology();

            Assert.Matches(Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicName);
            Assert.DoesNotMatch(Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topicName);
        }

        [Fact]
        public void ShouldAddTableToEarliestAutoOffsetResetList()
        {
            var topicName = "topic-1";
            Builder.Table(topicName, new ConsumedInternal<string, string>(Consumed.With<string, string>(AutoOffsetReset.Earliest)), MaterializedTest);
            Builder.BuildAndOptimizeTopology();

            Assert.Matches(Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topicName);
            Assert.DoesNotMatch(Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicName);
        }

        [Fact]
        public void ShouldAddTableToLatestAutoOffsetResetList()
        {
            var topicName = "topic-1";
            Builder.Table(topicName, new ConsumedInternal<string, string>(Consumed.With<string, string>(AutoOffsetReset.Latest)), MaterializedTest);
            Builder.BuildAndOptimizeTopology();

            Assert.Matches(Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicName);
            Assert.DoesNotMatch(Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topicName);
        }

        [Fact]
        public void ShouldNotAddTableToOffsetResetLists()
        {
            var topicName = "topic-1";
            Builder.Table(topicName, consumed, MaterializedTest);
            Assert.DoesNotMatch(Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicName);
            Assert.DoesNotMatch(Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topicName);
        }

        [Fact]
        public void ShouldNotAddRegexTopicsToOffsetResetLists()
        {
            var topicPattern = new Regex("topic-\\d", RegexOptions.Compiled);
            var topic = "topic-5";

            Builder.Stream(topicPattern, consumed);

            Assert.DoesNotMatch(Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topic);
            Assert.DoesNotMatch(Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topic);
        }

        [Fact]
        public void ShouldAddRegexTopicToEarliestAutoOffsetResetList()
        {
            var topicPattern = new Regex("topic-\\d+", RegexOptions.Compiled);
            var topicTwo = "topic-500000";

            Builder.Stream(topicPattern, new ConsumedInternal<string, string>(Consumed.With<string, string>(AutoOffsetReset.Earliest)));
            Builder.BuildAndOptimizeTopology();

            Assert.Matches(Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topicTwo);
            Assert.DoesNotMatch(Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicTwo);
        }

        [Fact]
        public void ShouldAddRegexTopicToLatestAutoOffsetResetList()
        {
            var topicPattern = new Regex("topic-\\d+", RegexOptions.Compiled);
            var topicTwo = "topic-1000000";

            Builder.Stream(topicPattern, new ConsumedInternal<string, string>(Consumed.With<string, string>(AutoOffsetReset.Latest)));
            Builder.BuildAndOptimizeTopology();

            Assert.Matches(Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicTwo);
            Assert.DoesNotMatch(Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topicTwo);
        }

        [Fact]
        public void ShouldHaveNullTimestampExtractorWhenNoneSupplied()
        {
            Builder.Stream(new[] { "topic" }, consumed);
            Builder.BuildAndOptimizeTopology();
            Builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(StreamsTestConfigs.GetStandardConfig(APP_ID)));
            ProcessorTopology processorTopology = Builder.InternalTopologyBuilder.Build(null);
            Assert.Null(processorTopology.Source("topic").TimestampExtractor);
        }

        [Fact]
        public void ShouldUseProvidedTimestampExtractor()
        {
            var consumed = new ConsumedInternal<string, string>(Consumed.With<string, string>(new MockTimestampExtractor()));
            Builder.Stream(new[] { "topic" }, consumed);
            Builder.BuildAndOptimizeTopology();
            ProcessorTopology processorTopology = Builder.InternalTopologyBuilder
                .RewriteTopology(new StreamsConfig(StreamsTestConfigs.GetStandardConfig(APP_ID)))
                .Build(null);

            Assert.True(processorTopology.Source("topic").TimestampExtractor is MockTimestampExtractor);
        }

        [Fact]
        public void KtableShouldHaveNullTimestampExtractorWhenNoneSupplied()
        {
            Builder.Table("topic", consumed, MaterializedTest);
            Builder.BuildAndOptimizeTopology();
            ProcessorTopology processorTopology = Builder.InternalTopologyBuilder
                .RewriteTopology(new StreamsConfig(StreamsTestConfigs.GetStandardConfig(APP_ID)))
                .Build(null);

            Assert.Null(processorTopology.Source("topic").TimestampExtractor);
        }

        [Fact]
        public void KtableShouldUseProvidedTimestampExtractor()
        {
            var consumed = new ConsumedInternal<string, string>(
                Consumed.With<string, string>(new MockTimestampExtractor()));

            Builder.Table("topic", consumed, MaterializedTest);
            Builder.BuildAndOptimizeTopology();
            ProcessorTopology processorTopology = Builder.InternalTopologyBuilder
                .RewriteTopology(new StreamsConfig(StreamsTestConfigs.GetStandardConfig(APP_ID)))
                .Build(null);

            Assert.True(processorTopology.Source("topic").TimestampExtractor is MockTimestampExtractor);
        }
    }
}
