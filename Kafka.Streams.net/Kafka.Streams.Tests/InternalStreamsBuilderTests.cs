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
        private InternalStreamsBuilder Builder => streamsBuilder.InternalStreamsBuilder;

        private readonly string storePrefix = "prefix-";
        private readonly ConsumedInternal<string, string> consumed = new ConsumedInternal<string, string>();

        private MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> materialized =>
            new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(
                Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As("test-store"),
                Builder,
                storePrefix);

        [Fact]
        public void TestNewName()
        {
            Assert.Equal("X-0000000000", Builder.NewProcessorName("X-"));
            Assert.Equal("Y-0000000001", Builder.NewProcessorName("Y-"));
            Assert.Equal("Z-0000000002", Builder.NewProcessorName("Z-"));

            InternalStreamsBuilder newBuilder = new StreamsBuilder().InternalStreamsBuilder;

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

            InternalStreamsBuilder newBuilder = new StreamsBuilder().InternalStreamsBuilder;

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
                .mapValues(new ValueMapper<string, string>((value) => value))
                .filter((k, v) => true);

            var processedSource2 = source2
                .filter((k, v) => true);

            var merged = processedSource1
                .merge(processedSource2)
                .merge(source3);

            merged
                .GroupByKey()
                .Count(Materialized<string, long, IKeyValueStore<Bytes, byte[]>>
                .As("my-table"));

            Builder.BuildAndOptimizeTopology();

            var actual = Builder.InternalTopologyBuilder.StateStoreNameToSourceTopics();
            Assert.Equal(new List<string> { "topic-1", "topic-2", "topic-3" }, actual["my-table"]);
        }

        [Fact]
        public void ShouldNotMaterializeSourceKTableIfNotRequired()
        {
            var materializedInternal = new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(
                Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.With(null, null), Builder, storePrefix);

            IKTable<string, string> table1 = Builder.table("topic2", consumed, materializedInternal);

            Builder.BuildAndOptimizeTopology();
            ProcessorTopology topology = Builder.InternalTopologyBuilder
                .RewriteTopology(StreamsTestConfigs.GetStandardConfig(APP_ID))
                .Build(null);

            Assert.Empty(topology.StateStores);
            Assert.Empty(topology.StoreToChangelogTopic);
            Assert.Null(table1.queryableStoreName);
        }

        [Fact]
        public void ShouldBuildGlobalTableWithNonQueryableStoreName()
        {
            var materializedInternal =
                 new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.With(null, null), Builder, storePrefix);

            IGlobalKTable<string, string> table1 = Builder.globalTable("topic2", consumed, materializedInternal);

            Assert.Null(table1.QueryableStoreName);
        }

        [Fact]
        public void ShouldBuildGlobalTableWithQueryaIbleStoreName()
        {
            var materializedInternal =
                 new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As("globalTable"), Builder, storePrefix);
            IGlobalKTable<string, string> table1 = Builder.globalTable("topic2", consumed, materializedInternal);

            Assert.Equal("globalTable", table1.QueryableStoreName);
        }

        [Fact]
        public void ShouldBuildSimpleGlobalTableTopology()
        {
            var materializedInternal = new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(
                Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As("globalTable"),
                Builder,
                storePrefix);

            Builder.globalTable("table", consumed, materializedInternal);

            Builder.BuildAndOptimizeTopology();
            ProcessorTopology topology = Builder.InternalTopologyBuilder
                .RewriteTopology(new StreamsConfig(StreamsTestConfigs.GetStandardConfig(APP_ID)))
                .BuildGlobalStateTopology();

            List<IStateStore> stateStores = topology.globalStateStores;

            Assert.Single(stateStores);
            Assert.Equal("globalTable", stateStores[0].name);
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
            MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> materializedInternal =
                 new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As("global1"), Builder, storePrefix);
            Builder.globalTable("table", consumed, materializedInternal);

            MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>> materializedInternal2 =
                     new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As("global2"), Builder, storePrefix);
            Builder.globalTable("table2", consumed, materializedInternal2);

            Builder.BuildAndOptimizeTopology();
            DoBuildGlobalTopologyWithAllGlobalTables();
        }

        [Fact]
        public void ShouldAddGlobalTablesToEachGroup()
        {
            var one = "globalTable";
            var two = "globalTable2";

            var materializedInternal =
                 new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As(one), Builder, storePrefix);
            IGlobalKTable<string, string> globalTable = Builder.globalTable("table", consumed, materializedInternal);

            var materializedInternal2 =
                 new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As(two), Builder, storePrefix);
            IGlobalKTable<string, string> globalTable2 = Builder.globalTable("table2", consumed, materializedInternal2);

            var materializedInternalNotGlobal =
                 new MaterializedInternal<string, string, IKeyValueStore<Bytes, byte[]>>(Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.As("not-global"), Builder, storePrefix);
            Builder.table("not-global", consumed, materializedInternalNotGlobal);

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
                    names.Add(stateStore.name);
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
                Materialized<string, string, IKeyValueStore<Bytes, byte[]>>
                .As("table-store"), Builder, storePrefix);

            IKTable<string, string> table = Builder.table("table-topic", consumed, materializedInternal);

            var mapper = new MockMapper.SelectValueKeyValueMapper<string, string>();
            IKStream<string, string> mapped = playEvents.map(mapper);
            mapped.LeftJoin(table, MockValueJoiner.TOSTRING_JOINER<string, string>()).GroupByKey().Count(Materialized<string, long, IKeyValueStore<Bytes, byte[]>>.As("count"));
            Builder.BuildAndOptimizeTopology();
            Builder.InternalTopologyBuilder.RewriteTopology(new StreamsConfig(StreamsTestConfigs.GetStandardConfig(APP_ID)));

            Assert.Equal(new[] { "table-topic" }, Builder.InternalTopologyBuilder.StateStoreNameToSourceTopics()["table-store"]);
            Assert.Equal(new[] { APP_ID + "-KSTREAM-MAP-0000000003-repartition" }, Builder.InternalTopologyBuilder.StateStoreNameToSourceTopics()["count"]);
        }

        [Fact]
        public void ShouldAddTopicToEarliestAutoOffsetResetList()
        {
            var topicName = "topic-1";
            var consumed = new ConsumedInternal<string, string>(Consumed<string, string>.with(AutoOffsetReset.Earliest));
            Builder.Stream(new[] { topicName }, consumed);
            Builder.BuildAndOptimizeTopology();

            Assert.Matches(Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topicName);
            Assert.DoesNotMatch(Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicName);
        }

        [Fact]
        public void ShouldAddTopicToLatestAutoOffsetResetList()
        {
            var topicName = "topic-1";

            var consumed = new ConsumedInternal<string, string>(Consumed<string, string>.with(AutoOffsetReset.Latest));
            Builder.Stream(new[] { topicName }, consumed);
            Builder.BuildAndOptimizeTopology();

            Assert.Matches(Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicName);
            Assert.DoesNotMatch(Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topicName);
        }

        [Fact]
        public void ShouldAddTableToEarliestAutoOffsetResetList()
        {
            var topicName = "topic-1";
            Builder.table(topicName, new ConsumedInternal<string, string>(Consumed<string, string>.with(AutoOffsetReset.Earliest)), materialized);
            Builder.BuildAndOptimizeTopology();

            Assert.Matches(Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topicName);
            Assert.DoesNotMatch(Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicName);
        }

        [Fact]
        public void ShouldAddTableToLatestAutoOffsetResetList()
        {
            var topicName = "topic-1";
            Builder.table(topicName, new ConsumedInternal<string, string>(Consumed<string, string>.with(AutoOffsetReset.Latest)), materialized);
            Builder.BuildAndOptimizeTopology();

            Assert.Matches(Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicName);
            Assert.DoesNotMatch(Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topicName);
        }

        [Fact]
        public void ShouldNotAddTableToOffsetResetLists()
        {
            var topicName = "topic-1";
            Builder.table(topicName, consumed, materialized);
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

            Builder.Stream(topicPattern, new ConsumedInternal<string, string>(Consumed<string, string>.with(AutoOffsetReset.Earliest)));
            Builder.BuildAndOptimizeTopology();

            Assert.Matches(Builder.InternalTopologyBuilder.EarliestResetTopicsPattern(), topicTwo);
            Assert.DoesNotMatch(Builder.InternalTopologyBuilder.LatestResetTopicsPattern(), topicTwo);
        }

        [Fact]
        public void ShouldAddRegexTopicToLatestAutoOffsetResetList()
        {
            var topicPattern = new Regex("topic-\\d+", RegexOptions.Compiled);
            var topicTwo = "topic-1000000";

            Builder.Stream(topicPattern, new ConsumedInternal<string, string>(Consumed<string, string>.with(AutoOffsetReset.Latest)));
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
            var consumed = new ConsumedInternal<string, string>(Consumed<string, string>.with(new MockTimestampExtractor()));
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
            Builder.table("topic", consumed, materialized);
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
                Consumed<string, string>.with(new MockTimestampExtractor()));

            Builder.table("topic", consumed, materialized);
            Builder.BuildAndOptimizeTopology();
            ProcessorTopology processorTopology = Builder.InternalTopologyBuilder
                .RewriteTopology(new StreamsConfig(StreamsTestConfigs.GetStandardConfig(APP_ID)))
                .Build(null);

            Assert.True(processorTopology.Source("topic").TimestampExtractor is MockTimestampExtractor);
        }
    }
}
