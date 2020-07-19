#pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
#pragma warning disable CS8625 // Cannot convert null literal to non-nullable reference type.

using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.Temporary;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;
using Kafka.Streams.Topologies;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class InternalTopologyBuilderTest
    {
        private readonly ISerde<string> stringSerde = Serdes.String();
        private InternalTopologyBuilder Builder => this.streamsBuilder.Context.InternalTopologyBuilder;
        private readonly IStoreBuilder<IKeyValueStore<int, byte[]>> storeBuilder;
        private readonly StreamsBuilder streamsBuilder;

        public InternalTopologyBuilderTest()
        {
            this.streamsBuilder = TestUtils.GetStreamsBuilder();
            this.storeBuilder = new MockKeyValueStoreBuilder(this.streamsBuilder.Context, "store", false);
        }

        [Fact]
        public void ShouldAddSourceWithOffsetReset()
        {
            string earliestTopic = "earliestTopic";
            string latestTopic = "latestTopic";

            Builder.AddSource<byte[], byte[]>(AutoOffsetReset.Earliest, "source", null, null, null, earliestTopic);
            Builder.AddSource<byte[], byte[]>(AutoOffsetReset.Latest, "source2", null, null, null, latestTopic);

            Assert.Matches(Builder.EarliestResetTopicsPattern(), earliestTopic);
            Assert.Matches(Builder.LatestResetTopicsPattern(), latestTopic);
        }

        [Fact]
        public void ShouldAddSourcePatternWithOffsetReset()
        {
            string earliestTopicPattern = "earliest.*Topic";
            string latestTopicPattern = "latest.*Topic";

            Builder.AddSource<byte[], byte[]>(AutoOffsetReset.Earliest, "source", null, null, null, new Regex(earliestTopicPattern, RegexOptions.Compiled));
            Builder.AddSource<byte[], byte[]>(AutoOffsetReset.Latest, "source2", null, null, null, new Regex(latestTopicPattern, RegexOptions.Compiled));

            Assert.Matches(Builder.EarliestResetTopicsPattern(), "earliestTestTopic");
            Assert.Matches(Builder.LatestResetTopicsPattern(), "latestTestTopic");
        }

        [Fact]
        public void ShouldAddSourceWithoutOffsetReset()
        {
            var expectedPattern = new Regex("test-topic", RegexOptions.Compiled);

            Builder.AddSource(null, "source", null, stringSerde.Deserializer, stringSerde.Deserializer, "test-topic");

            Assert.Equal(expectedPattern.ToString(), Builder.SourceTopicPattern().ToString());
            Assert.Equal("^$", Builder.EarliestResetTopicsPattern().ToString());
            Assert.Equal("^$", Builder.LatestResetTopicsPattern().ToString());
        }

        [Fact]
        public void ShouldAddPatternSourceWithoutOffsetReset()
        {
            var expectedPattern = new Regex("test-.*", RegexOptions.Compiled);

            var regex = new Regex("test-.*", RegexOptions.Compiled);

            Builder.AddSource(
                null,
                "source",
                null,
                stringSerde.Deserializer,
                stringSerde.Deserializer,
                regex);

            Assert.Equal(expectedPattern.ToString(), Builder.SourceTopicPattern().ToString());
            Assert.Equal("^$", Builder.EarliestResetTopicsPattern().ToString());
            Assert.Equal("^$", Builder.LatestResetTopicsPattern().ToString());
        }

        [Fact]
        public void ShouldNotAllowOffsetResetSourceWithoutTopics()
        {
            Assert.Throws<ArgumentNullException>(() => Builder.AddSource(
                AutoOffsetReset.Earliest,
                "source",
                null,
                stringSerde.Deserializer,
                stringSerde.Deserializer,
                (string[])null));
        }

        [Fact]
        public void ShouldNotAllowOffsetResetSourceWithDuplicateSourceName()
        {
            Builder.AddSource(AutoOffsetReset.Earliest, "source", null, stringSerde.Deserializer, stringSerde.Deserializer, "topic-1");
            Assert.Throws<TopologyException>(() => Builder.AddSource(AutoOffsetReset.Latest, "source", null, stringSerde.Deserializer, stringSerde.Deserializer, "topic-2"));
        }

        [Fact]
        public void TestAddSourceWithSameName()
        {
            Builder.AddSource<string, string>(null, "source", null, null, null, "topic-1");
            Assert.Throws<TopologyException>(() => Builder.AddSource<string, string>(null, "source", null, null, null, "topic-2"));
        }

        [Fact]
        public void TestAddSourceWithSameTopic()
        {
            Builder.AddSource<string, string>(null, "source", null, null, null, "topic-1");
            Assert.Throws<TopologyException>(() => Builder.AddSource<string, string>(null, "source-2", null, null, null, "topic-1"));
        }

        [Fact]
        public void TestAddProcessorWithSameName()
        {
            Builder.AddSource<string, string>(null, "source", null, null, null, "topic-1");
            Builder.AddProcessor<byte[], byte[]>("processor", new MockProcessorSupplier<byte[], byte[]>(), "source");

            Assert.Throws<TopologyException>(() => Builder.AddProcessor<byte[], byte[]>("processor", new MockProcessorSupplier<byte[], byte[]>(), "source"));
        }

        [Fact]
        public void TestAddProcessorWithWrongParent()
        {
            Assert.Throws<TopologyException>(() => Builder.AddProcessor<byte[], byte[]>("processor", new MockProcessorSupplier<byte[], byte[]>(), "source"));
        }

        [Fact]
        public void TestAddProcessorWithSelfParent()
        {
            Assert.Throws<TopologyException>(() => Builder.AddProcessor<byte[], byte[]>("processor", new MockProcessorSupplier<byte[], byte[]>(), "processor"));
        }

        [Fact]
        public void TestAddProcessorWithEmptyParents()
        {
            Assert.Throws<TopologyException>(() => Builder.AddProcessor<byte[], byte[]>("processor", new MockProcessorSupplier<byte[], byte[]>()));
        }

        [Fact]
        public void TestAddProcessorWithNullParents()
        {
            Assert.Throws<ArgumentNullException>(() => Builder.AddProcessor<byte[], byte[]>("processor", new MockProcessorSupplier<byte[], byte[]>(), (string)null));
        }

        [Fact]
        public void TestAddSinkWithSameName()
        {
            Builder.AddSource<string, string>(null, "source", null, null, null, "topic-1");
            Builder.AddSink<string, string>("sink", "topic-2", null, null, null, "source");

            Assert.Throws<TopologyException>(() => Builder.AddSink<string, string>("sink", "topic-3", null, null, null, "source"));
        }

        [Fact]
        public void TestAddSinkWithWrongParent()
        {
            Assert.Throws<TopologyException>(() => Builder.AddSink<string, string>("sink", "topic-2", null, null, null, "source"));
        }

        [Fact]
        public void TestAddSinkWithSelfParent()
        {
            Assert.Throws<TopologyException>(() => Builder.AddSink<string, string>("sink", "topic-2", null, null, null, "sink"));
        }


        [Fact]
        public void TestAddSinkWithEmptyParents()
        {
            Assert.Throws<TopologyException>(() => Builder.AddSink<string, string>("sink", "topic", null, null, null));
        }

        [Fact]
        public void TestAddSinkWithNullParents()
        {
            Assert.Throws<ArgumentNullException>(() => Builder.AddSink<string, string>("sink", "topic", null, null, null, (string)null));
        }

        [Fact]
        public void TestAddSinkConnectedWithParent()
        {
            Builder.AddSource<string, string>(null, "source", null, null, null, "source-topic");
            Builder.AddSink<string, string>("sink", "dest-topic", null, null, null, "source");

            Dictionary<int, HashSet<string>> nodeGroups = Builder.GetNodeGroups();
            HashSet<string> nodeGroup = nodeGroups[0];

            Assert.Contains("sink", nodeGroup);
            Assert.Contains("source", nodeGroup);
        }

        [Fact]
        public void TestAddSinkConnectedWithMultipleParent()
        {
            Builder.AddSource<string, string>(null, "source", null, null, null, "source-topic");
            Builder.AddSource<string, string>(null, "sourceII", null, null, null, "source-topicII");
            Builder.AddSink<string, string>("sink", "dest-topic", null, null, null, "source", "sourceII");

            Dictionary<int, HashSet<string>> nodeGroups = Builder.GetNodeGroups();
            HashSet<string> nodeGroup = nodeGroups[0];

            Assert.Contains("sink", nodeGroup);
            Assert.Contains("source", nodeGroup);
            Assert.Contains("sourceII", nodeGroup);
        }

        [Fact]
        public void TestPatternSourceTopic()
        {
            var expectedPattern = new Regex("topic-\\d", RegexOptions.Compiled);
            Builder.AddSource<string, string>(null, "source-1", null, null, null, expectedPattern);
            Assert.Equal(expectedPattern.ToString(), Builder.SourceTopicPattern().ToString());
        }

        [Fact]
        public void TestAddMoreThanOnePatternSourceNode()
        {
            var expectedPattern = new Regex("topics[A-Z]|.*-\\d", RegexOptions.Compiled);
            Builder.AddSource<string, string>(null, "source-1", null, null, null, new Regex("topics[A-Z]", RegexOptions.Compiled));
            Builder.AddSource<string, string>(null, "source-2", null, null, null, new Regex(".*-\\d", RegexOptions.Compiled));
            Assert.Equal(expectedPattern.ToString(), Builder.SourceTopicPattern().ToString());
        }

        [Fact]
        public void TestSubscribeTopicNameAndPattern()
        {
            var expectedPattern = new Regex("topic-bar|topic-foo|.*-\\d", RegexOptions.Compiled);
            Builder.AddSource<string, string>(null, "source-1", null, null, null, "topic-foo", "topic-bar");
            Builder.AddSource<string, string>(null, "source-2", null, null, null, new Regex(".*-\\d", RegexOptions.Compiled));
            Assert.Equal(expectedPattern.ToString(), Builder.SourceTopicPattern().ToString());
        }

        [Fact]
        public void TestPatternMatchesAlreadyProvidedTopicSource()
        {
            Builder.AddSource<string, string>(null, "source-1", null, null, null, "foo");

            Assert.Throws<TopologyException>(() => Builder.AddSource<string, string>(null, "source-2", null, null, null, new Regex("f.*", RegexOptions.Compiled)));
        }

        [Fact]
        public void TestNamedTopicMatchesAlreadyProvidedPattern()
        {
            Builder.AddSource<string, string>(null, "source-1", null, null, null, new Regex("f.*", RegexOptions.Compiled));
            Assert.Throws<TopologyException>(() => Builder.AddSource<string, string>(null, "source-2", null, null, null, "foo"));
        }

        [Fact]// (expected = TopologyException)
        public void TestAddStateStoreWithNonExistingProcessor()
        {
            Assert.Throws<TopologyException>(() => Builder.AddStateStore<string, string, IKeyValueStore<int, byte[]>>(storeBuilder, "no-such-processor"));
        }

        [Fact]
        public void TestAddStateStoreWithSource()
        {
            Builder.AddSource<string, string>(null, "source-1", null, null, null, "topic-1");
            Assert.Throws<TopologyException>(() => Builder.AddStateStore<string, string, IKeyValueStore<int, byte[]>>(storeBuilder, "source-1"));
        }

        [Fact]
        public void TestAddStateStoreWithSink()
        {
            Builder.AddSource<string, string>(null, "source-1", null, null, null, "topic-1");
            Builder.AddSink<string, string>("sink-1", "topic-1", null, null, null, "source-1");
            Assert.Throws<TopologyException>(() => Builder.AddStateStore<string, string, IKeyValueStore<int, byte[]>>(storeBuilder, "sink-1"));
        }

        [Fact]
        public void TestAddStateStoreWithDuplicates()
        {
            Builder.AddStateStore<string, string, IKeyValueStore<int, byte[]>>(storeBuilder);
            Assert.Throws<TopologyException>(() => Builder.AddStateStore<string, string, IKeyValueStore<int, byte[]>>(storeBuilder));
        }

        [Fact]
        public void TestAddStateStore()
        {
            Builder.AddStateStore<string, string, IKeyValueStore<int, byte[]>>(storeBuilder);
            Builder.SetApplicationId("X");
            Builder.AddSource<string, string>(null, "source-1", null, null, null, "topic-1");
            Builder.AddProcessor<string, string>("processor-1", new MockProcessorSupplier<byte[], byte[]>(), "source-1");

            Assert.Empty(Builder.Build(null).StateStores);

            Builder.ConnectProcessorAndStateStores("processor-1", storeBuilder.Name);

            List<IStateStore> suppliers = Builder.Build(null).StateStores;
            Assert.Single(suppliers);
            Assert.Equal(storeBuilder.Name, suppliers[0].Name);
        }

        [Fact]
        public void TestTopicGroups()
        {
            Builder.SetApplicationId("X");
            Builder.AddInternalTopic("topic-1x");
            Builder.AddSource<string, string>(null, "source-1", null, null, null, new string[] { "topic-1", "topic-1x" });
            Builder.AddSource<string, string>(null, "source-2", null, null, null, "topic-2");
            Builder.AddSource<string, string>(null, "source-3", null, null, null, "topic-3");
            Builder.AddSource<string, string>(null, "source-4", null, null, null, "topic-4");
            Builder.AddSource<string, string>(null, "source-5", null, null, null, "topic-5");

            Builder.AddProcessor<byte[], byte[]>("processor-1", new MockProcessorSupplier<byte[], byte[]>(), "source-1");

            Builder.AddProcessor<string, string>("processor-2", new MockProcessorSupplier<byte[], byte[]>(), "source-2", "processor-1");
            Builder.CopartitionSources(new HashSet<string> { "source-1", "source-2" });

            Builder.AddProcessor<string, string>("processor-3", new MockProcessorSupplier<byte[], byte[]>(), "source-3", "source-4");

            Dictionary<int, TopicsInfo> topicGroups = Builder.TopicGroups();

            Dictionary<int, TopicsInfo> expectedTopicGroups = new Dictionary<int, TopicsInfo>();
            expectedTopicGroups.Put(0, new TopicsInfo(new HashSet<string>(), new HashSet<string> { "topic-1", "X-topic-1x", "topic-2" }, new Dictionary<string, InternalTopicConfig>(), new Dictionary<string, InternalTopicConfig>()));
            expectedTopicGroups.Put(1, new TopicsInfo(new HashSet<string>(), new HashSet<string> { "topic-3", "topic-4" }, new Dictionary<string, InternalTopicConfig>(), new Dictionary<string, InternalTopicConfig>()));
            expectedTopicGroups.Put(2, new TopicsInfo(new HashSet<string>(), new HashSet<string> { "topic-5" }, new Dictionary<string, InternalTopicConfig>(), new Dictionary<string, InternalTopicConfig>()));

            Assert.Equal(3, topicGroups.Count);
            Assert.Equal(expectedTopicGroups, topicGroups);

            var CopartitionGroups = Builder.CopartitionGroups();

            Assert.Equal(new List<HashSet<string>> { new HashSet<string> { "topic-1", "X-topic-1x", "topic-2" } }, CopartitionGroups);
        }

        //[Fact]
        //public void TestTopicGroupsByStateStore()
        //{
        //    builder.SetApplicationId("X");
        //    builder.AddSource<string, string>(null, "source-1", null, null, null, new[] { "topic-1", "topic-1x" });
        //    builder.AddSource<string, string>(null, "source-2", null, null, null, "topic-2");
        //    builder.AddSource<string, string>(null, "source-3", null, null, null, "topic-3");
        //    builder.AddSource<string, string>(null, "source-4", null, null, null, "topic-4");
        //    builder.AddSource<string, string>(null, "source-5", null, null, null, "topic-5");
        //
        //    builder.AddProcessor<string, string>("processor-1", new MockProcessorSupplier<byte[], byte[]>(), "source-1");
        //    builder.AddProcessor<string, string>("processor-2", new MockProcessorSupplier<byte[], byte[]>(), "source-2");
        //    builder.AddStateStore<string, string, IKeyValueStore<int, byte[]>>(new MockKeyValueStoreBuilder(null, "store-1", false), "processor-1", "processor-2");
        //
        //    builder.AddProcessor<string, string>("processor-3", new MockProcessorSupplier<byte[], byte[]>(), "source-3");
        //    builder.AddProcessor<string, string>("processor-4", new MockProcessorSupplier<byte[], byte[]>(), "source-4");
        //    builder.AddStateStore<string, string, IKeyValueStore<int, byte[]>>(new MockKeyValueStoreBuilder(null, "store-2", false), "processor-3", "processor-4");
        //
        //    builder.AddProcessor<string, string>("processor-5", new MockProcessorSupplier<byte[], byte[]>(), "source-5");
        //    builder.AddStateStore<string, string, IKeyValueStore<int, byte[]>>(new MockKeyValueStoreBuilder(null, "store-3", false));
        //    builder.ConnectProcessorAndStateStores("processor-5", "store-3");
        //
        //    Dictionary<int, TopicsInfo> topicGroups = builder.TopicGroups();
        //
        //    Dictionary<int, TopicsInfo> expectedTopicGroups = new Dictionary<int, TopicsInfo>();
        //    string store1 = ProcessorStateManager.StoreChangelogTopic("X", "store-1");
        //    string store2 = ProcessorStateManager.StoreChangelogTopic("X", "store-2");
        //    string store3 = ProcessorStateManager.StoreChangelogTopic("X", "store-3");
        //    expectedTopicGroups.Put(0, new TopicsInfo(
        //        new HashSet<string>(), new HashSet<string> { "topic-1", "topic-1x", "topic-2" },
        //        new Dictionary<string, InternalTopicConfig>(),
        //        Collections.singletonMap(store1, new UnwindowedChangelogTopicConfig(store1, Collections.emptyMap()))));
        //    expectedTopicGroups.Put(1, new TopicsInfo(
        //        new HashSet<string>(), new HashSet<string> { "topic-3", "topic-4" },
        //        new Dictionary<string, InternalTopicConfig>(),
        //        Collections.singletonMap(store2, new UnwindowedChangelogTopicConfig(store2, Collections.emptyMap()))));
        //    expectedTopicGroups.Put(2, new TopicsInfo(
        //        new HashSet<string>(), new HashSet<string> { "topic-5" },
        //        new Dictionary<string, InternalTopicConfig>(),
        //        Collections.singletonMap(store3, new UnwindowedChangelogTopicConfig(store3, Collections.emptyMap()))));
        //
        //    Assert.Equal(3, topicGroups.Count);
        //    Assert.Equal(expectedTopicGroups, topicGroups);
        //}

        [Fact]
        public void TestBuild()
        {
            Builder.AddSource<string, string>(null, "source-1", null, null, null, new[] { "topic-1", "topic-1x" });
            Builder.AddSource<string, string>(null, "source-2", null, null, null, "topic-2");
            Builder.AddSource<string, string>(null, "source-3", null, null, null, "topic-3");
            Builder.AddSource<string, string>(null, "source-4", null, null, null, "topic-4");
            Builder.AddSource<string, string>(null, "source-5", null, null, null, "topic-5");

            Builder.AddProcessor<string, string>("processor-1", new MockProcessorSupplier<byte[], byte[]>(), "source-1");
            Builder.AddProcessor<string, string>("processor-2", new MockProcessorSupplier<byte[], byte[]>(), "source-2", "processor-1");
            Builder.AddProcessor<string, string>("processor-3", new MockProcessorSupplier<byte[], byte[]>(), "source-3", "source-4");

            Builder.SetApplicationId("X");
            ProcessorTopology topology0 = Builder.Build(0);
            ProcessorTopology topology1 = Builder.Build(1);
            ProcessorTopology topology2 = Builder.Build(2);

            Assert.Equal(new HashSet<string> { "source-1", "source-2", "processor-1", "processor-2" }, NodeNames(topology0.Processors()));
            Assert.Equal(new HashSet<string> { "source-3", "source-4", "processor-3" }, NodeNames(topology1.Processors()));
            Assert.Equal(new HashSet<string> { "source-5" }, NodeNames(topology2.Processors()));
        }

        [Fact]
        public void ShouldAllowIncrementalBuilds()
        {
            Dictionary<int, HashSet<string>> oldNodeGroups, newNodeGroups;

            oldNodeGroups = Builder.GetNodeGroups();
            Builder.AddSource<string, string>(null, "source-1", null, null, null, "topic-1");
            Builder.AddSource<string, string>(null, "source-2", null, null, null, "topic-2");
            newNodeGroups = Builder.GetNodeGroups();
            Assert.NotEqual(oldNodeGroups, newNodeGroups);

            oldNodeGroups = newNodeGroups;
            Builder.AddSource<string, string>(null, "source-3", null, null, null, new Regex("^$", RegexOptions.Compiled));
            Builder.AddSource<string, string>(null, "source-4", null, null, null, new Regex("^$", RegexOptions.Compiled));
            newNodeGroups = Builder.GetNodeGroups();
            Assert.NotEqual(oldNodeGroups, newNodeGroups);

            oldNodeGroups = newNodeGroups;
            Builder.AddProcessor<string, string>("processor-1", new MockProcessorSupplier<byte[], byte[]>(), "source-1");
            Builder.AddProcessor<string, string>("processor-2", new MockProcessorSupplier<byte[], byte[]>(), "source-2");
            Builder.AddProcessor<string, string>("processor-3", new MockProcessorSupplier<byte[], byte[]>(), "source-3");
            newNodeGroups = Builder.GetNodeGroups();
            Assert.NotEqual(oldNodeGroups, newNodeGroups);

            oldNodeGroups = newNodeGroups;
            Builder.AddSink<string, string>("sink-1", "sink-topic", null, null, null, "processor-1");
            newNodeGroups = Builder.GetNodeGroups();
            Assert.NotEqual(oldNodeGroups, newNodeGroups);

            oldNodeGroups = newNodeGroups;
            Builder.AddSink<string, string>("sink-2", (k, v, ctx) => "sink-topic", null, null, null, "processor-2");
            newNodeGroups = Builder.GetNodeGroups();
            Assert.NotEqual(oldNodeGroups, newNodeGroups);

            oldNodeGroups = newNodeGroups;
            Builder.AddStateStore<string, string, IKeyValueStore<int, byte[]>>(new MockKeyValueStoreBuilder(this.streamsBuilder.Context, "store-1", false), "processor-1", "processor-2");
            newNodeGroups = Builder.GetNodeGroups();
            Assert.NotEqual(oldNodeGroups, newNodeGroups);

            oldNodeGroups = newNodeGroups;
            Builder.AddStateStore<string, string, IKeyValueStore<int, byte[]>>(new MockKeyValueStoreBuilder(this.streamsBuilder.Context, "store-2", false));
            Builder.ConnectProcessorAndStateStores("processor-2", "store-2");
            Builder.ConnectProcessorAndStateStores("processor-3", "store-2");
            newNodeGroups = Builder.GetNodeGroups();
            Assert.NotEqual(oldNodeGroups, newNodeGroups);

            oldNodeGroups = newNodeGroups;
            Builder.AddGlobalStore(new MockKeyValueStoreBuilder(this.streamsBuilder.Context, "global-store", false).WithLoggingDisabled(), "globalSource", null, null, null, "globalTopic", "global-processor", new MockProcessorSupplier<byte[], byte[]>());
            newNodeGroups = Builder.GetNodeGroups();
            Assert.NotEqual(oldNodeGroups, newNodeGroups);
        }

        [Fact]// (expected = NullReferenceException)
        public void ShouldNotAllowNullNameWhenAddingSink()
        {
            Assert.Throws<ArgumentNullException>(() => Builder.AddSink<string, string>(null, "topic", null, null, null));
        }

        [Fact]// (expected = NullReferenceException)
        public void ShouldNotAllowNullTopicWhenAddingSink()
        {
            Assert.Throws<ArgumentNullException>(() => Builder.AddSink<string, string>("Name", (string)null, null, null, null));
        }

        [Fact]
        public void ShouldNotAllowNullTopicChooserWhenAddingSink()
        {
            Assert.Throws<ArgumentNullException>(() => Builder.AddSink<string, string>("Name", (TopicNameExtractor<string, string>)null, null, null, null));
        }

        [Fact]
        public void ShouldNotAllowNullNameWhenAddingProcessor()
        {
            Assert.Throws<ArgumentNullException>(() => Builder.AddProcessor<string, string>(null, null));
        }

        [Fact]
        public void ShouldNotAllowNullProcessorSupplier()
        {
            Assert.Throws<ArgumentNullException>(() => Builder.AddProcessor<string, string>("Name", null));
        }

        [Fact]
        public void ShouldNotAllowNullNameWhenAddingSource()
        {
            Assert.Throws<ArgumentNullException>(() => Builder.AddSource<string, string>(null, null, null, null, null, new Regex(".*", RegexOptions.Compiled)));
        }

        [Fact]// (expected = NullReferenceException)
        public void ShouldNotAllowNullProcessorNameWhenConnectingProcessorAndStateStores()
        {
            Assert.Throws<ArgumentNullException>(() => Builder.ConnectProcessorAndStateStores(null, "store"));
        }

        [Fact]
        public void ShouldNotAllowNullStateStoreNameWhenConnectingProcessorAndStateStores()
        {
            Assert.Throws<ArgumentNullException>(() => Builder.ConnectProcessorAndStateStores("processor", null));
        }

        [Fact]
        public void ShouldNotAddNullInternalTopic()
        {
            Assert.Throws<ArgumentNullException>(() => Builder.AddInternalTopic(null));
        }

        [Fact]
        public void ShouldNotSetApplicationIdToNull()
        {
            Assert.Throws<ArgumentNullException>(() => Builder.SetApplicationId(null));
        }

        [Fact]
        public void ShouldNotAddNullStateStoreSupplier()
        {
            Assert.Throws<ArgumentNullException>(() => Builder.AddStateStore<string, string, IKeyValueStore<int, byte[]>>(null));
        }

        private HashSet<string> NodeNames(IEnumerable<IProcessorNode> nodes)
        {
            HashSet<string> nodeNames = new HashSet<string>();
            foreach (ProcessorNode node in nodes)
            {
                nodeNames.Add(node.Name);
            }
            return nodeNames;
        }

        [Fact]
        public void ShouldAssociateStateStoreNameWhenStateStoreSupplierIsInternal()
        {
            Builder.AddSource<string, string>(null, "source", null, null, null, "topic");
            Builder.AddProcessor<string, string>("processor", new MockProcessorSupplier<byte[], byte[]>(), "source");
            Builder.AddStateStore<string, string, IKeyValueStore<int, byte[]>>(storeBuilder, "processor");
            Dictionary<string, List<string>> stateStoreNameToSourceTopic = Builder.StateStoreNameToSourceTopics();
            Assert.Single(stateStoreNameToSourceTopic);
            Assert.Equal(new[] { "topic" }, stateStoreNameToSourceTopic["store"]);
        }

        [Fact]
        public void ShouldAssociateStateStoreNameWhenStateStoreSupplierIsExternal()
        {
            Builder.AddSource<string, string>(null, "source", null, null, null, "topic");
            Builder.AddProcessor<string, string>("processor", new MockProcessorSupplier<byte[], byte[]>(), "source");
            Builder.AddStateStore<string, string, IKeyValueStore<int, byte[]>>(storeBuilder, "processor");
            Dictionary<string, List<string>> stateStoreNameToSourceTopic = Builder.StateStoreNameToSourceTopics();
            Assert.Single(stateStoreNameToSourceTopic);
            Assert.Equal(new[] { "topic" }, stateStoreNameToSourceTopic["store"]);
        }

        [Fact]
        public void ShouldCorrectlyMapStateStoreToInternalTopics()
        {
            Builder.SetApplicationId("appId");
            Builder.AddInternalTopic("internal-topic");
            Builder.AddSource<string, string>(null, "source", null, null, null, "internal-topic");
            Builder.AddProcessor<string, string>("processor", new MockProcessorSupplier<byte[], byte[]>(), "source");
            Builder.AddStateStore<string, string, IKeyValueStore<int, byte[]>>(storeBuilder, "processor");
            Dictionary<string, List<string>> stateStoreNameToSourceTopic = Builder.StateStoreNameToSourceTopics();
            Assert.Single(stateStoreNameToSourceTopic);
            Assert.Equal(new[] { "appId-internal-topic" }, stateStoreNameToSourceTopic["store"]);
        }

        //[Fact]
        //public void ShouldAddInternalTopicConfigForWindowStores()
        //{
        //    builder.SetApplicationId("appId");
        //    builder.AddSource<string, string>(null, "source", null, null, null, "topic");
        //    builder.AddProcessor<string, string>("processor", new MockProcessorSupplier<byte[], byte[]>(), "source");
        //    builder.AddStateStore(
        //        Stores.windowStoreBuilder(
        //            Stores.PersistentWindowStore("store1", TimeSpan.FromSeconds(30L), TimeSpan.FromSeconds(10L), false),
        //            Serdes.String(),
        //            Serdes.String()
        //        ),
        //        "processor"
        //    );
        //    builder.AddStateStore(
        //            Stores.sessionStoreBuilder(
        //                    Stores.PersistentSessionStore("store2", TimeSpan.FromSeconds(30)), Serdes.String(), Serdes.String()
        //            ),
        //            "processor"
        //    );
        //    Dictionary<int, TopicsInfo> topicGroups = builder.TopicGroups();
        //    TopicsInfo topicsInfo = topicGroups.Values.First();
        //    InternalTopicConfig topicConfig1 = topicsInfo.stateChangelogTopics["appId-store1-changelog"];
        //    Dictionary<string, string> properties1 = topicConfig1.getProperties(Collections.emptyMap(), 10000);
        //    Assert.Equal(2, properties1.Count);
        //    Assert.Equal(StreamsConfig.CLEANUP_POLICY_COMPACT + "," + StreamsConfig.StateCleanupDelayMsConfig, properties1[StreamsConfig.CLEANUP_POLICY_CONFIG]);
        //    Assert.Equal("40000", properties1[StreamsConfig.RETENTION_MS_CONFIG]);
        //    Assert.Equal("appId-store1-changelog", topicConfig1.Name);
        //    Assert.True(topicConfig1 is WindowedChangelogTopicConfig);
        //    InternalTopicConfig topicConfig2 = topicsInfo.stateChangelogTopics["appId-store2-changelog"];
        //    Dictionary<string, string> properties2 = topicConfig2.getProperties(Collections.emptyMap(), 10000);
        //    Assert.Equal(2, properties2.Count);
        //    Assert.Equal(StreamsConfig.CLEANUP_POLICY_COMPACT + "," + StreamsConfig.CLEANUP_POLICY_DELETE, properties2[StreamsConfig.CLEANUP_POLICY_CONFIG]);
        //    Assert.Equal("40000", properties2[StreamsConfig.RETENTION_MS_CONFIG]);
        //    Assert.Equal("appId-store2-changelog", topicConfig2.Name);
        //    Assert.True(topicConfig2 is WindowedChangelogTopicConfig);
        //}

        [Fact]
        public void ShouldAddInternalTopicConfigForNonWindowStores()
        {
            Builder.SetApplicationId("appId");
            Builder.AddSource<string, string>(null, "source", null, null, null, "topic");
            Builder.AddProcessor<string, string>("processor", new MockProcessorSupplier<byte[], byte[]>(), "source");
            Builder.AddStateStore<string, string, IKeyValueStore<int, byte[]>>(storeBuilder, "processor");
            Dictionary<int, TopicsInfo> topicGroups = Builder.TopicGroups();
            TopicsInfo topicsInfo = topicGroups.Values.First();
            InternalTopicConfig topicConfig = topicsInfo.stateChangelogTopics["appId-store-changelog"];
            var properties = topicConfig.GetProperties(new Dictionary<string, string?>(), 10000);
            Assert.Single(properties);
            //Assert.Equal(StreamsConfig.CLEANUP_POLICY_COMPACT, properties[StreamsConfig.CLEANUP_POLICY_CONFIG]);
            //Assert.Equal("appId-store-changelog", topicConfig.Name);
            //Assert.True(topicConfig is UnwindowedChangelogTopicConfig);
        }

        [Fact]
        public void ShouldAddInternalTopicConfigForRepartitionTopics()
        {
            Builder.SetApplicationId("appId");
            Builder.AddInternalTopic("foo");
            Builder.AddSource<string, string>(null, "source", null, null, null, "foo");
            TopicsInfo topicsInfo = Builder.TopicGroups().Values.First();
            InternalTopicConfig topicConfig = topicsInfo.repartitionSourceTopics["appId-foo"];
            var properties = topicConfig.GetProperties(new Dictionary<string, string?>(), 10000);
            Assert.Equal(3, properties.Count);
            //Assert.Equal((-1).ToString(), properties[StreamsConfig.RETENTION_MS_CONFIG]);
            //Assert.Equal(StreamsConfig.CLEANUP_POLICY_DELETE, properties[StreamsConfig.CLEANUP_POLICY_CONFIG]);
            //Assert.Equal("appId-foo", topicConfig.Name);
            //Assert.True(topicConfig is RepartitionTopicConfig);
        }


        [Fact]
        public void ShouldSetCorrectSourceNodesWithRegexUpdatedTopics()
        {// throws Exception
            Builder.AddSource<string, string>(null, "source-1", null, null, null, "topic-foo");
            Builder.AddSource<string, string>(null, "source-2", null, null, null, new Regex("topic-[A-C]", RegexOptions.Compiled));
            Builder.AddSource<string, string>(null, "source-3", null, null, null, new Regex("topic-\\d", RegexOptions.Compiled));

            SubscriptionUpdates subscriptionUpdates = new SubscriptionUpdates();
            var updatedTopicsField = subscriptionUpdates.GetType().GetProperty("updatedTopicSubscriptions");
            //updatedTopicsField.setAccessible(true);

            //HashSet<string> updatedTopics = (HashSet<string>)updatedTopicsField[subscriptionUpdates];
            //
            //updatedTopics.Add("topic-B");
            //updatedTopics.Add("topic-3");
            //updatedTopics.Add("topic-A");

            Builder.UpdateSubscriptions(subscriptionUpdates, null);
            Builder.SetApplicationId("test-id");

            Dictionary<int, TopicsInfo> topicGroups = Builder.TopicGroups();
            Assert.Contains("topic-foo", topicGroups[0].sourceTopics);
            Assert.Contains("topic-A", topicGroups[1].sourceTopics);
            Assert.Contains("topic-B", topicGroups[1].sourceTopics);
            Assert.Contains("topic-3", topicGroups[2].sourceTopics);
        }

        //[Fact]
        //public void ShouldAddTimestampExtractorPerSource()
        //{
        //    builder.AddSource<string, string>(null, "source", new MockTimestampExtractor(), null, null, "topic");
        //    ProcessorTopology processorTopology = builder.RewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig())).Build(null);
        //    Assert.Equal(processorTopology.Source("topic").GetTimestampExtractor(), typeof(MockTimestampExtractor));
        //}

        //[Fact]
        //public void ShouldAddTimestampExtractorWithPatternPerSource()
        //{
        //    var pattern = new Regex("t.*", RegexOptions.Compiled);
        //    builder.AddSource<string, string>(null, "source", new MockTimestampExtractor(), null, null, pattern);
        //    ProcessorTopology processorTopology = builder.RewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig())).Build(null);
        //    Assert.Equal(processorTopology.Source(pattern.ToString()).GetTimestampExtractor(), typeof(MockTimestampExtractor));
        //}

        [Fact]
        public void ShouldSortProcessorNodesCorrectly()
        {
            Builder.AddSource<string, string>(null, "source1", null, null, null, "topic1");
            Builder.AddSource<string, string>(null, "source2", null, null, null, "topic2");
            Builder.AddProcessor<string, string>("processor1", new MockProcessorSupplier<byte[], byte[]>(), "source1");
            Builder.AddProcessor<string, string>("processor2", new MockProcessorSupplier<byte[], byte[]>(), "source1", "source2");
            Builder.AddProcessor<string, string>("processor3", new MockProcessorSupplier<byte[], byte[]>(), "processor2");
            Builder.AddSink<string, string>("sink1", "topic2", null, null, null, "processor1", "processor3");

            Assert.Single(Builder.Describe().subtopologies);

            var iterator = ((ISubtopology)Builder.Describe().subtopologies).nodes.GetEnumerator();//.nodesInOrder();

            Assert.True(iterator.MoveNext());
            AbstractNode node = (AbstractNode)iterator.Current;
            Assert.Equal("source1", node.Name);
            Assert.Equal(6, node.Size);

            Assert.True(iterator.MoveNext());
            node = (AbstractNode)iterator.Current;
            Assert.Equal("source2", node.Name);
            Assert.Equal(4, node.Size);

            Assert.True(iterator.MoveNext());
            node = (AbstractNode)iterator.Current;
            Assert.Equal("processor2", node.Name);
            Assert.Equal(3, node.Size);

            Assert.True(iterator.MoveNext());
            node = (AbstractNode)iterator.Current;
            Assert.Equal("processor1", node.Name);
            Assert.Equal(2, node.Size);

            Assert.True(iterator.MoveNext());
            node = (AbstractNode)iterator.Current;
            Assert.Equal("processor3", node.Name);
            Assert.Equal(2, node.Size);

            Assert.True(iterator.MoveNext());
            node = (AbstractNode)iterator.Current;
            Assert.Equal("sink1", node.Name);
            Assert.Equal(1, node.Size);
        }


        [Fact]
        public void ShouldConnectRegexMatchedTopicsToStateStore()
        {// throws Exception
            Builder.AddSource<string, string>(null, "ingest", null, null, null, new Regex("topic-\\d+", RegexOptions.Compiled));
            Builder.AddProcessor<string, string>("my-processor", new MockProcessorSupplier<byte[], byte[]>(), "ingest");
            Builder.AddStateStore<string, string, IKeyValueStore<int, byte[]>>(storeBuilder, "my-processor");

            SubscriptionUpdates subscriptionUpdates = new SubscriptionUpdates();
            //var updatedTopicsField = subscriptionUpdates.GetType().getDeclaredField("updatedTopicSubscriptions");
            //updatedTopicsField.setAccessible(true);
            //
            //HashSet<string> updatedTopics = (HashSet<string>)updatedTopicsField[subscriptionUpdates];
            //
            //updatedTopics.Add("topic-2");
            //updatedTopics.Add("topic-3");
            //updatedTopics.Add("topic-A");

            Builder.UpdateSubscriptions(subscriptionUpdates, "test-thread");
            Builder.SetApplicationId("test-app");

            Dictionary<string, List<string>> stateStoreAndTopics = Builder.StateStoreNameToSourceTopics();
            List<string> topics = stateStoreAndTopics[storeBuilder.Name];

            Assert.Equal(2, topics.Count);

            Assert.Contains("topic-2", topics);
            Assert.Contains("topic-3", topics);
            Assert.DoesNotContain("topic-A", topics);
        }


        [Fact]// (expected = TopologyException)
        public void ShouldNotAllowToAddGlobalStoreWithSourceNameEqualsProcessorName()
        {
            string sameNameForSourceAndProcessor = "sameName";
            Assert.Throws<TopologyException>(() => Builder.AddGlobalStore(
                storeBuilder,
                sameNameForSourceAndProcessor,
                null,
                null,
                null,
                "anyTopicName",
                sameNameForSourceAndProcessor,
                new MockProcessorSupplier<byte[], byte[]>()));
        }

        [Fact]
        public void ShouldThrowIfNameIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => new Source(null, new HashSet<string>(), null));
        }

        [Fact]
        public void ShouldThrowIfTopicAndPatternAreNull()
        {
            Exception e = Assert.Throws<ArgumentException>(() => new Source("Name", null, null));
            Assert.Equal("Either topics or pattern must be not-null, but both are null.", e.Message);
        }

        [Fact]
        public void ShouldThrowIfBothTopicAndPatternAreNotNull()
        {
            Exception e = Assert.Throws<ArgumentException>(() => new Source("Name", new HashSet<string>(), new Regex("", RegexOptions.Compiled)));
            Assert.Equal("Either topics or pattern must be null, but both are not null.", e.Message);
        }

        [Fact]
        public void SourceShouldBeEqualIfNameAndTopicListAreTheSame()
        {
            Source @base = new Source("Name", Collections.singleton("topic"), null);
            Source sameAsBase = new Source("Name", Collections.singleton("topic"), null);

            Assert.Equal(@base, sameAsBase);
        }

        [Fact]
        public void SourceShouldBeEqualIfNameAndPatternAreTheSame()
        {
            Source @base = new Source("Name", null, new Regex("topic", RegexOptions.Compiled));
            Source sameAsBase = new Source("Name", null, new Regex("topic", RegexOptions.Compiled));

            Assert.Equal(@base, sameAsBase);
        }

        [Fact]
        public void SourceShouldNotBeEqualForDifferentNamesWithSameTopicList()
        {
            Source @base = new Source("Name", Collections.singleton("topic"), null);
            Source differentName = new Source("name2", Collections.singleton("topic"), null);

            Assert.NotEqual(@base, differentName);
        }

        [Fact]
        public void SourceShouldNotBeEqualForDifferentNamesWithSamePattern()
        {
            Source @base = new Source("Name", null, new Regex("topic", RegexOptions.Compiled));
            Source differentName = new Source("name2", null, new Regex("topic", RegexOptions.Compiled));

            Assert.NotEqual(@base, differentName);
        }

        [Fact]
        public void SourceShouldNotBeEqualForDifferentTopicList()
        {
            Source @base = new Source("Name", Collections.singleton("topic"), null);
            Source differentTopicList = new Source("Name", new HashSet<string>(), null);
            Source differentTopic = new Source("Name", Collections.singleton("topic2"), null);

            Assert.NotEqual(@base, differentTopicList);
            Assert.NotEqual(@base, differentTopic);
        }

        [Fact]
        public void SourceShouldNotBeEqualForDifferentPattern()
        {
            Source @base = new Source("Name", null, new Regex("topic", RegexOptions.Compiled));
            Source differentPattern = new Source("Name", null, new Regex("topic2", RegexOptions.Compiled));
            Source overlappingPattern = new Source("Name", null, new Regex("top*", RegexOptions.Compiled));

            Assert.NotEqual(@base, differentPattern);
            Assert.NotEqual(@base, overlappingPattern);
        }
    }
}

#pragma warning restore CS8600 // Converting null literal or possible null value to non-nullable type.
#pragma warning restore CS8625 // Cannot convert null literal to non-nullable reference type.
