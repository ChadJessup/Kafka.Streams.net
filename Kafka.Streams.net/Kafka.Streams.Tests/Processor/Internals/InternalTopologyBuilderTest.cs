//using Kafka.Streams.Configs;
//using Kafka.Streams.Errors;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State;
//using Kafka.Streams.Tests.Mocks;
//using Kafka.Streams.Topologies;
//using System.Collections.Generic;
//using System.Text.RegularExpressions;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    public class InternalTopologyBuilderTest
//    {
//        private ISerde<string> stringSerde = Serdes.String();
//        private InternalTopologyBuilder builder = new InternalTopologyBuilder();
//        private IStoreBuilder<string> storeBuilder = new MockKeyValueStoreBuilder("store", false);

//        [Xunit.Fact]
//        public void ShouldAddSourceWithOffsetReset()
//        {
//            string earliestTopic = "earliestTopic";
//            string latestTopic = "latestTopic";

//            builder.AddSource(Topology.AutoOffsetReset.EARLIEST, "source", null, null, null, earliestTopic);
//            builder.AddSource(Topology.AutoOffsetReset.LATEST, "source2", null, null, null, latestTopic);

//            Assert.True(builder.earliestResetTopicsPattern().matcher(earliestTopic).matches());
//            Assert.True(builder.latestResetTopicsPattern().matcher(latestTopic).matches());
//        }

//        [Xunit.Fact]
//        public void ShouldAddSourcePatternWithOffsetReset()
//        {
//            string earliestTopicPattern = "earliest.*Topic";
//            string latestTopicPattern = "latest.*Topic";

//            builder.AddSource(Topology.AutoOffsetReset.EARLIEST, "source", null, null, null, new Regex(earliestTopicPattern, RegexOptions.Compiled));
//            builder.AddSource(Topology.AutoOffsetReset.LATEST, "source2", null, null, null, new Regex(latestTopicPattern, RegexOptions.Compiled));

//            Assert.True(builder.EarliestResetTopicsPattern.matcher("earliestTestTopic").matches());
//            Assert.True(builder.LatestResetTopicsPattern.matcher("latestTestTopic").matches());
//        }

//        [Xunit.Fact]
//        public void ShouldAddSourceWithoutOffsetReset()
//        {
//            var expectedPattern = new Regex("test-topic", RegexOptions.Compiled);

//            builder.AddSource(null, "source", null, stringSerde.Deserializer, stringSerde.Deserializer, "test-topic");

//            Assert.Equal(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
//            Assert.Equal(builder.EarliestResetTopicsPattern.pattern(), "");
//            Assert.Equal(builder.LatestResetTopicsPattern.pattern(), "");
//        }

//        [Xunit.Fact]
//        public void ShouldAddPatternSourceWithoutOffsetReset()
//        {
//            var expectedPattern = new Regex("test-.*", RegexOptions.Compiled);

//            builder.AddSource(null, "source", null, stringSerde.Deserializer, stringSerde.Deserializer, new Regex("test-.*", RegexOptions.Compiled));

//            Assert.Equal(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
//            Assert.Equal(builder.earliestResetTopicsPattern().pattern(), "");
//            Assert.Equal(builder.latestResetTopicsPattern().pattern(), "");
//        }

//        [Xunit.Fact]// (expected = TopologyException)
//        public void ShouldNotAllowOffsetResetSourceWithoutTopics()
//        {
//            builder.AddSource(Topology.AutoOffsetReset.EARLIEST, "source", null, stringSerde.deserializer(), stringSerde.deserializer());
//        }

//        [Xunit.Fact]
//        public void ShouldNotAllowOffsetResetSourceWithDuplicateSourceName()
//        {
//            builder.AddSource(Topology.AutoOffsetReset.EARLIEST, "source", null, stringSerde.deserializer(), stringSerde.deserializer(), "topic-1");
//            try
//            {
//                builder.AddSource(Topology.AutoOffsetReset.LATEST, "source", null, stringSerde.deserializer(), stringSerde.deserializer(), "topic-2");
//                Assert.True(false, "Should throw TopologyException for duplicate source name");
//            }
//            catch (TopologyException expected) { /* ok */ }
//        }

//        [Xunit.Fact]
//        public void TestAddSourceWithSameName()
//        {
//            builder.AddSource<string, string>(null, "source", null, null, null, "topic-1");
//            try
//            {
//                builder.AddSource<string, string>(null, "source", null, null, null, "topic-2");
//                Assert.True(false, "Should throw TopologyException with source name conflict");
//            }
//            catch (TopologyException expected) { /* ok */ }
//        }

//        [Xunit.Fact]
//        public void TestAddSourceWithSameTopic()
//        {
//            builder.AddSource<string, string>(null, "source", null, null, null, "topic-1");
//            try
//            {
//                builder.AddSource<string, string>(null, "source-2", null, null, null, "topic-1");
//                Assert.True(false, "Should throw TopologyException with topic conflict");
//            }
//            catch (TopologyException expected) { /* ok */ }
//        }

//        [Xunit.Fact]
//        public void TestAddProcessorWithSameName()
//        {
//            builder.AddSource<string, string>(null, "source", null, null, null, "topic-1");
//            builder.AddProcessor("processor", new MockProcessorSupplier(), "source");
//            try
//            {
//                builder.AddProcessor("processor", new MockProcessorSupplier(), "source");
//                Assert.True(false, "Should throw TopologyException with processor name conflict");
//            }
//            catch (TopologyException expected) { /* ok */ }
//        }

//        [Xunit.Fact]// (expected = TopologyException)
//        public void TestAddProcessorWithWrongParent()
//        {
//            builder.AddProcessor("processor", new MockProcessorSupplier(), "source");
//        }

//        [Xunit.Fact]// (expected = TopologyException)
//        public void TestAddProcessorWithSelfParent()
//        {
//            builder.AddProcessor("processor", new MockProcessorSupplier(), "processor");
//        }

//        [Xunit.Fact]// (expected = TopologyException)
//        public void TestAddProcessorWithEmptyParents()
//        {
//            builder.AddProcessor("processor", new MockProcessorSupplier());
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void TestAddProcessorWithNullParents()
//        {
//            builder.AddProcessor("processor", new MockProcessorSupplier(), (string)null);
//        }

//        [Xunit.Fact]
//        public void TestAddSinkWithSameName()
//        {
//            builder.AddSource<string, string>(null, "source", null, null, null, "topic-1");
//            builder.AddSink<string, string>("sink", "topic-2", null, null, null, "source");
//            try
//            {
//                builder.AddSink<string, string>("sink", "topic-3", null, null, null, "source");
//                Assert.True(false, "Should throw TopologyException with sink name conflict");
//            }
//            catch (TopologyException expected) { /* ok */ }
//        }

//        [Xunit.Fact]// (expected = TopologyException)
//        public void TestAddSinkWithWrongParent()
//        {
//            builder.AddSink<string, string>("sink", "topic-2", null, null, null, "source");
//        }

//        [Xunit.Fact]// (expected = TopologyException)
//        public void TestAddSinkWithSelfParent()
//        {
//            builder.AddSink<string, string>("sink", "topic-2", null, null, null, "sink");
//        }


//        [Xunit.Fact]// (expected = TopologyException)
//        public void TestAddSinkWithEmptyParents()
//        {
//            builder.AddSink<string, string>("sink", "topic", null, null, null);
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void TestAddSinkWithNullParents()
//        {
//            builder.AddSink<string, string>("sink", "topic", null, null, null, (string)null);
//        }

//        [Xunit.Fact]
//        public void TestAddSinkConnectedWithParent()
//        {
//            builder.AddSource<string, string>(null, "source", null, null, null, "source-topic");
//            builder.AddSink<string, string>("sink", "dest-topic", null, null, null, "source");

//            Dictionary<int, HashSet<string>> nodeGroups = builder.nodeGroups();
//            HashSet<string> nodeGroup = nodeGroups.Get(0);

//            Assert.Contains("sink", nodeGroup);
//            Assert.Contains("source", nodeGroup);
//        }

//        [Xunit.Fact]
//        public void TestAddSinkConnectedWithMultipleParent()
//        {
//            builder.AddSource<string, string>(null, "source", null, null, null, "source-topic");
//            builder.AddSource<string, string>(null, "sourceII", null, null, null, "source-topicII");
//            builder.AddSink<string, string>("sink", "dest-topic", null, null, null, "source", "sourceII");

//            Dictionary<int, HashSet<string>> nodeGroups = builder.nodeGroups();
//            HashSet<string> nodeGroup = nodeGroups.Get(0);

//            Assert.Contains("sink", nodeGroup);
//            Assert.Contains("source", nodeGroup);
//            Assert.Contains("sourceII", nodeGroup);
//        }

//        [Xunit.Fact]
//        public void TestSourceTopics()
//        {
//            builder.SetApplicationId("X");
//            builder.AddSource<string, string>(null, "source-1", null, null, null, "topic-1");
//            builder.AddSource<string, string>(null, "source-2", null, null, null, "topic-2");
//            builder.AddSource<string, string>(null, "source-3", null, null, null, "topic-3");
//            builder.AddInternalTopic("topic-3");

//            var expectedPattern = new Regex("X-topic-3|topic-1|topic-2", RegexOptions.Compiled);

//            Assert.Equal(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
//        }

//        [Xunit.Fact]
//        public void TestPatternSourceTopic()
//        {
//            Pattern expectedPattern = new Regex("topic-\\d", RegexOptions.Compiled);
//            builder.AddSource(null, "source-1", null, null, null, expectedPattern);
//            Assert.Equal(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
//        }

//        [Xunit.Fact]
//        public void TestAddMoreThanOnePatternSourceNode()
//        {
//            Pattern expectedPattern = new Regex("topics[A-Z]|.*-\\d", RegexOptions.Compiled);
//            builder.AddSource<string, string>(null, "source-1", null, null, null, new Regex("topics[A-Z]", RegexOptions.Compiled));
//            builder.AddSource<string, string>(null, "source-2", null, null, null, new Regex(".*-\\d", RegexOptions.Compiled));
//            Assert.Equal(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
//        }

//        [Xunit.Fact]
//        public void TestSubscribeTopicNameAndPattern()
//        {
//            Pattern expectedPattern = new Regex("topic-bar|topic-foo|.*-\\d", RegexOptions.Compiled);
//            builder.AddSource<string, string>(null, "source-1", null, null, null, "topic-foo", "topic-bar");
//            builder.AddSource<string, string>(null, "source-2", null, null, null, new Regex(".*-\\d", RegexOptions.Compiled));
//            Assert.Equal(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
//        }

//        [Xunit.Fact]
//        public void TestPatternMatchesAlreadyProvidedTopicSource()
//        {
//            builder.AddSource<string, string>(null, "source-1", null, null, null, "foo");
//            try
//            {
//                builder.AddSource<string, string>(null, "source-2", null, null, null, new Regex("f.*", RegexOptions.Compiled));
//                Assert.True(false, "Should throw TopologyException with topic name/pattern conflict");
//            }
//            catch (TopologyException expected) { /* ok */ }
//        }

//        [Xunit.Fact]
//        public void TestNamedTopicMatchesAlreadyProvidedPattern()
//        {
//            builder.AddSource<string, string>(null, "source-1", null, null, null, new Regex("f.*", RegexOptions.Compiled));
//            try
//            {
//                builder.AddSource<string, string>(null, "source-2", null, null, null, "foo");
//                Assert.True(false, "Should throw TopologyException with topic name/pattern conflict");
//            }
//            catch (TopologyException expected) { /* ok */ }
//        }

//        [Xunit.Fact]// (expected = TopologyException)
//        public void TestAddStateStoreWithNonExistingProcessor()
//        {
//            builder.AddStateStore<string, string, string>(storeBuilder, "no-such-processor");
//        }

//        [Xunit.Fact]
//        public void TestAddStateStoreWithSource()
//        {
//            builder.AddSource<string, string>(null, "source-1", null, null, null, "topic-1");
//            try
//            {
//                builder.AddStateStore<string, string>(storeBuilder, "source-1");
//                Assert.True(false, "Should throw TopologyException with store cannot be added to source");
//            }
//            catch (TopologyException expected) { /* ok */ }
//        }

//        [Xunit.Fact]
//        public void TestAddStateStoreWithSink()
//        {
//            builder.AddSource(null, "source-1", null, null, null, "topic-1");
//            builder.AddSink("sink-1", "topic-1", null, null, null, "source-1");
//            try
//            {
//                builder.AddStateStore(storeBuilder, "sink-1");
//                Assert.True(false, "Should throw TopologyException with store cannot be added to sink");
//            }
//            catch (TopologyException expected) { /* ok */ }
//        }

//        [Xunit.Fact]
//        public void TestAddStateStoreWithDuplicates()
//        {
//            builder.addStateStore(storeBuilder);
//            try
//            {
//                builder.addStateStore(storeBuilder);
//                Assert.True(false, "Should throw TopologyException with store name conflict");
//            }
//            catch (TopologyException expected) { /* ok */ }
//        }

//        [Xunit.Fact]
//        public void TestAddStateStore()
//        {
//            builder.addStateStore(storeBuilder);
//            builder.setApplicationId("X");
//            builder.AddSource(null, "source-1", null, null, null, "topic-1");
//            builder.AddProcessor("processor-1", new MockProcessorSupplier(), "source-1");

//            Assert.Equal(0, builder.Build(null).stateStores().Count);

//            builder.connectProcessorAndStateStores("processor-1", storeBuilder.name());

//            List<IStateStore> suppliers = builder.Build(null).stateStores();
//            Assert.Equal(1, suppliers.Count);
//            Assert.Equal(storeBuilder.name(), suppliers.Get(0).name());
//        }

//        [Xunit.Fact]
//        public void TestTopicGroups()
//        {
//            builder.setApplicationId("X");
//            builder.AddInternalTopic("topic-1x");
//            builder.AddSource(null, "source-1", null, null, null, "topic-1", "topic-1x");
//            builder.AddSource(null, "source-2", null, null, null, "topic-2");
//            builder.AddSource(null, "source-3", null, null, null, "topic-3");
//            builder.AddSource(null, "source-4", null, null, null, "topic-4");
//            builder.AddSource(null, "source-5", null, null, null, "topic-5");

//            builder.AddProcessor("processor-1", new MockProcessorSupplier(), "source-1");

//            builder.AddProcessor("processor-2", new MockProcessorSupplier(), "source-2", "processor-1");
//            builder.copartitionSources(asList("source-1", "source-2"));

//            builder.AddProcessor("processor-3", new MockProcessorSupplier(), "source-3", "source-4");

//            Dictionary<int, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();

//            Dictionary<int, InternalTopologyBuilder.TopicsInfo> expectedTopicGroups = new HashMap<>();
//            expectedTopicGroups.put(0, new InternalTopologyBuilder.TopicsInfo(Collections.emptySet(), mkSet("topic-1", "X-topic-1x", "topic-2"), Collections.emptyMap(), Collections.emptyMap()));
//            expectedTopicGroups.put(1, new InternalTopologyBuilder.TopicsInfo(Collections.emptySet(), mkSet("topic-3", "topic-4"), Collections.emptyMap(), Collections.emptyMap()));
//            expectedTopicGroups.put(2, new InternalTopologyBuilder.TopicsInfo(Collections.emptySet(), mkSet("topic-5"), Collections.emptyMap(), Collections.emptyMap()));

//            Assert.Equal(3, topicGroups.Count);
//            Assert.Equal(expectedTopicGroups, topicGroups);

//            Collection<HashSet<string>> copartitionGroups = builder.copartitionGroups();

//            Assert.Equal(mkSet(mkSet("topic-1", "X-topic-1x", "topic-2")), new HashSet<>(copartitionGroups));
//        }

//        [Xunit.Fact]
//        public void TestTopicGroupsByStateStore()
//        {
//            builder.setApplicationId("X");
//            builder.AddSource(null, "source-1", null, null, null, "topic-1", "topic-1x");
//            builder.AddSource(null, "source-2", null, null, null, "topic-2");
//            builder.AddSource(null, "source-3", null, null, null, "topic-3");
//            builder.AddSource(null, "source-4", null, null, null, "topic-4");
//            builder.AddSource(null, "source-5", null, null, null, "topic-5");

//            builder.AddProcessor("processor-1", new MockProcessorSupplier(), "source-1");
//            builder.AddProcessor("processor-2", new MockProcessorSupplier(), "source-2");
//            builder.addStateStore(new MockKeyValueStoreBuilder("store-1", false), "processor-1", "processor-2");

//            builder.AddProcessor("processor-3", new MockProcessorSupplier(), "source-3");
//            builder.AddProcessor("processor-4", new MockProcessorSupplier(), "source-4");
//            builder.addStateStore(new MockKeyValueStoreBuilder("store-2", false), "processor-3", "processor-4");

//            builder.AddProcessor("processor-5", new MockProcessorSupplier(), "source-5");
//            builder.addStateStore(new MockKeyValueStoreBuilder("store-3", false));
//            builder.connectProcessorAndStateStores("processor-5", "store-3");

//            Dictionary<int, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();

//            Dictionary<int, InternalTopologyBuilder.TopicsInfo> expectedTopicGroups = new HashMap<>();
//            string store1 = ProcessorStateManager.storeChangelogTopic("X", "store-1");
//            string store2 = ProcessorStateManager.storeChangelogTopic("X", "store-2");
//            string store3 = ProcessorStateManager.storeChangelogTopic("X", "store-3");
//            expectedTopicGroups.put(0, new InternalTopologyBuilder.TopicsInfo(
//                Collections.emptySet(), mkSet("topic-1", "topic-1x", "topic-2"),
//                Collections.emptyMap(),
//                Collections.singletonMap(store1, new UnwindowedChangelogTopicConfig(store1, Collections.emptyMap()))));
//            expectedTopicGroups.put(1, new InternalTopologyBuilder.TopicsInfo(
//                Collections.emptySet(), mkSet("topic-3", "topic-4"),
//                Collections.emptyMap(),
//                Collections.singletonMap(store2, new UnwindowedChangelogTopicConfig(store2, Collections.emptyMap()))));
//            expectedTopicGroups.put(2, new InternalTopologyBuilder.TopicsInfo(
//                Collections.emptySet(), mkSet("topic-5"),
//                Collections.emptyMap(),
//                Collections.singletonMap(store3, new UnwindowedChangelogTopicConfig(store3, Collections.emptyMap()))));

//            Assert.Equal(3, topicGroups.Count);
//            Assert.Equal(expectedTopicGroups, topicGroups);
//        }

//        [Xunit.Fact]
//        public void TestBuild()
//        {
//            builder.AddSource(null, "source-1", null, null, null, "topic-1", "topic-1x");
//            builder.AddSource(null, "source-2", null, null, null, "topic-2");
//            builder.AddSource(null, "source-3", null, null, null, "topic-3");
//            builder.AddSource(null, "source-4", null, null, null, "topic-4");
//            builder.AddSource(null, "source-5", null, null, null, "topic-5");

//            builder.AddProcessor("processor-1", new MockProcessorSupplier(), "source-1");
//            builder.AddProcessor("processor-2", new MockProcessorSupplier(), "source-2", "processor-1");
//            builder.AddProcessor("processor-3", new MockProcessorSupplier(), "source-3", "source-4");

//            builder.setApplicationId("X");
//            ProcessorTopology topology0 = builder.Build(0);
//            ProcessorTopology topology1 = builder.Build(1);
//            ProcessorTopology topology2 = builder.Build(2);

//            Assert.Equal(mkSet("source-1", "source-2", "processor-1", "processor-2"), nodeNames(topology0.processors()));
//            Assert.Equal(mkSet("source-3", "source-4", "processor-3"), nodeNames(topology1.processors()));
//            Assert.Equal(mkSet("source-5"), nodeNames(topology2.processors()));
//        }

//        [Xunit.Fact]
//        public void ShouldAllowIncrementalBuilds()
//        {
//            Dictionary<int, HashSet<string>> oldNodeGroups, newNodeGroups;

//            oldNodeGroups = builder.nodeGroups();
//            builder.AddSource(null, "source-1", null, null, null, "topic-1");
//            builder.AddSource(null, "source-2", null, null, null, "topic-2");
//            newNodeGroups = builder.nodeGroups();
//            Assert.NotEqual(oldNodeGroups, newNodeGroups);

//            oldNodeGroups = newNodeGroups;
//            builder.AddSource(null, "source-3", null, null, null, new Regex("", RegexOptions.Compiled));
//            builder.AddSource(null, "source-4", null, null, null, new Regex("", RegexOptions.Compiled));
//            newNodeGroups = builder.nodeGroups();
//            Assert.NotEqual(oldNodeGroups, newNodeGroups);

//            oldNodeGroups = newNodeGroups;
//            builder.AddProcessor("processor-1", new MockProcessorSupplier(), "source-1");
//            builder.AddProcessor("processor-2", new MockProcessorSupplier(), "source-2");
//            builder.AddProcessor("processor-3", new MockProcessorSupplier(), "source-3");
//            newNodeGroups = builder.nodeGroups();
//            Assert.NotEqual(oldNodeGroups, newNodeGroups);

//            oldNodeGroups = newNodeGroups;
//            builder.AddSink("sink-1", "sink-topic", null, null, null, "processor-1");
//            newNodeGroups = builder.nodeGroups();
//            Assert.NotEqual(oldNodeGroups, newNodeGroups);

//            oldNodeGroups = newNodeGroups;
//            builder.AddSink("sink-2", (k, v, ctx) => "sink-topic", null, null, null, "processor-2");
//            newNodeGroups = builder.nodeGroups();
//            Assert.NotEqual(oldNodeGroups, newNodeGroups);

//            oldNodeGroups = newNodeGroups;
//            builder.addStateStore(new MockKeyValueStoreBuilder("store-1", false), "processor-1", "processor-2");
//            newNodeGroups = builder.nodeGroups();
//            Assert.NotEqual(oldNodeGroups, newNodeGroups);

//            oldNodeGroups = newNodeGroups;
//            builder.addStateStore(new MockKeyValueStoreBuilder("store-2", false));
//            builder.connectProcessorAndStateStores("processor-2", "store-2");
//            builder.connectProcessorAndStateStores("processor-3", "store-2");
//            newNodeGroups = builder.nodeGroups();
//            Assert.NotEqual(oldNodeGroups, newNodeGroups);

//            oldNodeGroups = newNodeGroups;
//            builder.addGlobalStore(new MockKeyValueStoreBuilder("global-store", false).withLoggingDisabled(), "globalSource", null, null, null, "globalTopic", "global-processor", new MockProcessorSupplier());
//            newNodeGroups = builder.nodeGroups();
//            Assert.NotEqual(oldNodeGroups, newNodeGroups);
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldNotAllowNullNameWhenAddingSink()
//        {
//            builder.AddSink(null, "topic", null, null, null);
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldNotAllowNullTopicWhenAddingSink()
//        {
//            builder.AddSink("name", (string)null, null, null, null);
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldNotAllowNullTopicChooserWhenAddingSink()
//        {
//            builder.AddSink("name", (TopicNameExtractor<object, object>)null, null, null, null);
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldNotAllowNullNameWhenAddingProcessor()
//        {
//            builder.AddProcessor(null, () => null);
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldNotAllowNullProcessorSupplier()
//        {
//            builder.AddProcessor("name", null);
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldNotAllowNullNameWhenAddingSource()
//        {
//            builder.AddSource(null, null, null, null, null, new Regex(".*", RegexOptions.Compiled));
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldNotAllowNullProcessorNameWhenConnectingProcessorAndStateStores()
//        {
//            builder.connectProcessorAndStateStores(null, "store");
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldNotAllowNullStateStoreNameWhenConnectingProcessorAndStateStores()
//        {
//            builder.connectProcessorAndStateStores("processor", new string[] { null });
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldNotAddNullInternalTopic()
//        {
//            builder.AddInternalTopic(null);
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldNotSetApplicationIdToNull()
//        {
//            builder.setApplicationId(null);
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldNotAddNullStateStoreSupplier()
//        {
//            builder.addStateStore(null);
//        }

//        private HashSet<string> NodeNames(Collection<ProcessorNode> nodes)
//        {
//            HashSet<string> nodeNames = new HashSet<>();
//            foreach (ProcessorNode node in nodes)
//            {
//                nodeNames.Add(node.name());
//            }
//            return nodeNames;
//        }

//        [Xunit.Fact]
//        public void ShouldAssociateStateStoreNameWhenStateStoreSupplierIsInternal()
//        {
//            builder.AddSource(null, "source", null, null, null, "topic");
//            builder.AddProcessor("processor", new MockProcessorSupplier(), "source");
//            builder.addStateStore(storeBuilder, "processor");
//            Dictionary<string, List<string>> stateStoreNameToSourceTopic = builder.stateStoreNameToSourceTopics();
//            Assert.Equal(1, stateStoreNameToSourceTopic.Count);
//            Assert.Equal(Collections.singletonList("topic"), stateStoreNameToSourceTopic.Get("store"));
//        }

//        [Xunit.Fact]
//        public void ShouldAssociateStateStoreNameWhenStateStoreSupplierIsExternal()
//        {
//            builder.AddSource(null, "source", null, null, null, "topic");
//            builder.AddProcessor("processor", new MockProcessorSupplier(), "source");
//            builder.addStateStore(storeBuilder, "processor");
//            Dictionary<string, List<string>> stateStoreNameToSourceTopic = builder.stateStoreNameToSourceTopics();
//            Assert.Equal(1, stateStoreNameToSourceTopic.Count);
//            Assert.Equal(Collections.singletonList("topic"), stateStoreNameToSourceTopic.Get("store"));
//        }

//        [Xunit.Fact]
//        public void ShouldCorrectlyMapStateStoreToInternalTopics()
//        {
//            builder.setApplicationId("appId");
//            builder.AddInternalTopic("internal-topic");
//            builder.AddSource(null, "source", null, null, null, "internal-topic");
//            builder.AddProcessor("processor", new MockProcessorSupplier(), "source");
//            builder.addStateStore(storeBuilder, "processor");
//            Dictionary<string, List<string>> stateStoreNameToSourceTopic = builder.stateStoreNameToSourceTopics();
//            Assert.Single(stateStoreNameToSourceTopic);
//            Assert.Equal(Collections.singletonList("appId-internal-topic"), stateStoreNameToSourceTopic.Get("store"));
//        }

//        [Xunit.Fact]
//        public void ShouldAddInternalTopicConfigForWindowStores()
//        {
//            builder.setApplicationId("appId");
//            builder.AddSource(null, "source", null, null, null, "topic");
//            builder.AddProcessor("processor", new MockProcessorSupplier(), "source");
//            builder.addStateStore(
//                Stores.windowStoreBuilder(
//                    Stores.PersistentWindowStore("store1", ofSeconds(30L), ofSeconds(10L), false),
//                    Serdes.String(),
//                    Serdes.String()
//                ),
//                "processor"
//            );
//            builder.addStateStore(
//                    Stores.sessionStoreBuilder(
//                            Stores.PersistentSessionStore("store2", ofSeconds(30)), Serdes.String(), Serdes.String()
//                    ),
//                    "processor"
//            );
//            Dictionary<int, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();
//            InternalTopologyBuilder.TopicsInfo topicsInfo = topicGroups.values().iterator().MoveNext();
//            InternalTopicConfig topicConfig1 = topicsInfo.stateChangelogTopics.Get("appId-store1-changelog");
//            Dictionary<string, string> properties1 = topicConfig1.getProperties(Collections.emptyMap(), 10000);
//            Assert.Equal(2, properties1.Count);
//            Assert.Equal(TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE, properties1.Get(TopicConfig.CLEANUP_POLICY_CONFIG));
//            Assert.Equal("40000", properties1.Get(TopicConfig.RETENTION_MS_CONFIG));
//            Assert.Equal("appId-store1-changelog", topicConfig1.name());
//            Assert.True(topicConfig1 is WindowedChangelogTopicConfig);
//            InternalTopicConfig topicConfig2 = topicsInfo.stateChangelogTopics.Get("appId-store2-changelog");
//            Dictionary<string, string> properties2 = topicConfig2.getProperties(Collections.emptyMap(), 10000);
//            Assert.Equal(2, properties2.Count);
//            Assert.Equal(TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE, properties2.Get(TopicConfig.CLEANUP_POLICY_CONFIG));
//            Assert.Equal("40000", properties2.Get(TopicConfig.RETENTION_MS_CONFIG));
//            Assert.Equal("appId-store2-changelog", topicConfig2.name());
//            Assert.True(topicConfig2 is WindowedChangelogTopicConfig);
//        }

//        [Xunit.Fact]
//        public void ShouldAddInternalTopicConfigForNonWindowStores()
//        {
//            builder.SetApplicationId("appId");
//            builder.AddSource(null, "source", null, null, null, "topic");
//            builder.AddProcessor("processor", new MockProcessorSupplier(), "source");
//            builder.AddStateStore(storeBuilder, "processor");
//            Dictionary<int, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();
//            InternalTopologyBuilder.TopicsInfo topicsInfo = topicGroups.values().iterator().MoveNext();
//            InternalTopicConfig topicConfig = topicsInfo.stateChangelogTopics.Get("appId-store-changelog");
//            Dictionary<string, string> properties = topicConfig.getProperties(Collections.emptyMap(), 10000);
//            Assert.Equal(1, properties.Count);
//            Assert.Equal(TopicConfig.CLEANUP_POLICY_COMPACT, properties.Get(TopicConfig.CLEANUP_POLICY_CONFIG));
//            Assert.Equal("appId-store-changelog", topicConfig.name());
//            Assert.True(topicConfig is UnwindowedChangelogTopicConfig);
//        }

//        [Xunit.Fact]
//        public void ShouldAddInternalTopicConfigForRepartitionTopics()
//        {
//            builder.setApplicationId("appId");
//            builder.AddInternalTopic("foo");
//            builder.AddSource(null, "source", null, null, null, "foo");
//            InternalTopologyBuilder.TopicsInfo topicsInfo = builder.topicGroups().values().iterator().MoveNext();
//            InternalTopicConfig topicConfig = topicsInfo.repartitionSourceTopics.Get("appId-foo");
//            Dictionary<string, string> properties = topicConfig.getProperties(Collections.emptyMap(), 10000);
//            Assert.Equal(3, properties.Count);
//            Assert.Equal(string.valueOf(-1), properties.Get(TopicConfig.RETENTION_MS_CONFIG));
//            Assert.Equal(TopicConfig.CLEANUP_POLICY_DELETE, properties.Get(TopicConfig.CLEANUP_POLICY_CONFIG));
//            Assert.Equal("appId-foo", topicConfig.name());
//            Assert.True(topicConfig is RepartitionTopicConfig);
//        }


//        [Xunit.Fact]
//        public void ShouldSetCorrectSourceNodesWithRegexUpdatedTopics()
//        {// throws Exception
//            builder.AddSource(null, "source-1", null, null, null, "topic-foo");
//            builder.AddSource(null, "source-2", null, null, null, new Regex("topic-[A-C]", RegexOptions.Compiled));
//            builder.AddSource(null, "source-3", null, null, null, new Regex("topic-\\d", RegexOptions.Compiled));

//            InternalTopologyBuilder.SubscriptionUpdates subscriptionUpdates = new InternalTopologyBuilder.SubscriptionUpdates();
//            Field updatedTopicsField = subscriptionUpdates.getClass().getDeclaredField("updatedTopicSubscriptions");
//            updatedTopicsField.setAccessible(true);

//            HashSet<string> updatedTopics = (HashSet<string>)updatedTopicsField.Get(subscriptionUpdates);

//            updatedTopics.Add("topic-B");
//            updatedTopics.Add("topic-3");
//            updatedTopics.Add("topic-A");

//            builder.updateSubscriptions(subscriptionUpdates, null);
//            builder.setApplicationId("test-id");

//            Dictionary<int, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();
//            Assert.True(topicGroups.Get(0).sourceTopics.Contains("topic-foo"));
//            Assert.True(topicGroups.Get(1).sourceTopics.Contains("topic-A"));
//            Assert.True(topicGroups.Get(1).sourceTopics.Contains("topic-B"));
//            Assert.True(topicGroups.Get(2).sourceTopics.Contains("topic-3"));

//        }

//        [Xunit.Fact]
//        public void ShouldAddTimestampExtractorPerSource()
//        {
//            builder.AddSource(null, "source", new MockTimestampExtractor(), null, null, "topic");
//            ProcessorTopology processorTopology = builder.rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig())).Build(null);
//            Assert.Equal(processorTopology.source("topic").getTimestampExtractor(), instanceOf(MockTimestampExtractor));
//        }

//        [Xunit.Fact]
//        public void ShouldAddTimestampExtractorWithPatternPerSource()
//        {
//            var pattern = new Regex("t.*", RegexOptions.Compiled);
//            builder.AddSource(null, "source", new MockTimestampExtractor(), null, null, pattern);
//            ProcessorTopology processorTopology = builder.rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig())).Build(null);
//            Assert.Equal(processorTopology.Source(pattern.pattern()).getTimestampExtractor(), instanceOf(MockTimestampExtractor));
//        }

//        [Xunit.Fact]
//        public void ShouldSortProcessorNodesCorrectly()
//        {
//            builder.AddSource(null, "source1", null, null, null, "topic1");
//            builder.AddSource(null, "source2", null, null, null, "topic2");
//            builder.AddProcessor("processor1", new MockProcessorSupplier(), "source1");
//            builder.AddProcessor("processor2", new MockProcessorSupplier(), "source1", "source2");
//            builder.AddProcessor("processor3", new MockProcessorSupplier(), "processor2");
//            builder.AddSink("sink1", "topic2", null, null, null, "processor1", "processor3");

//            Assert.Equal(1, builder.describe().subtopologies().Count);

//            Iterator<TopologyDescription.Node> iterator = ((InternalTopologyBuilder.Subtopology)builder.describe().subtopologies().iterator().MoveNext()).nodesInOrder();

//            Assert.True(iterator.hasNext());
//            InternalTopologyBuilder.AbstractNode node = (InternalTopologyBuilder.AbstractNode)iterator.MoveNext();
//            Assert.Equal("source1", node.name);
//            Assert.Equal(6, node.size);

//            Assert.True(iterator.hasNext());
//            node = (InternalTopologyBuilder.AbstractNode)iterator.MoveNext();
//            Assert.Equal("source2", node.name);
//            Assert.Equal(4, node.size);

//            Assert.True(iterator.hasNext());
//            node = (InternalTopologyBuilder.AbstractNode)iterator.MoveNext();
//            Assert.Equal("processor2", node.name);
//            Assert.Equal(3, node.size);

//            Assert.True(iterator.hasNext());
//            node = (InternalTopologyBuilder.AbstractNode)iterator.MoveNext();
//            Assert.Equal("processor1", node.name);
//            Assert.Equal(2, node.size);

//            Assert.True(iterator.hasNext());
//            node = (InternalTopologyBuilder.AbstractNode)iterator.MoveNext();
//            Assert.Equal("processor3", node.name);
//            Assert.Equal(2, node.size);

//            Assert.True(iterator.hasNext());
//            node = (InternalTopologyBuilder.AbstractNode)iterator.MoveNext();
//            Assert.Equal("sink1", node.name);
//            Assert.Equal(1, node.size);
//        }


//        [Xunit.Fact]
//        public void ShouldConnectRegexMatchedTopicsToStateStore()
//        {// throws Exception
//            builder.AddSource(null, "ingest", null, null, null, new Regex("topic-\\d+", RegexOptions.Compiled));
//            builder.AddProcessor("my-processor", new MockProcessorSupplier(), "ingest");
//            builder.AddStateStore(storeBuilder, "my-processor");

//            InternalTopologyBuilder.SubscriptionUpdates subscriptionUpdates = new InternalTopologyBuilder.SubscriptionUpdates();
//            Field updatedTopicsField = subscriptionUpdates.getClass().getDeclaredField("updatedTopicSubscriptions");
//            updatedTopicsField.setAccessible(true);

//            HashSet<string> updatedTopics = (HashSet<string>)updatedTopicsField.Get(subscriptionUpdates);

//            updatedTopics.Add("topic-2");
//            updatedTopics.Add("topic-3");
//            updatedTopics.Add("topic-A");

//            builder.UpdateSubscriptions(subscriptionUpdates, "test-thread");
//            builder.SetApplicationId("test-app");

//            Dictionary<string, List<string>> stateStoreAndTopics = builder.stateStoreNameToSourceTopics();
//            List<string> topics = stateStoreAndTopics.Get(storeBuilder.name());

//            Assert.Equal("Expected to contain two topics", 2, topics.Count);

//            Assert.True(topics.Contains("topic-2"));
//            Assert.True(topics.Contains("topic-3"));
//            Assert.False(topics.Contains("topic-A"));
//        }


//        [Xunit.Fact]// (expected = TopologyException)
//        public void ShouldNotAllowToAddGlobalStoreWithSourceNameEqualsProcessorName()
//        {
//            string sameNameForSourceAndProcessor = "sameName";
//            builder.addGlobalStore(
//                (IStoreBuilder<IKeyValueStore>)storeBuilder,
//                sameNameForSourceAndProcessor,
//                null,
//                null,
//                null,
//                "anyTopicName",
//                sameNameForSourceAndProcessor,
//                new MockProcessorSupplier());
//        }

//        [Xunit.Fact]
//        public void ShouldThrowIfNameIsNull()
//        {
//            Exception e = Assert.Throws<NullReferenceException>(() => new InternalTopologyBuilder.Source(null, Collections.emptySet(), null));
//            Assert.Equal("name cannot be null", e.ToString());
//        }

//        [Xunit.Fact]
//        public void ShouldThrowIfTopicAndPatternAreNull()
//        {
//            Exception e = Assert.Throws(ArgumentException, () => new InternalTopologyBuilder.Source("name", null, null));
//            Assert.Equal("Either topics or pattern must be not-null, but both are null.", e.ToString());
//        }

//        [Xunit.Fact]
//        public void ShouldThrowIfBothTopicAndPatternAreNotNull()
//        {
//            Exception e = Assert.Throws(ArgumentException, () => new InternalTopologyBuilder.Source("name", Collections.emptySet(), new Regex("", RegexOptions.Compiled)));
//            Assert.Equal("Either topics or pattern must be null, but both are not null.", e.ToString());
//        }

//        [Xunit.Fact]
//        public void SourceShouldBeEqualIfNameAndTopicListAreTheSame()
//        {
//            InternalTopologyBuilder.Source @base = new InternalTopologyBuilder.Source("name", Collections.singleton("topic"), null);
//            InternalTopologyBuilder.Source sameAsBase = new InternalTopologyBuilder.Source("name", Collections.singleton("topic"), null);

//            Assert.Equal(@base, (sameAsBase));
//        }

//        [Xunit.Fact]
//        public void SourceShouldBeEqualIfNameAndPatternAreTheSame()
//        {
//            InternalTopologyBuilder.Source base = new InternalTopologyBuilder.Source("name", null, new Regex("topic", RegexOptions.Compiled));
//            InternalTopologyBuilder.Source sameAsBase = new InternalTopologyBuilder.Source("name", null, new Regex("topic", RegexOptions.Compiled));

//            Assert.Equal(base, (sameAsBase));
//        }

//        [Xunit.Fact]
//        public void SourceShouldNotBeEqualForDifferentNamesWithSameTopicList()
//        {
//            InternalTopologyBuilder.Source base = new InternalTopologyBuilder.Source("name", Collections.singleton("topic"), null);
//            InternalTopologyBuilder.Source differentName = new InternalTopologyBuilder.Source("name2", Collections.singleton("topic"), null);

//            Assert.Equal(base, not(equalTo(differentName)));
//        }

//        [Xunit.Fact]
//        public void SourceShouldNotBeEqualForDifferentNamesWithSamePattern()
//        {
//            InternalTopologyBuilder.Source base = new InternalTopologyBuilder.Source("name", null, new Regex("topic", RegexOptions.Compiled));
//            InternalTopologyBuilder.Source differentName = new InternalTopologyBuilder.Source("name2", null, new Regex("topic", RegexOptions.Compiled));

//            Assert.Equal(base, not(equalTo(differentName)));
//        }

//        [Xunit.Fact]
//        public void SourceShouldNotBeEqualForDifferentTopicList()
//        {
//            InternalTopologyBuilder.Source base = new InternalTopologyBuilder.Source("name", Collections.singleton("topic"), null);
//            InternalTopologyBuilder.Source differentTopicList = new InternalTopologyBuilder.Source("name", Collections.emptySet(), null);
//            InternalTopologyBuilder.Source differentTopic = new InternalTopologyBuilder.Source("name", Collections.singleton("topic2"), null);

//            Assert.Equal(base, not(equalTo(differentTopicList)));
//            Assert.Equal(base, not(equalTo(differentTopic)));
//        }

//        [Xunit.Fact]
//        public void SourceShouldNotBeEqualForDifferentPattern()
//        {
//            InternalTopologyBuilder.Source base = new InternalTopologyBuilder.Source("name", null, new Regex("topic", RegexOptions.Compiled));
//            InternalTopologyBuilder.Source differentPattern = new InternalTopologyBuilder.Source("name", null, new Regex("topic2", RegexOptions.Compiled));
//            InternalTopologyBuilder.Source overlappingPattern = new InternalTopologyBuilder.Source("name", null, new Regex("top*", RegexOptions.Compiled));

//            Assert.Equal(base, not(equalTo(differentPattern)));
//            Assert.Equal(base, not(equalTo(overlappingPattern)));
//        }
//    }
//}
///*






//*

//*





//*/













































