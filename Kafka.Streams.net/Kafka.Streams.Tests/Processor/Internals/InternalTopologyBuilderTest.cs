/*






 *

 *





 */













































public class InternalTopologyBuilderTest {

    private Serde<string> stringSerde = Serdes.String();
    private InternalTopologyBuilder builder = new InternalTopologyBuilder();
    private StoreBuilder storeBuilder = new MockKeyValueStoreBuilder("store", false);

    [Xunit.Fact]
    public void ShouldAddSourceWithOffsetReset() {
        string earliestTopic = "earliestTopic";
        string latestTopic = "latestTopic";

        builder.addSource(Topology.AutoOffsetReset.EARLIEST, "source", null, null, null, earliestTopic);
        builder.addSource(Topology.AutoOffsetReset.LATEST, "source2", null, null, null, latestTopic);

        Assert.True(builder.earliestResetTopicsPattern().matcher(earliestTopic).matches());
        Assert.True(builder.latestResetTopicsPattern().matcher(latestTopic).matches());
    }

    [Xunit.Fact]
    public void ShouldAddSourcePatternWithOffsetReset() {
        string earliestTopicPattern = "earliest.*Topic";
        string latestTopicPattern = "latest.*Topic";

        builder.addSource(Topology.AutoOffsetReset.EARLIEST, "source", null, null, null, new Regex(earliestTopicPattern, RegexOptions.Compiled));
        builder.addSource(Topology.AutoOffsetReset.LATEST, "source2", null, null, null,  new Regex(latestTopicPattern, RegexOptions.Compiled));

        Assert.True(builder.earliestResetTopicsPattern().matcher("earliestTestTopic").matches());
        Assert.True(builder.latestResetTopicsPattern().matcher("latestTestTopic").matches());
    }

    [Xunit.Fact]
    public void ShouldAddSourceWithoutOffsetReset() {
        Pattern expectedPattern = new Regex("test-topic", RegexOptions.Compiled);

        builder.addSource(null, "source", null, stringSerde.deserializer(), stringSerde.deserializer(), "test-topic");

        Assert.Equal(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
        Assert.Equal(builder.earliestResetTopicsPattern().pattern(), "");
        Assert.Equal(builder.latestResetTopicsPattern().pattern(), "");
    }

    [Xunit.Fact]
    public void ShouldAddPatternSourceWithoutOffsetReset() {
        Pattern expectedPattern = new Regex("test-.*", RegexOptions.Compiled);

        builder.addSource(null, "source", null, stringSerde.deserializer(), stringSerde.deserializer(), new Regex("test-.*", RegexOptions.Compiled));

        Assert.Equal(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
        Assert.Equal(builder.earliestResetTopicsPattern().pattern(), "");
        Assert.Equal(builder.latestResetTopicsPattern().pattern(), "");
    }

    [Xunit.Fact]// (expected = TopologyException)
    public void ShouldNotAllowOffsetResetSourceWithoutTopics() {
        builder.addSource(Topology.AutoOffsetReset.EARLIEST, "source", null, stringSerde.deserializer(), stringSerde.deserializer());
    }

    [Xunit.Fact]
    public void ShouldNotAllowOffsetResetSourceWithDuplicateSourceName() {
        builder.addSource(Topology.AutoOffsetReset.EARLIEST, "source", null, stringSerde.deserializer(), stringSerde.deserializer(), "topic-1");
        try {
            builder.addSource(Topology.AutoOffsetReset.LATEST, "source", null, stringSerde.deserializer(), stringSerde.deserializer(), "topic-2");
            Assert.True(false, "Should throw TopologyException for duplicate source name");
        } catch (TopologyException expected) { /* ok */ }
    }

    [Xunit.Fact]
    public void TestAddSourceWithSameName() {
        builder.addSource(null, "source", null, null, null, "topic-1");
        try {
            builder.addSource(null, "source", null, null, null, "topic-2");
            Assert.True(false, "Should throw TopologyException with source name conflict");
        } catch (TopologyException expected) { /* ok */ }
    }

    [Xunit.Fact]
    public void TestAddSourceWithSameTopic() {
        builder.addSource(null, "source", null, null, null, "topic-1");
        try {
            builder.addSource(null, "source-2", null, null, null, "topic-1");
            Assert.True(false, "Should throw TopologyException with topic conflict");
        } catch (TopologyException expected) { /* ok */ }
    }

    [Xunit.Fact]
    public void TestAddProcessorWithSameName() {
        builder.addSource(null, "source", null, null, null, "topic-1");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        try {
            builder.addProcessor("processor", new MockProcessorSupplier(), "source");
            Assert.True(false, "Should throw TopologyException with processor name conflict");
        } catch (TopologyException expected) { /* ok */ }
    }

    [Xunit.Fact]// (expected = TopologyException)
    public void TestAddProcessorWithWrongParent() {
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
    }

    [Xunit.Fact]// (expected = TopologyException)
    public void TestAddProcessorWithSelfParent() {
        builder.addProcessor("processor", new MockProcessorSupplier(), "processor");
    }

    [Xunit.Fact]// (expected = TopologyException)
    public void TestAddProcessorWithEmptyParents() {
        builder.addProcessor("processor", new MockProcessorSupplier());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void TestAddProcessorWithNullParents() {
        builder.addProcessor("processor", new MockProcessorSupplier(), (string) null);
    }

    [Xunit.Fact]
    public void TestAddSinkWithSameName() {
        builder.addSource(null, "source", null, null, null, "topic-1");
        builder.addSink("sink", "topic-2", null, null, null, "source");
        try {
            builder.addSink("sink", "topic-3", null, null, null, "source");
            Assert.True(false, "Should throw TopologyException with sink name conflict");
        } catch (TopologyException expected) { /* ok */ }
    }

    [Xunit.Fact]// (expected = TopologyException)
    public void TestAddSinkWithWrongParent() {
        builder.addSink("sink", "topic-2", null, null, null, "source");
    }

    [Xunit.Fact]// (expected = TopologyException)
    public void TestAddSinkWithSelfParent() {
        builder.addSink("sink", "topic-2", null, null, null, "sink");
    }


    [Xunit.Fact]// (expected = TopologyException)
    public void TestAddSinkWithEmptyParents() {
        builder.addSink("sink", "topic", null, null, null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void TestAddSinkWithNullParents() {
        builder.addSink("sink", "topic", null, null, null, (string) null);
    }

    [Xunit.Fact]
    public void TestAddSinkConnectedWithParent() {
        builder.addSource(null, "source", null, null, null, "source-topic");
        builder.addSink("sink", "dest-topic", null, null, null, "source");

        Dictionary<int, HashSet<string>> nodeGroups = builder.nodeGroups();
        HashSet<string> nodeGroup = nodeGroups.get(0);

        Assert.True(nodeGroup.Contains("sink"));
        Assert.True(nodeGroup.Contains("source"));
    }

    [Xunit.Fact]
    public void TestAddSinkConnectedWithMultipleParent() {
        builder.addSource(null, "source", null, null, null, "source-topic");
        builder.addSource(null, "sourceII", null, null, null, "source-topicII");
        builder.addSink("sink", "dest-topic", null, null, null, "source", "sourceII");

        Dictionary<int, HashSet<string>> nodeGroups = builder.nodeGroups();
        HashSet<string> nodeGroup = nodeGroups.get(0);

        Assert.True(nodeGroup.Contains("sink"));
        Assert.True(nodeGroup.Contains("source"));
        Assert.True(nodeGroup.Contains("sourceII"));
    }

    [Xunit.Fact]
    public void TestSourceTopics() {
        builder.setApplicationId("X");
        builder.addSource(null, "source-1", null, null, null, "topic-1");
        builder.addSource(null, "source-2", null, null, null, "topic-2");
        builder.addSource(null, "source-3", null, null, null, "topic-3");
        builder.addInternalTopic("topic-3");

        Pattern expectedPattern = new Regex("X-topic-3|topic-1|topic-2", RegexOptions.Compiled);

        Assert.Equal(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
    }

    [Xunit.Fact]
    public void TestPatternSourceTopic() {
        Pattern expectedPattern = new Regex("topic-\\d", RegexOptions.Compiled);
        builder.addSource(null, "source-1", null, null, null, expectedPattern);
        Assert.Equal(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
    }

    [Xunit.Fact]
    public void TestAddMoreThanOnePatternSourceNode() {
        Pattern expectedPattern = new Regex("topics[A-Z]|.*-\\d", RegexOptions.Compiled);
        builder.addSource(null, "source-1", null, null, null, new Regex("topics[A-Z]", RegexOptions.Compiled));
        builder.addSource(null, "source-2", null, null, null, new Regex(".*-\\d", RegexOptions.Compiled));
        Assert.Equal(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
    }

    [Xunit.Fact]
    public void TestSubscribeTopicNameAndPattern() {
        Pattern expectedPattern = new Regex("topic-bar|topic-foo|.*-\\d", RegexOptions.Compiled);
        builder.addSource(null, "source-1", null, null, null, "topic-foo", "topic-bar");
        builder.addSource(null, "source-2", null, null, null, new Regex(".*-\\d", RegexOptions.Compiled));
        Assert.Equal(expectedPattern.pattern(), builder.sourceTopicPattern().pattern());
    }

    [Xunit.Fact]
    public void TestPatternMatchesAlreadyProvidedTopicSource() {
        builder.addSource(null, "source-1", null, null, null, "foo");
        try {
            builder.addSource(null, "source-2", null, null, null, new Regex("f.*", RegexOptions.Compiled));
            Assert.True(false, "Should throw TopologyException with topic name/pattern conflict");
        } catch (TopologyException expected) { /* ok */ }
    }

    [Xunit.Fact]
    public void TestNamedTopicMatchesAlreadyProvidedPattern() {
        builder.addSource(null, "source-1", null, null, null, new Regex("f.*", RegexOptions.Compiled));
        try {
            builder.addSource(null, "source-2", null, null, null, "foo");
            Assert.True(false, "Should throw TopologyException with topic name/pattern conflict");
        } catch (TopologyException expected) { /* ok */ }
    }

    [Xunit.Fact]// (expected = TopologyException)
    public void TestAddStateStoreWithNonExistingProcessor() {
        builder.addStateStore(storeBuilder, "no-such-processor");
    }

    [Xunit.Fact]
    public void TestAddStateStoreWithSource() {
        builder.addSource(null, "source-1", null, null, null, "topic-1");
        try {
            builder.addStateStore(storeBuilder, "source-1");
            Assert.True(false, "Should throw TopologyException with store cannot be added to source");
        } catch (TopologyException expected) { /* ok */ }
    }

    [Xunit.Fact]
    public void TestAddStateStoreWithSink() {
        builder.addSource(null, "source-1", null, null, null, "topic-1");
        builder.addSink("sink-1", "topic-1", null, null, null, "source-1");
        try {
            builder.addStateStore(storeBuilder, "sink-1");
            Assert.True(false, "Should throw TopologyException with store cannot be added to sink");
        } catch (TopologyException expected) { /* ok */ }
    }

    [Xunit.Fact]
    public void TestAddStateStoreWithDuplicates() {
        builder.addStateStore(storeBuilder);
        try {
            builder.addStateStore(storeBuilder);
            Assert.True(false, "Should throw TopologyException with store name conflict");
        } catch (TopologyException expected) { /* ok */ }
    }

    [Xunit.Fact]
    public void TestAddStateStore() {
        builder.addStateStore(storeBuilder);
        builder.setApplicationId("X");
        builder.addSource(null, "source-1", null, null, null, "topic-1");
        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source-1");

        Assert.Equal(0, builder.build(null).stateStores().Count);

        builder.connectProcessorAndStateStores("processor-1", storeBuilder.name());

        List<StateStore> suppliers = builder.build(null).stateStores();
        Assert.Equal(1, suppliers.Count);
        Assert.Equal(storeBuilder.name(), suppliers.get(0).name());
    }

    [Xunit.Fact]
    public void TestTopicGroups() {
        builder.setApplicationId("X");
        builder.addInternalTopic("topic-1x");
        builder.addSource(null, "source-1", null, null, null, "topic-1", "topic-1x");
        builder.addSource(null, "source-2", null, null, null, "topic-2");
        builder.addSource(null, "source-3", null, null, null, "topic-3");
        builder.addSource(null, "source-4", null, null, null, "topic-4");
        builder.addSource(null, "source-5", null, null, null, "topic-5");

        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source-1");

        builder.addProcessor("processor-2", new MockProcessorSupplier(), "source-2", "processor-1");
        builder.copartitionSources(asList("source-1", "source-2"));

        builder.addProcessor("processor-3", new MockProcessorSupplier(), "source-3", "source-4");

        Dictionary<int, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();

        Dictionary<int, InternalTopologyBuilder.TopicsInfo> expectedTopicGroups = new HashMap<>();
        expectedTopicGroups.put(0, new InternalTopologyBuilder.TopicsInfo(Collections.emptySet(), mkSet("topic-1", "X-topic-1x", "topic-2"), Collections.emptyMap(), Collections.emptyMap()));
        expectedTopicGroups.put(1, new InternalTopologyBuilder.TopicsInfo(Collections.emptySet(), mkSet("topic-3", "topic-4"), Collections.emptyMap(), Collections.emptyMap()));
        expectedTopicGroups.put(2, new InternalTopologyBuilder.TopicsInfo(Collections.emptySet(), mkSet("topic-5"), Collections.emptyMap(), Collections.emptyMap()));

        Assert.Equal(3, topicGroups.Count);
        Assert.Equal(expectedTopicGroups, topicGroups);

        Collection<HashSet<string>> copartitionGroups = builder.copartitionGroups();

        Assert.Equal(mkSet(mkSet("topic-1", "X-topic-1x", "topic-2")), new HashSet<>(copartitionGroups));
    }

    [Xunit.Fact]
    public void TestTopicGroupsByStateStore() {
        builder.setApplicationId("X");
        builder.addSource(null, "source-1", null, null, null, "topic-1", "topic-1x");
        builder.addSource(null, "source-2", null, null, null, "topic-2");
        builder.addSource(null, "source-3", null, null, null, "topic-3");
        builder.addSource(null, "source-4", null, null, null, "topic-4");
        builder.addSource(null, "source-5", null, null, null, "topic-5");

        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source-1");
        builder.addProcessor("processor-2", new MockProcessorSupplier(), "source-2");
        builder.addStateStore(new MockKeyValueStoreBuilder("store-1", false), "processor-1", "processor-2");

        builder.addProcessor("processor-3", new MockProcessorSupplier(), "source-3");
        builder.addProcessor("processor-4", new MockProcessorSupplier(), "source-4");
        builder.addStateStore(new MockKeyValueStoreBuilder("store-2", false), "processor-3", "processor-4");

        builder.addProcessor("processor-5", new MockProcessorSupplier(), "source-5");
        builder.addStateStore(new MockKeyValueStoreBuilder("store-3", false));
        builder.connectProcessorAndStateStores("processor-5", "store-3");

        Dictionary<int, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();

        Dictionary<int, InternalTopologyBuilder.TopicsInfo> expectedTopicGroups = new HashMap<>();
        string store1 = ProcessorStateManager.storeChangelogTopic("X", "store-1");
        string store2 = ProcessorStateManager.storeChangelogTopic("X", "store-2");
        string store3 = ProcessorStateManager.storeChangelogTopic("X", "store-3");
        expectedTopicGroups.put(0, new InternalTopologyBuilder.TopicsInfo(
            Collections.emptySet(), mkSet("topic-1", "topic-1x", "topic-2"),
            Collections.emptyMap(),
            Collections.singletonMap(store1, new UnwindowedChangelogTopicConfig(store1, Collections.emptyMap()))));
        expectedTopicGroups.put(1, new InternalTopologyBuilder.TopicsInfo(
            Collections.emptySet(), mkSet("topic-3", "topic-4"),
            Collections.emptyMap(),
            Collections.singletonMap(store2, new UnwindowedChangelogTopicConfig(store2, Collections.emptyMap()))));
        expectedTopicGroups.put(2, new InternalTopologyBuilder.TopicsInfo(
            Collections.emptySet(), mkSet("topic-5"),
            Collections.emptyMap(),
            Collections.singletonMap(store3, new UnwindowedChangelogTopicConfig(store3, Collections.emptyMap()))));

        Assert.Equal(3, topicGroups.Count);
        Assert.Equal(expectedTopicGroups, topicGroups);
    }

    [Xunit.Fact]
    public void TestBuild() {
        builder.addSource(null, "source-1", null, null, null, "topic-1", "topic-1x");
        builder.addSource(null, "source-2", null, null, null, "topic-2");
        builder.addSource(null, "source-3", null, null, null, "topic-3");
        builder.addSource(null, "source-4", null, null, null, "topic-4");
        builder.addSource(null, "source-5", null, null, null, "topic-5");

        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source-1");
        builder.addProcessor("processor-2", new MockProcessorSupplier(), "source-2", "processor-1");
        builder.addProcessor("processor-3", new MockProcessorSupplier(), "source-3", "source-4");

        builder.setApplicationId("X");
        ProcessorTopology topology0 = builder.build(0);
        ProcessorTopology topology1 = builder.build(1);
        ProcessorTopology topology2 = builder.build(2);

        Assert.Equal(mkSet("source-1", "source-2", "processor-1", "processor-2"), nodeNames(topology0.processors()));
        Assert.Equal(mkSet("source-3", "source-4", "processor-3"), nodeNames(topology1.processors()));
        Assert.Equal(mkSet("source-5"), nodeNames(topology2.processors()));
    }

    [Xunit.Fact]
    public void ShouldAllowIncrementalBuilds() {
        Dictionary<int, HashSet<string>> oldNodeGroups, newNodeGroups;

        oldNodeGroups = builder.nodeGroups();
        builder.addSource(null, "source-1", null, null, null, "topic-1");
        builder.addSource(null, "source-2", null, null, null, "topic-2");
        newNodeGroups = builder.nodeGroups();
        Assert.NotEqual(oldNodeGroups, newNodeGroups);

        oldNodeGroups = newNodeGroups;
        builder.addSource(null, "source-3", null, null, null, new Regex("", RegexOptions.Compiled));
        builder.addSource(null, "source-4", null, null, null, new Regex("", RegexOptions.Compiled));
        newNodeGroups = builder.nodeGroups();
        Assert.NotEqual(oldNodeGroups, newNodeGroups);

        oldNodeGroups = newNodeGroups;
        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source-1");
        builder.addProcessor("processor-2", new MockProcessorSupplier(), "source-2");
        builder.addProcessor("processor-3", new MockProcessorSupplier(), "source-3");
        newNodeGroups = builder.nodeGroups();
        Assert.NotEqual(oldNodeGroups, newNodeGroups);

        oldNodeGroups = newNodeGroups;
        builder.addSink("sink-1", "sink-topic", null, null, null, "processor-1");
        newNodeGroups = builder.nodeGroups();
        Assert.NotEqual(oldNodeGroups, newNodeGroups);

        oldNodeGroups = newNodeGroups;
        builder.addSink("sink-2", (k, v, ctx) => "sink-topic", null, null, null, "processor-2");
        newNodeGroups = builder.nodeGroups();
        Assert.NotEqual(oldNodeGroups, newNodeGroups);

        oldNodeGroups = newNodeGroups;
        builder.addStateStore(new MockKeyValueStoreBuilder("store-1", false), "processor-1", "processor-2");
        newNodeGroups = builder.nodeGroups();
        Assert.NotEqual(oldNodeGroups, newNodeGroups);

        oldNodeGroups = newNodeGroups;
        builder.addStateStore(new MockKeyValueStoreBuilder("store-2", false));
        builder.connectProcessorAndStateStores("processor-2", "store-2");
        builder.connectProcessorAndStateStores("processor-3", "store-2");
        newNodeGroups = builder.nodeGroups();
        Assert.NotEqual(oldNodeGroups, newNodeGroups);

        oldNodeGroups = newNodeGroups;
        builder.addGlobalStore(new MockKeyValueStoreBuilder("global-store", false).withLoggingDisabled(), "globalSource", null, null, null, "globalTopic", "global-processor", new MockProcessorSupplier());
        newNodeGroups = builder.nodeGroups();
        Assert.NotEqual(oldNodeGroups, newNodeGroups);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldNotAllowNullNameWhenAddingSink() {
        builder.addSink(null, "topic", null, null, null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldNotAllowNullTopicWhenAddingSink() {
        builder.addSink("name", (string) null, null, null, null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldNotAllowNullTopicChooserWhenAddingSink() {
        builder.addSink("name", (TopicNameExtractor<object, object>) null, null, null, null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldNotAllowNullNameWhenAddingProcessor() {
        builder.addProcessor(null, () => null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldNotAllowNullProcessorSupplier() {
        builder.addProcessor("name", null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldNotAllowNullNameWhenAddingSource() {
        builder.addSource(null, null, null, null, null, new Regex(".*", RegexOptions.Compiled));
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldNotAllowNullProcessorNameWhenConnectingProcessorAndStateStores() {
        builder.connectProcessorAndStateStores(null, "store");
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldNotAllowNullStateStoreNameWhenConnectingProcessorAndStateStores() {
        builder.connectProcessorAndStateStores("processor", new string[]{null});
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldNotAddNullInternalTopic() {
        builder.addInternalTopic(null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldNotSetApplicationIdToNull() {
        builder.setApplicationId(null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldNotAddNullStateStoreSupplier() {
        builder.addStateStore(null);
    }

    private HashSet<string> NodeNames(Collection<ProcessorNode> nodes) {
        HashSet<string> nodeNames = new HashSet<>();
        foreach (ProcessorNode node in nodes) {
            nodeNames.add(node.name());
        }
        return nodeNames;
    }

    [Xunit.Fact]
    public void ShouldAssociateStateStoreNameWhenStateStoreSupplierIsInternal() {
        builder.addSource(null, "source", null, null, null, "topic");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addStateStore(storeBuilder, "processor");
        Dictionary<string, List<string>> stateStoreNameToSourceTopic = builder.stateStoreNameToSourceTopics();
        Assert.Equal(1, stateStoreNameToSourceTopic.Count);
        Assert.Equal(Collections.singletonList("topic"), stateStoreNameToSourceTopic.get("store"));
    }

    [Xunit.Fact]
    public void ShouldAssociateStateStoreNameWhenStateStoreSupplierIsExternal() {
        builder.addSource(null, "source", null, null, null, "topic");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addStateStore(storeBuilder, "processor");
        Dictionary<string, List<string>> stateStoreNameToSourceTopic = builder.stateStoreNameToSourceTopics();
        Assert.Equal(1, stateStoreNameToSourceTopic.Count);
        Assert.Equal(Collections.singletonList("topic"), stateStoreNameToSourceTopic.get("store"));
    }

    [Xunit.Fact]
    public void ShouldCorrectlyMapStateStoreToInternalTopics() {
        builder.setApplicationId("appId");
        builder.addInternalTopic("internal-topic");
        builder.addSource(null, "source", null, null, null, "internal-topic");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addStateStore(storeBuilder, "processor");
        Dictionary<string, List<string>> stateStoreNameToSourceTopic = builder.stateStoreNameToSourceTopics();
        Assert.Equal(1, stateStoreNameToSourceTopic.Count);
        Assert.Equal(Collections.singletonList("appId-internal-topic"), stateStoreNameToSourceTopic.get("store"));
    }

    [Xunit.Fact]
    public void ShouldAddInternalTopicConfigForWindowStores() {
        builder.setApplicationId("appId");
        builder.addSource(null, "source", null, null, null, "topic");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addStateStore(
            Stores.windowStoreBuilder(
                Stores.persistentWindowStore("store1", ofSeconds(30L), ofSeconds(10L), false),
                Serdes.String(),
                Serdes.String()
            ),
            "processor"
        );
        builder.addStateStore(
                Stores.sessionStoreBuilder(
                        Stores.persistentSessionStore("store2", ofSeconds(30)), Serdes.String(), Serdes.String()
                ),
                "processor"
        );
        Dictionary<int, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();
        InternalTopologyBuilder.TopicsInfo topicsInfo = topicGroups.values().iterator().next();
        InternalTopicConfig topicConfig1 = topicsInfo.stateChangelogTopics.get("appId-store1-changelog");
        Dictionary<string, string> properties1 = topicConfig1.getProperties(Collections.emptyMap(), 10000);
        Assert.Equal(2, properties1.Count);
        Assert.Equal(TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE, properties1.get(TopicConfig.CLEANUP_POLICY_CONFIG));
        Assert.Equal("40000", properties1.get(TopicConfig.RETENTION_MS_CONFIG));
        Assert.Equal("appId-store1-changelog", topicConfig1.name());
        Assert.True(topicConfig1 is WindowedChangelogTopicConfig);
        InternalTopicConfig topicConfig2 = topicsInfo.stateChangelogTopics.get("appId-store2-changelog");
        Dictionary<string, string> properties2 = topicConfig2.getProperties(Collections.emptyMap(), 10000);
        Assert.Equal(2, properties2.Count);
        Assert.Equal(TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE, properties2.get(TopicConfig.CLEANUP_POLICY_CONFIG));
        Assert.Equal("40000", properties2.get(TopicConfig.RETENTION_MS_CONFIG));
        Assert.Equal("appId-store2-changelog", topicConfig2.name());
        Assert.True(topicConfig2 is WindowedChangelogTopicConfig);
    }

    [Xunit.Fact]
    public void ShouldAddInternalTopicConfigForNonWindowStores() {
        builder.setApplicationId("appId");
        builder.addSource(null, "source", null, null, null, "topic");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addStateStore(storeBuilder, "processor");
        Dictionary<int, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();
        InternalTopologyBuilder.TopicsInfo topicsInfo = topicGroups.values().iterator().next();
        InternalTopicConfig topicConfig = topicsInfo.stateChangelogTopics.get("appId-store-changelog");
        Dictionary<string, string> properties = topicConfig.getProperties(Collections.emptyMap(), 10000);
        Assert.Equal(1, properties.Count);
        Assert.Equal(TopicConfig.CLEANUP_POLICY_COMPACT, properties.get(TopicConfig.CLEANUP_POLICY_CONFIG));
        Assert.Equal("appId-store-changelog", topicConfig.name());
        Assert.True(topicConfig is UnwindowedChangelogTopicConfig);
    }

    [Xunit.Fact]
    public void ShouldAddInternalTopicConfigForRepartitionTopics() {
        builder.setApplicationId("appId");
        builder.addInternalTopic("foo");
        builder.addSource(null, "source", null, null, null, "foo");
        InternalTopologyBuilder.TopicsInfo topicsInfo = builder.topicGroups().values().iterator().next();
        InternalTopicConfig topicConfig = topicsInfo.repartitionSourceTopics.get("appId-foo");
        Dictionary<string, string> properties = topicConfig.getProperties(Collections.emptyMap(), 10000);
        Assert.Equal(3, properties.Count);
        Assert.Equal(string.valueOf(-1), properties.get(TopicConfig.RETENTION_MS_CONFIG));
        Assert.Equal(TopicConfig.CLEANUP_POLICY_DELETE, properties.get(TopicConfig.CLEANUP_POLICY_CONFIG));
        Assert.Equal("appId-foo", topicConfig.name());
        Assert.True(topicConfig is RepartitionTopicConfig);
    }

    
    [Xunit.Fact]
    public void ShouldSetCorrectSourceNodesWithRegexUpdatedTopics() {// throws Exception
        builder.addSource(null, "source-1", null, null, null, "topic-foo");
        builder.addSource(null, "source-2", null, null, null, new Regex("topic-[A-C]", RegexOptions.Compiled));
        builder.addSource(null, "source-3", null, null, null, new Regex("topic-\\d", RegexOptions.Compiled));

        InternalTopologyBuilder.SubscriptionUpdates subscriptionUpdates = new InternalTopologyBuilder.SubscriptionUpdates();
        Field updatedTopicsField  = subscriptionUpdates.getClass().getDeclaredField("updatedTopicSubscriptions");
        updatedTopicsField.setAccessible(true);

        HashSet<string> updatedTopics = (HashSet<string>) updatedTopicsField.get(subscriptionUpdates);

        updatedTopics.add("topic-B");
        updatedTopics.add("topic-3");
        updatedTopics.add("topic-A");

        builder.updateSubscriptions(subscriptionUpdates, null);
        builder.setApplicationId("test-id");

        Dictionary<int, InternalTopologyBuilder.TopicsInfo> topicGroups = builder.topicGroups();
        Assert.True(topicGroups.get(0).sourceTopics.Contains("topic-foo"));
        Assert.True(topicGroups.get(1).sourceTopics.Contains("topic-A"));
        Assert.True(topicGroups.get(1).sourceTopics.Contains("topic-B"));
        Assert.True(topicGroups.get(2).sourceTopics.Contains("topic-3"));

    }

    [Xunit.Fact]
    public void ShouldAddTimestampExtractorPerSource() {
        builder.addSource(null, "source", new MockTimestampExtractor(), null, null, "topic");
        ProcessorTopology processorTopology = builder.rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig())).build(null);
        Assert.Equal(processorTopology.source("topic").getTimestampExtractor(), instanceOf(MockTimestampExtractor));
    }

    [Xunit.Fact]
    public void ShouldAddTimestampExtractorWithPatternPerSource() {
        Pattern pattern = new Regex("t.*", RegexOptions.Compiled);
        builder.addSource(null, "source", new MockTimestampExtractor(), null, null, pattern);
        ProcessorTopology processorTopology = builder.rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig())).build(null);
        Assert.Equal(processorTopology.source(pattern.pattern()).getTimestampExtractor(), instanceOf(MockTimestampExtractor));
    }

    [Xunit.Fact]
    public void ShouldSortProcessorNodesCorrectly() {
        builder.addSource(null, "source1", null, null, null, "topic1");
        builder.addSource(null, "source2", null, null, null, "topic2");
        builder.addProcessor("processor1", new MockProcessorSupplier(), "source1");
        builder.addProcessor("processor2", new MockProcessorSupplier(), "source1", "source2");
        builder.addProcessor("processor3", new MockProcessorSupplier(), "processor2");
        builder.addSink("sink1", "topic2", null, null, null, "processor1", "processor3");

        Assert.Equal(1, builder.describe().subtopologies().Count);

        Iterator<TopologyDescription.Node> iterator = ((InternalTopologyBuilder.Subtopology) builder.describe().subtopologies().iterator().next()).nodesInOrder();

        Assert.True(iterator.hasNext());
        InternalTopologyBuilder.AbstractNode node = (InternalTopologyBuilder.AbstractNode) iterator.next();
        Assert.Equal("source1", node.name);
        Assert.Equal(6, node.size);

        Assert.True(iterator.hasNext());
        node = (InternalTopologyBuilder.AbstractNode) iterator.next();
        Assert.Equal("source2", node.name);
        Assert.Equal(4, node.size);

        Assert.True(iterator.hasNext());
        node = (InternalTopologyBuilder.AbstractNode) iterator.next();
        Assert.Equal("processor2", node.name);
        Assert.Equal(3, node.size);

        Assert.True(iterator.hasNext());
        node = (InternalTopologyBuilder.AbstractNode) iterator.next();
        Assert.Equal("processor1", node.name);
        Assert.Equal(2, node.size);

        Assert.True(iterator.hasNext());
        node = (InternalTopologyBuilder.AbstractNode) iterator.next();
        Assert.Equal("processor3", node.name);
        Assert.Equal(2, node.size);

        Assert.True(iterator.hasNext());
        node = (InternalTopologyBuilder.AbstractNode) iterator.next();
        Assert.Equal("sink1", node.name);
        Assert.Equal(1, node.size);
    }

    
    [Xunit.Fact]
    public void ShouldConnectRegexMatchedTopicsToStateStore() {// throws Exception
        builder.addSource(null, "ingest", null, null, null, new Regex("topic-\\d+", RegexOptions.Compiled));
        builder.addProcessor("my-processor", new MockProcessorSupplier(), "ingest");
        builder.addStateStore(storeBuilder, "my-processor");

        InternalTopologyBuilder.SubscriptionUpdates subscriptionUpdates = new InternalTopologyBuilder.SubscriptionUpdates();
        Field updatedTopicsField  = subscriptionUpdates.getClass().getDeclaredField("updatedTopicSubscriptions");
        updatedTopicsField.setAccessible(true);

        HashSet<string> updatedTopics = (HashSet<string>) updatedTopicsField.get(subscriptionUpdates);

        updatedTopics.add("topic-2");
        updatedTopics.add("topic-3");
        updatedTopics.add("topic-A");

        builder.updateSubscriptions(subscriptionUpdates, "test-thread");
        builder.setApplicationId("test-app");

        Dictionary<string, List<string>> stateStoreAndTopics = builder.stateStoreNameToSourceTopics();
        List<string> topics = stateStoreAndTopics.get(storeBuilder.name());

        Assert.Equal("Expected to contain two topics", 2, topics.Count);

        Assert.True(topics.Contains("topic-2"));
        Assert.True(topics.Contains("topic-3"));
        Assert.False(topics.Contains("topic-A"));
    }

    
    [Xunit.Fact]// (expected = TopologyException)
    public void ShouldNotAllowToAddGlobalStoreWithSourceNameEqualsProcessorName() {
        string sameNameForSourceAndProcessor = "sameName";
        builder.addGlobalStore(
            (StoreBuilder<KeyValueStore>) storeBuilder,
            sameNameForSourceAndProcessor,
            null,
            null,
            null,
            "anyTopicName",
            sameNameForSourceAndProcessor,
            new MockProcessorSupplier());
    }

    [Xunit.Fact]
    public void ShouldThrowIfNameIsNull() {
        Exception e = assertThrows(NullPointerException, () => new InternalTopologyBuilder.Source(null, Collections.emptySet(), null));
        Assert.Equal("name cannot be null", e.getMessage());
    }

    [Xunit.Fact]
    public void ShouldThrowIfTopicAndPatternAreNull() {
        Exception e = assertThrows(IllegalArgumentException, () => new InternalTopologyBuilder.Source("name", null, null));
        Assert.Equal("Either topics or pattern must be not-null, but both are null.", e.getMessage());
    }

    [Xunit.Fact]
    public void ShouldThrowIfBothTopicAndPatternAreNotNull() {
        Exception e = assertThrows(IllegalArgumentException, () => new InternalTopologyBuilder.Source("name", Collections.emptySet(), new Regex("", RegexOptions.Compiled)));
        Assert.Equal("Either topics or pattern must be null, but both are not null.", e.getMessage());
    }

    [Xunit.Fact]
    public void SourceShouldBeEqualIfNameAndTopicListAreTheSame() {
        InternalTopologyBuilder.Source base = new InternalTopologyBuilder.Source("name", Collections.singleton("topic"), null);
        InternalTopologyBuilder.Source sameAsBase = new InternalTopologyBuilder.Source("name", Collections.singleton("topic"), null);

        Assert.Equal(base, (sameAsBase));
    }

    [Xunit.Fact]
    public void SourceShouldBeEqualIfNameAndPatternAreTheSame() {
        InternalTopologyBuilder.Source base = new InternalTopologyBuilder.Source("name", null, new Regex("topic", RegexOptions.Compiled));
        InternalTopologyBuilder.Source sameAsBase = new InternalTopologyBuilder.Source("name", null, new Regex("topic", RegexOptions.Compiled));

        Assert.Equal(base, (sameAsBase));
    }

    [Xunit.Fact]
    public void SourceShouldNotBeEqualForDifferentNamesWithSameTopicList() {
        InternalTopologyBuilder.Source base = new InternalTopologyBuilder.Source("name", Collections.singleton("topic"), null);
        InternalTopologyBuilder.Source differentName = new InternalTopologyBuilder.Source("name2", Collections.singleton("topic"), null);

        Assert.Equal(base, not(equalTo(differentName)));
    }

    [Xunit.Fact]
    public void SourceShouldNotBeEqualForDifferentNamesWithSamePattern() {
        InternalTopologyBuilder.Source base = new InternalTopologyBuilder.Source("name", null, new Regex("topic", RegexOptions.Compiled));
        InternalTopologyBuilder.Source differentName = new InternalTopologyBuilder.Source("name2", null, new Regex("topic", RegexOptions.Compiled));

        Assert.Equal(base, not(equalTo(differentName)));
    }

    [Xunit.Fact]
    public void SourceShouldNotBeEqualForDifferentTopicList() {
        InternalTopologyBuilder.Source base = new InternalTopologyBuilder.Source("name", Collections.singleton("topic"), null);
        InternalTopologyBuilder.Source differentTopicList = new InternalTopologyBuilder.Source("name", Collections.emptySet(), null);
        InternalTopologyBuilder.Source differentTopic = new InternalTopologyBuilder.Source("name", Collections.singleton("topic2"), null);

        Assert.Equal(base, not(equalTo(differentTopicList)));
        Assert.Equal(base, not(equalTo(differentTopic)));
    }

    [Xunit.Fact]
    public void SourceShouldNotBeEqualForDifferentPattern() {
        InternalTopologyBuilder.Source base = new InternalTopologyBuilder.Source("name", null, new Regex("topic", RegexOptions.Compiled));
        InternalTopologyBuilder.Source differentPattern = new InternalTopologyBuilder.Source("name", null, new Regex("topic2", RegexOptions.Compiled));
        InternalTopologyBuilder.Source overlappingPattern = new InternalTopologyBuilder.Source("name", null, new Regex("top*", RegexOptions.Compiled));

        Assert.Equal(base, not(equalTo(differentPattern)));
        Assert.Equal(base, not(equalTo(overlappingPattern)));
    }
}
