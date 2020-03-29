/*






 *

 *





 */





































public class StreamsMetadataStateTest {

    private StreamsMetadataState metadataState;
    private HostInfo hostOne;
    private HostInfo hostTwo;
    private HostInfo hostThree;
    private TopicPartition topic1P0;
    private TopicPartition topic2P0;
    private TopicPartition topic3P0;
    private Dictionary<HostInfo, HashSet<TopicPartition>> hostToPartitions;
    private StreamsBuilder builder;
    private TopicPartition topic1P1;
    private TopicPartition topic2P1;
    private TopicPartition topic4P0;
    private Cluster cluster;
    private string globalTable = "global-table";
    private StreamPartitioner<string, object> partitioner;

    
    public void before() {
        builder = new StreamsBuilder();
        KStream<object, object> one = builder.stream("topic-one");
        one.groupByKey().count(Materialized<object, long, KeyValueStore<Bytes, byte[]>>.As("table-one"));

        KStream<object, object> two = builder.stream("topic-two");
        two.groupByKey().count(Materialized<object, long, KeyValueStore<Bytes, byte[]>>.As("table-two"));

        builder.stream("topic-three")
                .groupByKey()
                .count(Materialized<object, long, KeyValueStore<Bytes, byte[]>>.As("table-three"));

        one.merge(two).groupByKey().count(Materialized<object, long, KeyValueStore<Bytes, byte[]>>.As("merged-table"));

        builder.stream("topic-four").mapValues(new ValueMapper<object, object>() {
            
            public object apply(object value) {
                return value;
            }
        });

        builder.globalTable("global-topic",
                            Consumed.with(null, null),
                            Materialized<object, object, KeyValueStore<Bytes, byte[]>>.As(globalTable));

        TopologyWrapper.getInternalTopologyBuilder(builder.build()).setApplicationId("appId");

        topic1P0 = new TopicPartition("topic-one", 0);
        topic1P1 = new TopicPartition("topic-one", 1);
        topic2P0 = new TopicPartition("topic-two", 0);
        topic2P1 = new TopicPartition("topic-two", 1);
        topic3P0 = new TopicPartition("topic-three", 0);
        topic4P0 = new TopicPartition("topic-four", 0);

        hostOne = new HostInfo("host-one", 8080);
        hostTwo = new HostInfo("host-two", 9090);
        hostThree = new HostInfo("host-three", 7070);
        hostToPartitions = new HashMap<>();
        hostToPartitions.put(hostOne, Utils.mkSet(topic1P0, topic2P1, topic4P0));
        hostToPartitions.put(hostTwo, Utils.mkSet(topic2P0, topic1P1));
        hostToPartitions.put(hostThree, Collections.singleton(topic3P0));

        List<PartitionInfo> partitionInfos = Array.asList(
                new PartitionInfo("topic-one", 0, null, null, null),
                new PartitionInfo("topic-one", 1, null, null, null),
                new PartitionInfo("topic-two", 0, null, null, null),
                new PartitionInfo("topic-two", 1, null, null, null),
                new PartitionInfo("topic-three", 0, null, null, null),
                new PartitionInfo("topic-four", 0, null, null, null));

        cluster = new Cluster(null, Collections.<Node>emptyList(), partitionInfos, Collections.<string>emptySet(), Collections.<string>emptySet());
        metadataState = new StreamsMetadataState(TopologyWrapper.getInternalTopologyBuilder(builder.build()), hostOne);
        metadataState.onChange(hostToPartitions, cluster);
        partitioner = new StreamPartitioner<string, object>() {
            
            public int partition(string topic, string key, object value, int numPartitions) {
                return 1;
            }
        };
    }

    [Xunit.Fact]
    public void shouldNotThrowNPEWhenOnChangeNotCalled() {
        new StreamsMetadataState(TopologyWrapper.getInternalTopologyBuilder(builder.build()), hostOne).getAllMetadataForStore("store");
    }

    [Xunit.Fact]
    public void shouldGetAllStreamInstances() {
        StreamsMetadata one = new StreamsMetadata(hostOne, Utils.mkSet(globalTable, "table-one", "table-two", "merged-table"),
                Utils.mkSet(topic1P0, topic2P1, topic4P0));
        StreamsMetadata two = new StreamsMetadata(hostTwo, Utils.mkSet(globalTable, "table-two", "table-one", "merged-table"),
                Utils.mkSet(topic2P0, topic1P1));
        StreamsMetadata three = new StreamsMetadata(hostThree, Utils.mkSet(globalTable, "table-three"),
                Collections.singleton(topic3P0));

        Collection<StreamsMetadata> actual = metadataState.getAllMetadata();
        Assert.Equal(3, actual.Count);
        Assert.True("expected " + actual + " to contain " + one, actual.Contains(one));
        Assert.True("expected " + actual + " to contain " + two, actual.Contains(two));
        Assert.True("expected " + actual + " to contain " + three, actual.Contains(three));
    }

    [Xunit.Fact]
    public void shouldGetAllStreamsInstancesWithNoStores() {
        builder.stream("topic-five").filter(new Predicate<object, object>() {
            
            public bool test(object key, object value) {
                return true;
            }
        }).to("some-other-topic");

        TopicPartition tp5 = new TopicPartition("topic-five", 1);
        HostInfo hostFour = new HostInfo("host-four", 8080);
        hostToPartitions.put(hostFour, Utils.mkSet(tp5));

        metadataState.onChange(hostToPartitions, cluster.withPartitions(Collections.singletonMap(tp5, new PartitionInfo("topic-five", 1, null, null, null))));

        StreamsMetadata expected = new StreamsMetadata(hostFour, Collections.singleton(globalTable),
                Collections.singleton(tp5));
        Collection<StreamsMetadata> actual = metadataState.getAllMetadata();
        Assert.True("expected " + actual + " to contain " + expected, actual.Contains(expected));
    }

    [Xunit.Fact]
    public void shouldGetInstancesForStoreName() {
        StreamsMetadata one = new StreamsMetadata(hostOne, Utils.mkSet(globalTable, "table-one", "table-two", "merged-table"),
                Utils.mkSet(topic1P0, topic2P1, topic4P0));
        StreamsMetadata two = new StreamsMetadata(hostTwo, Utils.mkSet(globalTable, "table-two", "table-one", "merged-table"),
                Utils.mkSet(topic2P0, topic1P1));
        Collection<StreamsMetadata> actual = metadataState.getAllMetadataForStore("table-one");
        Assert.Equal(2, actual.Count);
        Assert.True("expected " + actual + " to contain " + one, actual.Contains(one));
        Assert.True("expected " + actual + " to contain " + two, actual.Contains(two));
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowIfStoreNameIsNullOnGetAllInstancesWithStore() {
        metadataState.getAllMetadataForStore(null);
    }

    [Xunit.Fact]
    public void shouldReturnEmptyCollectionOnGetAllInstancesWithStoreWhenStoreDoesntExist() {
        Collection<StreamsMetadata> actual = metadataState.getAllMetadataForStore("not-a-store");
        Assert.True(actual.isEmpty());
    }

    [Xunit.Fact]
    public void shouldGetInstanceWithKey() {
        TopicPartition tp4 = new TopicPartition("topic-three", 1);
        hostToPartitions.put(hostTwo, Utils.mkSet(topic2P0, tp4));

        metadataState.onChange(hostToPartitions, cluster.withPartitions(Collections.singletonMap(tp4, new PartitionInfo("topic-three", 1, null, null, null))));

        StreamsMetadata expected = new StreamsMetadata(hostThree, Utils.mkSet(globalTable, "table-three"),
                Collections.singleton(topic3P0));

        StreamsMetadata actual = metadataState.getMetadataWithKey("table-three",
                                                                    "the-key",
                                                                    Serdes.String().Serializer);

        Assert.Equal(expected, actual);
    }

    [Xunit.Fact]
    public void shouldGetInstanceWithKeyAndCustomPartitioner() {
        TopicPartition tp4 = new TopicPartition("topic-three", 1);
        hostToPartitions.put(hostTwo, Utils.mkSet(topic2P0, tp4));

        metadataState.onChange(hostToPartitions, cluster.withPartitions(Collections.singletonMap(tp4, new PartitionInfo("topic-three", 1, null, null, null))));

        StreamsMetadata expected = new StreamsMetadata(hostTwo, Utils.mkSet(globalTable, "table-two", "table-three", "merged-table"),
                Utils.mkSet(topic2P0, tp4));

        StreamsMetadata actual = metadataState.getMetadataWithKey("table-three", "the-key", partitioner);
        Assert.Equal(expected, actual);
    }

    [Xunit.Fact]
    public void shouldReturnNotAvailableWhenClusterIsEmpty() {
        metadataState.onChange(Collections.<HostInfo, HashSet<TopicPartition>>emptyMap(), Cluster.empty());
        StreamsMetadata result = metadataState.getMetadataWithKey("table-one", "a", Serdes.String().Serializer);
        Assert.Equal(StreamsMetadata.NOT_AVAILABLE, result);
    }

    [Xunit.Fact]
    public void shouldGetInstanceWithKeyWithMergedStreams() {
        TopicPartition topic2P2 = new TopicPartition("topic-two", 2);
        hostToPartitions.put(hostTwo, Utils.mkSet(topic2P0, topic1P1, topic2P2));
        metadataState.onChange(hostToPartitions, cluster.withPartitions(Collections.singletonMap(topic2P2, new PartitionInfo("topic-two", 2, null, null, null))));

        StreamsMetadata expected = new StreamsMetadata(hostTwo, Utils.mkSet("global-table", "table-two", "table-one", "merged-table"),
                Utils.mkSet(topic2P0, topic1P1, topic2P2));

        StreamsMetadata actual = metadataState.getMetadataWithKey("merged-table", "123", new StreamPartitioner<string, object>() {
            
            public int partition(string topic, string key, object value, int numPartitions) {
                return 2;
            }
        });

        Assert.Equal(expected, actual);

    }

    [Xunit.Fact]
    public void shouldReturnNullOnGetWithKeyWhenStoreDoesntExist() {
        StreamsMetadata actual = metadataState.getMetadataWithKey("not-a-store",
                "key",
                Serdes.String().Serializer);
        assertNull(actual);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowWhenKeyIsNull() {
        metadataState.getMetadataWithKey("table-three", null, Serdes.String().Serializer);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowWhenSerializerIsNull() {
        metadataState.getMetadataWithKey("table-three", "key", (Serializer<object>) null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowIfStoreNameIsNull() {
        metadataState.getMetadataWithKey(null, "key", Serdes.String().Serializer);
    }

    
    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowIfStreamPartitionerIsNull() {
        metadataState.getMetadataWithKey(null, "key", (StreamPartitioner) null);
    }

    [Xunit.Fact]
    public void shouldHaveGlobalStoreInAllMetadata() {
        Collection<StreamsMetadata> metadata = metadataState.getAllMetadataForStore(globalTable);
        Assert.Equal(3, metadata.Count);
        foreach (StreamsMetadata streamsMetadata in metadata) {
            Assert.True(streamsMetadata.stateStoreNames().Contains(globalTable));
        }
    }

    [Xunit.Fact]
    public void shouldGetMyMetadataForGlobalStoreWithKey() {
        StreamsMetadata metadata = metadataState.getMetadataWithKey(globalTable, "key", Serdes.String().Serializer);
        Assert.Equal(hostOne, metadata.hostInfo());
    }

    [Xunit.Fact]
    public void shouldGetAnyHostForGlobalStoreByKeyIfMyHostUnknown() {
        StreamsMetadataState streamsMetadataState = new StreamsMetadataState(TopologyWrapper.getInternalTopologyBuilder(builder.build()), StreamsMetadataState.UNKNOWN_HOST);
        streamsMetadataState.onChange(hostToPartitions, cluster);
        assertNotNull(streamsMetadataState.getMetadataWithKey(globalTable, "key", Serdes.String().Serializer));
    }

    [Xunit.Fact]
    public void shouldGetMyMetadataForGlobalStoreWithKeyAndPartitioner() {
        StreamsMetadata metadata = metadataState.getMetadataWithKey(globalTable, "key", partitioner);
        Assert.Equal(hostOne, metadata.hostInfo());
    }

    [Xunit.Fact]
    public void shouldGetAnyHostForGlobalStoreByKeyAndPartitionerIfMyHostUnknown() {
        StreamsMetadataState streamsMetadataState = new StreamsMetadataState(TopologyWrapper.getInternalTopologyBuilder(builder.build()), StreamsMetadataState.UNKNOWN_HOST);
        streamsMetadataState.onChange(hostToPartitions, cluster);
        assertNotNull(streamsMetadataState.getMetadataWithKey(globalTable, "key", partitioner));
    }


}
