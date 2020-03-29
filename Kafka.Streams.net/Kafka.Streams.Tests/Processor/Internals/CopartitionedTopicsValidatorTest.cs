/*






 *

 *





 */

















public class CopartitionedTopicsValidatorTest {

    private StreamsPartitionAssignor.CopartitionedTopicsValidator validator =
        new StreamsPartitionAssignor.CopartitionedTopicsValidator("thread");
    private Dictionary<TopicPartition, PartitionInfo> partitions = new HashMap<>();
    private Cluster cluster = Cluster.empty();

    
    public void before() {
        partitions.put(
            new TopicPartition("first", 0),
            new PartitionInfo("first", 0, null, null, null));
        partitions.put(
            new TopicPartition("first", 1),
            new PartitionInfo("first", 1, null, null, null));
        partitions.put(
            new TopicPartition("second", 0),
            new PartitionInfo("second", 0, null, null, null));
        partitions.put(
            new TopicPartition("second", 1),
            new PartitionInfo("second", 1, null, null, null));
    }

    [Xunit.Fact]// (expected = IllegalStateException)
    public void shouldThrowTopologyBuilderExceptionIfNoPartitionsFoundForCoPartitionedTopic() {
        validator.validate(Collections.singleton("topic"),
                           Collections.emptyMap(),
                           cluster);
    }

    [Xunit.Fact]// (expected = TopologyException)
    public void shouldThrowTopologyBuilderExceptionIfPartitionCountsForCoPartitionedTopicsDontMatch() {
        partitions.remove(new TopicPartition("second", 0));
        validator.validate(Utils.mkSet("first", "second"),
                           Collections.emptyMap(),
                           cluster.withPartitions(partitions));
    }


    [Xunit.Fact]
    public void shouldEnforceCopartitioningOnRepartitionTopics() {
        InternalTopicConfig config = createTopicConfig("repartitioned", 10);

        validator.validate(Utils.mkSet("first", "second", config.name()),
                           Collections.singletonMap(config.name(), config),
                           cluster.withPartitions(partitions));

        Assert.Equal(config.numberOfPartitions(), (2));
    }


    [Xunit.Fact]
    public void shouldSetNumPartitionsToMaximumPartitionsWhenAllTopicsAreRepartitionTopics() {
        InternalTopicConfig one = createTopicConfig("one", 1);
        InternalTopicConfig two = createTopicConfig("two", 15);
        InternalTopicConfig three = createTopicConfig("three", 5);
        Dictionary<string, InternalTopicConfig> repartitionTopicConfig = new HashMap<>();

        repartitionTopicConfig.put(one.name(), one);
        repartitionTopicConfig.put(two.name(), two);
        repartitionTopicConfig.put(three.name(), three);

        validator.validate(Utils.mkSet(one.name(),
                                       two.name(),
                                       three.name()),
                           repartitionTopicConfig,
                           cluster
        );

        Assert.Equal(one.numberOfPartitions(), (15));
        Assert.Equal(two.numberOfPartitions(), (15));
        Assert.Equal(three.numberOfPartitions(), (15));
    }

    private InternalTopicConfig createTopicConfig(string repartitionTopic,
                                                                               int partitions) {
        InternalTopicConfig repartitionTopicConfig =
            new RepartitionTopicConfig(repartitionTopic, Collections.emptyMap());

        repartitionTopicConfig.setNumberOfPartitions(partitions);
        return repartitionTopicConfig;
    }

}