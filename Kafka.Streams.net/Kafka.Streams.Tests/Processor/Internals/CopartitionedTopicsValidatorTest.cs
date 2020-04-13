using Confluent.Kafka;
using Kafka.Streams.Processors.Internals;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class CopartitionedTopicsValidatorTest
    {

        private StreamsPartitionAssignor.CopartitionedTopicsValidator validator =
            new StreamsPartitionAssignor.CopartitionedTopicsValidator("thread");
        private Dictionary<TopicPartition, PartitionInfo> partitions = new HashMap<>();
        private Cluster cluster = Cluster.empty();


        public void Before()
        {
            partitions.Put(
                new TopicPartition("first", 0),
                new PartitionInfo("first", 0, null, null, null));
            partitions.Put(
                new TopicPartition("first", 1),
                new PartitionInfo("first", 1, null, null, null));
            partitions.Put(
                new TopicPartition("second", 0),
                new PartitionInfo("second", 0, null, null, null));
            partitions.Put(
                new TopicPartition("second", 1),
                new PartitionInfo("second", 1, null, null, null));
        }

        [Fact]// (expected = IllegalStateException)
        public void ShouldThrowTopologyBuilderExceptionIfNoPartitionsFoundForCoPartitionedTopic()
        {
            validator.validate(Collections.singleton("topic"),
                               Collections.emptyMap(),
                               cluster);
        }

        [Fact]// (expected = TopologyException)
        public void ShouldThrowTopologyBuilderExceptionIfPartitionCountsForCoPartitionedTopicsDontMatch()
        {
            partitions.remove(new TopicPartition("second", 0));
            validator.validate(Utils.mkSet("first", "second"),
                               Collections.emptyMap(),
                               cluster.withPartitions(partitions));
        }


        [Fact]
        public void ShouldEnforceCopartitioningOnRepartitionTopics()
        {
            InternalTopicConfig config = CreateTopicConfig("repartitioned", 10);

            validator.validate(Utils.mkSet("first", "second", config.Name()),
                               Collections.singletonMap(config.Name(), config),
                               cluster.withPartitions(partitions));

            Assert.Equal(config.numberOfPartitions(), 2);
        }


        [Fact]
        public void ShouldSetNumPartitionsToMaximumPartitionsWhenAllTopicsAreRepartitionTopics()
        {
            InternalTopicConfig one = CreateTopicConfig("one", 1);
            InternalTopicConfig two = CreateTopicConfig("two", 15);
            InternalTopicConfig three = CreateTopicConfig("three", 5);
            Dictionary<string, InternalTopicConfig> repartitionTopicConfig = new HashMap<>();

            repartitionTopicConfig.Put(one.Name(), one);
            repartitionTopicConfig.Put(two.Name(), two);
            repartitionTopicConfig.Put(three.Name(), three);

            validator.validate(Utils.mkSet(one.Name(),
                                           two.Name(),
                                           three.Name()),
                               repartitionTopicConfig,
                               cluster
            );

            Assert.Equal(15, one.numberOfPartitions);
            Assert.Equal(15, two.numberOfPartitions);
            Assert.Equal(15, three.numberOfPartitions);
        }

        private InternalTopicConfig CreateTopicConfig(string repartitionTopic, int partitions)
        {
            InternalTopicConfig repartitionTopicConfig =
                new RepartitionTopicConfig(repartitionTopic, Collections.emptyMap());

            repartitionTopicConfig.SetNumberOfPartitions(partitions);
            return repartitionTopicConfig;
        }
    }
}
