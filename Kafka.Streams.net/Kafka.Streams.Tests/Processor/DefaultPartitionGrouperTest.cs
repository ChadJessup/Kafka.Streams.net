using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Processors;
using Kafka.Streams.Tasks;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Processor
{
    public class DefaultPartitionGrouperTest
    {

        private List<PartitionInfo> infos = Arrays.asList(
                new PartitionInfo("topic1", 0, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
                new PartitionInfo("topic1", 1, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
                new PartitionInfo("topic1", 2, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
                new PartitionInfo("topic2", 0, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
                new PartitionInfo("topic2", 1, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>())
        );

        private Cluster metadata = new Cluster(
            "cluster",
            Collections.singletonList(Node.noNode()),
            infos,
            Collections.< string > emptySet(),
            Collections.< string > emptySet());

        [Fact]
        public void ShouldComputeGroupingForTwoGroups()
        {
            IPartitionGrouper grouper = new DefaultPartitionGrouper();
            Dictionary<TaskId, HashSet<TopicPartition>> expectedPartitionsForTask = new Dictionary<TaskId, HashSet<TopicPartition>>();
            Dictionary<int, HashSet<string>> topicGroups = new Dictionary<int, HashSet<string>>();

            int topicGroupId = 0;

            topicGroups.Put(topicGroupId, mkSet("topic1"));
            expectedPartitionsForTask.Put(new TaskId(topicGroupId, 0), mkSet(new TopicPartition("topic1", 0)));
            expectedPartitionsForTask.Put(new TaskId(topicGroupId, 1), mkSet(new TopicPartition("topic1", 1)));
            expectedPartitionsForTask.Put(new TaskId(topicGroupId, 2), mkSet(new TopicPartition("topic1", 2)));

            topicGroups.Put(++topicGroupId, mkSet("topic2"));
            expectedPartitionsForTask.Put(new TaskId(topicGroupId, 0), mkSet(new TopicPartition("topic2", 0)));
            expectedPartitionsForTask.Put(new TaskId(topicGroupId, 1), mkSet(new TopicPartition("topic2", 1)));

            Assert.Equal(expectedPartitionsForTask, grouper.partitionGroups(topicGroups, metadata));
        }

        [Fact]
        public void ShouldComputeGroupingForSingleGroupWithMultipleTopics()
        {
            PartitionGrouper grouper = new DefaultPartitionGrouper();
            Dictionary<TaskId, HashSet<TopicPartition>> expectedPartitionsForTask = new Dictionary<TaskId, HashSet<TopicPartition>>();
            Dictionary<int, HashSet<string>> topicGroups = new Dictionary<int, HashSet<string>>();

            int topicGroupId = 0;

            topicGroups.Put(topicGroupId, mkSet("topic1", "topic2"));
            expectedPartitionsForTask.Put(
                new TaskId(topicGroupId, 0),
                mkSet(new TopicPartition("topic1", 0), new TopicPartition("topic2", 0)));
            expectedPartitionsForTask.Put(
                new TaskId(topicGroupId, 1),
                mkSet(new TopicPartition("topic1", 1), new TopicPartition("topic2", 1)));
            expectedPartitionsForTask.Put(
                new TaskId(topicGroupId, 2),
                mkSet(new TopicPartition("topic1", 2)));

            Assert.Equal(expectedPartitionsForTask, grouper.partitionGroups(topicGroups, metadata));
        }

        [Fact]// (expected = RuntimeException)
        public void ShouldNotCreateAnyTasksBecauseOneTopicHasUnknownPartitions()
        {
            PartitionGrouper grouper = new DefaultPartitionGrouper();
            Dictionary<int, HashSet<string>> topicGroups = new Dictionary<int, HashSet<string>>();

            int topicGroupId = 0;

            topicGroups.Put(topicGroupId, mkSet("topic1", "unknownTopic", "topic2"));
            grouper.partitionGroups(topicGroups, metadata);
        }
    }
}
