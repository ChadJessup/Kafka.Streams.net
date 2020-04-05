//namespace Kafka.Streams.Tests.Processor
//{
//    /*






//    *

//    *





//    */


















//    public class DefaultPartitionGrouperTest
//    {

//        private List<PartitionInfo> infos = Array.asList(
//                new PartitionInfo("topic1", 0, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic1", 1, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic1", 2, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic2", 0, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
//                new PartitionInfo("topic2", 1, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>())
//        );

//        private Cluster metadata = new Cluster(
//            "cluster",
//            Collections.singletonList(Node.noNode()),
//            infos,
//            Collections.< string > emptySet(),
//            Collections.< string > emptySet());

//        [Xunit.Fact]
//        public void ShouldComputeGroupingForTwoGroups()
//        {
//            PartitionGrouper grouper = new DefaultPartitionGrouper();
//            Dictionary<TaskId, HashSet<TopicPartition>> expectedPartitionsForTask = new HashMap<>();
//            Dictionary<int, HashSet<string>> topicGroups = new HashMap<>();

//            int topicGroupId = 0;

//            topicGroups.put(topicGroupId, mkSet("topic1"));
//            expectedPartitionsForTask.put(new TaskId(topicGroupId, 0), mkSet(new TopicPartition("topic1", 0)));
//            expectedPartitionsForTask.put(new TaskId(topicGroupId, 1), mkSet(new TopicPartition("topic1", 1)));
//            expectedPartitionsForTask.put(new TaskId(topicGroupId, 2), mkSet(new TopicPartition("topic1", 2)));

//            topicGroups.put(++topicGroupId, mkSet("topic2"));
//            expectedPartitionsForTask.put(new TaskId(topicGroupId, 0), mkSet(new TopicPartition("topic2", 0)));
//            expectedPartitionsForTask.put(new TaskId(topicGroupId, 1), mkSet(new TopicPartition("topic2", 1)));

//            Assert.Equal(expectedPartitionsForTask, grouper.partitionGroups(topicGroups, metadata));
//        }

//        [Xunit.Fact]
//        public void ShouldComputeGroupingForSingleGroupWithMultipleTopics()
//        {
//            PartitionGrouper grouper = new DefaultPartitionGrouper();
//            Dictionary<TaskId, HashSet<TopicPartition>> expectedPartitionsForTask = new HashMap<>();
//            Dictionary<int, HashSet<string>> topicGroups = new HashMap<>();

//            int topicGroupId = 0;

//            topicGroups.put(topicGroupId, mkSet("topic1", "topic2"));
//            expectedPartitionsForTask.put(
//                new TaskId(topicGroupId, 0),
//                mkSet(new TopicPartition("topic1", 0), new TopicPartition("topic2", 0)));
//            expectedPartitionsForTask.put(
//                new TaskId(topicGroupId, 1),
//                mkSet(new TopicPartition("topic1", 1), new TopicPartition("topic2", 1)));
//            expectedPartitionsForTask.put(
//                new TaskId(topicGroupId, 2),
//                mkSet(new TopicPartition("topic1", 2)));

//            Assert.Equal(expectedPartitionsForTask, grouper.partitionGroups(topicGroups, metadata));
//        }

//        [Xunit.Fact]// (expected = RuntimeException)
//        public void ShouldNotCreateAnyTasksBecauseOneTopicHasUnknownPartitions()
//        {
//            PartitionGrouper grouper = new DefaultPartitionGrouper();
//            Dictionary<int, HashSet<string>> topicGroups = new HashMap<>();

//            int topicGroupId = 0;

//            topicGroups.put(topicGroupId, mkSet("topic1", "unknownTopic", "topic2"));
//            grouper.partitionGroups(topicGroups, metadata);
//        }
//    }
//}
///*






//*

//*





//*/


















