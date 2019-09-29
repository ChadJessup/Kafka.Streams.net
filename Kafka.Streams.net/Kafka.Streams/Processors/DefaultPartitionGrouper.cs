//using Confluent.Kafka;
//using Kafka.Common;
//using Microsoft.Extensions.Logging;
//using System.Collections.Generic;
//using System.Linq;

//namespace Kafka.Streams.Processors
//{
//    /**
//     * Default implementation of the {@link PartitionGrouper} interface that groups partitions by the partition id.
//     *
//     * Join operations requires that topics of the joining entities are copartitoned, i.e., being partitioned by the same key and having the same
//     * number of partitions. Copartitioning is ensured by having the same number of partitions on
//     * joined topics, and by using the serialization and Producer's default partitioner.
//     */
//    public class DefaultPartitionGrouper : IPartitionGrouper
//    {
//        private static ILogger log = new LoggerFactory().CreateLogger<DefaultPartitionGrouper>();
//        /**
//         * Generate tasks with the assigned topic partitions.
//         *
//         * @param topicGroups   group of topics that need to be joined together
//         * @param metadata      metadata of the consuming cluster
//         * @return The map from generated task ids to the assigned partitions
//         */
//        public Dictionary<TaskId, HashSet<TopicPartition>> partitionGroups(Dictionary<int, HashSet<string>> topicGroups, Cluster metadata)
//        {
//            Dictionary<TaskId, HashSet<TopicPartition>> groups = new Dictionary<TaskId, HashSet<TopicPartition>>();

//            foreach (KeyValuePair<int, HashSet<string>> entry in topicGroups)
//            {
//                int topicGroupId = entry.Key;
//                HashSet<string> topicGroup = entry.Value;

//                int maxNumPartitions = maxNumPartitions(metadata, topicGroup);

//                for (int partitionId = 0; partitionId < maxNumPartitions; partitionId++)
//                {
//                    HashSet<TopicPartition> group = new HashSet<TopicPartition>(topicGroup.Count);

//                    foreach (string topic in topicGroup)
//                    {
//                        List<PartitionInfo> partitions = metadata.partitionsForTopic(topic);
//                        if (partitionId < partitions.Count)
//                        {
//                            group.Add(new TopicPartition(topic, partitionId));
//                        }
//                    }
//                    groups.Add(new TaskId(topicGroupId, partitionId), Collections.unmodifiableSet(group));
//                }
//            }

//            return Collections.unmodifiableMap(groups);
//        }

//        /**
//         * @throws StreamsException if no metadata can be received for a topic
//         */
//        protected int maxNumPartitions(Cluster metadata, HashSet<string> topics)
//        {
//            int maxNumPartitions = 0;
//            foreach (string topic in topics)
//            {
//                List<PartitionInfo> partitions = metadata.partitionsForTopic(topic);
//                if (!partitions.Any())
//                {
//                    log.LogError("Empty partitions for topic {}", topic);
//                    throw new RuntimeException("Empty partitions for topic " + topic);
//                }

//                int numPartitions = partitions.Count;
//                if (numPartitions > maxNumPartitions)
//                {
//                    maxNumPartitions = numPartitions;
//                }
//            }
//            return maxNumPartitions;
//        }
//    }
//}