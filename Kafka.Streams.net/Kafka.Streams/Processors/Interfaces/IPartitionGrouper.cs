using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Tasks;
using System.Collections.Generic;

namespace Kafka.Streams.Processors
{
    /**
     * A partition grouper that generates partition groups given the list of topic-partitions.
     *
     * This grouper also acts as the stream task creation function along with partition distribution
     * such that each generated partition group is assigned with a distinct {@link TaskId};
     * the created task ids will then be assigned to Kafka Streams instances that host the stream
     * processing application.
     */
    public interface IPartitionGrouper
    {
        /**
         * Returns a map of task ids to groups of partitions. A partition group forms a task, thus, partitions that are
         * expected to be processed together must be in the same group.
         *
         * Note that the grouping of partitions need to be <b>sticky</b> such that for a given partition, its assigned
         * task should always be the same regardless of the input parameters to this function. This is to ensure task's
         * local state stores remain valid through workload rebalances among Kafka Streams instances.
         *
         * The default partition grouper : this interface by assigning all partitions across different topics with the same
         * partition id into the same task. See {@link DefaultPartitionGrouper} for more information.
         *
         * @param topicGroups The map from the topic group id to topics
         * @param metadata Metadata of the consuming cluster
         * @return a map of task ids to groups of partitions
         */
        Dictionary<TaskId, HashSet<TopicPartition>> partitionGroups(Dictionary<int, HashSet<string>> topicGroups, Cluster metadata);
    }
}