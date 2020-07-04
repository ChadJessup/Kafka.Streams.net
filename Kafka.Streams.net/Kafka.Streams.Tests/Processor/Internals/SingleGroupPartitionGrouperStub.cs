using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Processors;
using Kafka.Streams.Tasks;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Processor.Internals
{
    /**
     * Used for testing the assignment of a subset of a topology group, not the entire topology
     */
    public class SingleGroupPartitionGrouperStub : IPartitionGrouper
    {
        //private PartitionGrouper defaultPartitionGrouper = new DefaultPartitionGrouper();

        public Dictionary<TaskId, HashSet<TopicPartition>> PartitionGroups(Dictionary<int, HashSet<string>> topicGroups, Cluster metadata)
        {
            Dictionary<int, HashSet<string>> includedTopicGroups = new Dictionary<int, HashSet<string>>();

            foreach (var entry in topicGroups)
            {
                includedTopicGroups.Add(entry.Key, entry.Value);
                break; // arbitrarily use the first entry only
            }

            //Dictionary<TaskId, HashSet<TopicPartition>> result = defaultPartitionGrouper.partitionGroups(includedTopicGroups, metadata);
            return null;// result;
        }
    }
}
