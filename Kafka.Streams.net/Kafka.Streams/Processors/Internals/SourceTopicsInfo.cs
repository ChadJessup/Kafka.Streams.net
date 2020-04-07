
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class SourceTopicsInfo
    {
        public List<string> sourceTopics { get; }
        public int maxPartitions { get; }
        public string topicWithMostPartitions { get; }

        public SourceTopicsInfo(List<string> sourceTopics)
        {
            this.sourceTopics = sourceTopics;
            foreach (var topic in sourceTopics)
            {
                //List<PartitionInfo> partitions = clusterMetadata.partitionsForTopic(topic);
                //if (partitions.Count > maxPartitions)
                //{
                //    maxPartitions = partitions.Count;
                //    topicWithMostPartitions = partitions[0].Topic;
                //}
            }
        }
    }
}
