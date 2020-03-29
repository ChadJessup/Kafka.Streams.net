/*






 *

 *





 */












/**
 * Used for testing the assignment of a subset of a topology group, not the entire topology
 */
public class SingleGroupPartitionGrouperStub : PartitionGrouper {
    private PartitionGrouper defaultPartitionGrouper = new DefaultPartitionGrouper();

    
    public Dictionary<TaskId, HashSet<TopicPartition>> partitionGroups(Map<int, HashSet<string>> topicGroups, Cluster metadata) {
        Dictionary<int, HashSet<string>> includedTopicGroups = new HashMap<>();

        foreach (Map.Entry<int, HashSet<string>> entry in topicGroups.entrySet()) {
            includedTopicGroups.put(entry.getKey(), entry.getValue());
            break; // arbitrarily use the first entry only
        }
        Dictionary<TaskId, HashSet<TopicPartition>> result = defaultPartitionGrouper.partitionGroups(includedTopicGroups, metadata);
        return result;
    }
}
