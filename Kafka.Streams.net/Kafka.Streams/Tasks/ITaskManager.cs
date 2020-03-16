using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.State;
using Kafka.Streams.Topologies;
using System.Collections.Generic;

namespace Kafka.Streams.Tasks
{
    public interface ITaskManager
    {
        IAdminClient adminClient { get; }

        StreamTask activeTask(TopicPartition partition);
        HashSet<TaskId> activeTaskIds();
        Dictionary<TaskId, StreamTask> activeTasks();
        InternalTopologyBuilder builder();
        HashSet<TaskId> cachedTasksIds();
        int commitAll();
        void createTasks(List<TopicPartition> assignment);
        IAdminClient getAdminClient();
        bool hasActiveRunningTasks();
        bool hasStandbyRunningTasks();
        int maybeCommitActiveTasksPerUserRequested();
        void maybePurgeCommitedRecords();
        HashSet<TaskId> prevActiveTaskIds();
        int process(long now);
        int punctuate();
        void SetAssignmentMetadata(Dictionary<TaskId, HashSet<TopicPartition>> activeTasks, Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks);
        void SetClusterMetadata(Cluster cluster);
        void SetConsumer(IConsumer<byte[], byte[]> consumer);
        void SetPartitionsByHostState(Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHostState);
        void SetThreadClientId(string threadClientId);
        void Shutdown(bool clean);
        StandbyTask StandbyTask(TopicPartition partition);
        HashSet<TaskId> StandbyTaskIds();
        Dictionary<TaskId, StandbyTask> StandbyTasks();
        HashSet<TaskId> SuspendedActiveTaskIds();
        HashSet<TaskId> SuspendedStandbyTaskIds();
        void SuspendTasksAndState();
        string ToString();
        string ToString(string indent);
        bool UpdateNewAndRestoringTasks();
        void UpdateSubscriptionsFromAssignment(List<TopicPartition> partitions);
        void UpdateSubscriptionsFromMetadata(HashSet<string> topics);
    }
}