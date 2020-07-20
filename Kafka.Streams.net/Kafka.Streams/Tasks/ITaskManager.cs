using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.State;
using Kafka.Streams.Topologies;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Tasks
{
    public interface ITaskManager
    {
        IAdminClient adminClient { get; }

        StreamTask ActiveTask(TopicPartition partition);
        HashSet<TaskId> ActiveTaskIds();
        Dictionary<TaskId, StreamTask> ActiveTasks();
        InternalTopologyBuilder Builder();
        HashSet<TaskId> CachedTasksIds();
        int CommitAll();
        void CreateTasks(List<TopicPartition> assignment);
        IAdminClient GetAdminClient();
        bool HasActiveRunningTasks();
        bool HasStandbyRunningTasks();
        int MaybeCommitActiveTasksPerUserRequested();
        void MaybePurgeCommitedRecords();
        HashSet<TaskId> PrevActiveTaskIds();
        int Process(DateTime now);
        int Punctuate();
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
        bool NeedsInitializationOrRestoration();
        Dictionary<TaskId, long> GetTaskOffsetSums();
        void handleAssignment(Dictionary<TaskId, HashSet<TopicPartition>> activeTasks, object v);
        Guid processId();
        void handleRebalanceStart(HashSet<string> topics);
    }
}
