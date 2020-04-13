using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Clients.Consumers;
using Kafka.Streams.Errors;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace Kafka.Streams.Tasks
{
    public class TaskManager : ITaskManager
    {
        // initialize the task list
        // activeTasks needs to be concurrent as it can be accessed
        // by QueryableState
        private readonly ILogger<TaskManager> logger;
        private readonly ILoggerFactory loggerFactory;

        //public Guid processId { get; }
        private readonly AssignedStreamsTasks active;
        private readonly AssignedStandbyTasks standby;
        private readonly IChangelogReader changelogReader;
        private readonly IConsumer<byte[], byte[]> restoreConsumer;
        private readonly AbstractTaskCreator<StreamTask> taskCreator;
        private readonly AbstractTaskCreator<StandbyTask> standbyTaskCreator;
        private readonly StreamsMetadataState streamsMetadataState;

        public IAdminClient adminClient { get; }
        private readonly DeleteRecordsResult deleteRecordsResult;

        // following information is updated during rebalance phase by the partition assignor
        private Cluster cluster;
        private Dictionary<TaskId, HashSet<TopicPartition>> assignedActiveTasks;
        private Dictionary<TaskId, HashSet<TopicPartition>> assignedStandbyTasks;

        private string threadClientId;
        private IConsumer<byte[], byte[]> consumer;

        public TaskManager(
            ILoggerFactory loggerFactory,
            ILogger<TaskManager> logger,
            IChangelogReader changelogReader,
            //Guid processId,
            RestoreConsumer restoreConsumer,
            StreamsMetadataState streamsMetadataState,
            AbstractTaskCreator<StreamTask> taskCreator,
            AbstractTaskCreator<StandbyTask> standbyTaskCreator,
            IAdminClient adminClient,
            AssignedStreamsTasks active,
            AssignedStandbyTasks standby)
        {
            this.changelogReader = changelogReader;
            //this.processId = processId;
            this.streamsMetadataState = streamsMetadataState;
            this.restoreConsumer = restoreConsumer;
            this.taskCreator = taskCreator;
            this.standbyTaskCreator = standbyTaskCreator;
            this.active = active;
            this.standby = standby;

            this.logger = logger;

            this.adminClient = adminClient;
        }

        public void CreateTasks(List<TopicPartition> assignment)
        {
            if (this.consumer == null)
            {
                var logPrefix = "";
                throw new InvalidOperationException(logPrefix + "consumer has not been initialized while adding stream tasks. This should not happen.");
            }

            // do this first as we may have suspended standby tasks that
            // will become active or vice versa
            this.standby.CloseNonAssignedSuspendedTasks(this.assignedStandbyTasks);
            this.active.CloseNonAssignedSuspendedTasks(this.assignedActiveTasks);

            this.AddStreamTasks(assignment);
            this.AddStandbyTasks();

            // TODO: can't pause here, due to handler not actually being called after assingment.
            // Pause All the partitions until the underlying state store is ready for All the active tasks.
            //logger.LogTrace($"Pausing partitions: {assignment.ToJoinedString()}");
            //consumer.Pause(assignment);//.Select(a => a.TopicPartition));
        }

        private void AddStreamTasks(List<TopicPartition> assignment)
        {
            if (!this.assignedActiveTasks?.Any() ?? false)
            {
                return;
            }

            var newTasks = new Dictionary<TaskId, HashSet<TopicPartition>>();
            // collect newly assigned tasks and reopen re-assigned tasks
            this.logger.LogDebug($"Adding assigned tasks as active: {this.assignedActiveTasks}");

            foreach (var entry in this.assignedActiveTasks ?? Enumerable.Empty<KeyValuePair<TaskId, HashSet<TopicPartition>>>())
            {
                TaskId taskId = entry.Key;
                HashSet<TopicPartition> partitions = entry.Value;

                if (assignment.TrueForAll(p => partitions.Contains(p)))
                {
                    try
                    {

                        if (!this.active.MaybeResumeSuspendedTask(taskId, partitions))
                        {
                            newTasks.Add(taskId, partitions);
                        }
                    }
                    catch (StreamsException e)
                    {
                        this.logger.LogError("Failed to resume an active task {} due to the following error:", taskId, e);
                        throw;
                    }
                }
                else
                {

                    this.logger.LogWarning("Task {} owned partitions {} are not contained in the assignment {}", taskId, partitions, assignment);
                }
            }

            if (!newTasks.Any())
            {
                return;
            }

            // CANNOT FIND RETRY AND BACKOFF LOGIC
            // create All newly assigned tasks (guard against race condition with other thread via backoff and retry)
            // => other thread will call removeSuspendedTasks(); eventually
            this.logger.LogTrace($"New active tasks to be created: {newTasks}");

            foreach (StreamTask task in this.taskCreator.CreateTasks(this.consumer, this.threadClientId, newTasks))
            {
                this.active.AddNewTask(task);
            }
        }

        private void AddStandbyTasks()
        {
            Dictionary<TaskId, HashSet<TopicPartition>> assignedStandbyTasks = this.assignedStandbyTasks;
            if (!assignedStandbyTasks?.Any() ?? true)
            {
                return;
            }

            this.logger.LogDebug("Adding assigned standby tasks {}", assignedStandbyTasks);
            var newStandbyTasks = new Dictionary<TaskId, HashSet<TopicPartition>>();
            // collect newly assigned standby tasks and reopen re-assigned standby tasks
            foreach (var entry in assignedStandbyTasks ?? new Dictionary<TaskId, HashSet<TopicPartition>>())
            {
                if (!this.standby.MaybeResumeSuspendedTask(entry.Key, entry.Value))
                {
                    newStandbyTasks.Add(entry.Key, entry.Value);
                }
            }

            if (!newStandbyTasks.Any())
            {
                return;
            }

            // create All newly assigned standby tasks (guard against race condition with other thread via backoff and retry)
            // => other thread will call removeSuspendedStandbyTasks(); eventually
            this.logger.LogTrace("New standby tasks to be created: {}", newStandbyTasks);

            foreach (StandbyTask task in this.standbyTaskCreator.CreateTasks(this.consumer, this.threadClientId, newStandbyTasks))
            {
                this.standby.AddNewTask(task);
            }
        }

        public void SetThreadClientId(string threadClientId)
            => this.threadClientId = threadClientId;

        public HashSet<TaskId> ActiveTaskIds()
        {
            return this.active.AllAssignedTaskIds();
        }

        public HashSet<TaskId> StandbyTaskIds()
        {
            return this.standby.AllAssignedTaskIds();
        }

        public HashSet<TaskId> PrevActiveTaskIds()
        {
            return this.active.PreviousTaskIds();
        }

        /**
         * Returns ids of tasks whose states are kept on the local storage.
         */
        public HashSet<TaskId> CachedTasksIds()
        {
            // A client could contain some inactive tasks whose states are still kept on the local storage in the following scenarios:
            // 1) the client is actively maintaining standby tasks by maintaining their states from the change log.
            // 2) the client has just got some tasks migrated out of itself to other clients while these task states
            //    have not been cleaned up yet (this can happen in a rolling bounce upgrade, for example).

            var tasks = new HashSet<TaskId>();

            var stateDirs = this.taskCreator.stateDirectory.ListTaskDirectories();
            if (stateDirs != null)
            {
                foreach (var dir in stateDirs)
                {
                    try
                    {

                        var id = TaskId.Parse(dir.FullName);
                        // if the checkpoint file exists, the state is valid.
                        if (new DirectoryInfo(Path.Combine(dir.FullName, StateManagerUtil.CHECKPOINT_FILE_NAME)).Exists)
                        {
                            tasks.Add(id);
                        }
                    }
                    catch (TaskIdFormatException)
                    {
                        // there may be some unknown files that sits in the same directory,
                        // we should ignore these files instead trying to delete them as well
                    }
                }
            }

            return tasks;
        }

        public InternalTopologyBuilder Builder()
        {
            return this.taskCreator.builder;
        }

        /**
         * Similar to shutdownTasksAndState, however does not Close the task managers, in the hope that
         * soon the tasks will be assigned again
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public void SuspendTasksAndState()
        {
            this.logger.LogDebug("Suspending All active tasks {} and standby tasks {}", this.active.RunningTaskIds(), this.standby.RunningTaskIds());

            var firstException = new RuntimeException();

            firstException = Interlocked.Exchange(ref firstException, this.active.Suspend());
            // Close All restoring tasks as well and then reset changelog reader;
            // for those restoring and still assigned tasks, they will be re-created
            // in.AddStreamTasks.
            firstException = Interlocked.Exchange(ref firstException, this.active.CloseAllRestoringTasks());
            this.changelogReader.Reset();

            firstException = Interlocked.Exchange(ref firstException, this.standby.Suspend());

            // Remove the changelog partitions from restore consumer
            this.restoreConsumer.Unsubscribe();

            //Exception exception = firstException[];
            //if (exception != null)
            //{
            //    throw new StreamsException(logPrefix + "failed to suspend stream tasks", exception);
            //}
        }

        public void Shutdown(bool clean)
        {
            var firstException = new RuntimeException();

            this.logger.LogDebug($"Shutting down All active tasks {this.active.RunningTaskIds()}, " +
                $"standby tasks {this.standby.RunningTaskIds()}," +
                $" suspended tasks {this.active.PreviousTaskIds()}, " +
                $"and suspended standby tasks {this.active.PreviousTaskIds()}");

            try
            {
                this.active.Close(clean);
            }
            catch (RuntimeException fe)
            {
                firstException = Interlocked.Exchange(ref firstException, fe);
            }

            this.standby.Close(clean);

            // Remove the changelog partitions from restore consumer
            try
            {
                this.restoreConsumer.Unsubscribe();
            }
            catch (RuntimeException fe)
            {
                firstException = Interlocked.Exchange(ref firstException, fe);
            }

            this.taskCreator.Close();
            this.standbyTaskCreator.Close();

            RuntimeException fatalException = firstException;
            if (fatalException != null)
            {
                throw fatalException;
            }
        }

        public IAdminClient GetAdminClient()
        {
            return this.adminClient;
        }

        public HashSet<TaskId> SuspendedActiveTaskIds()
        {
            return this.active.PreviousTaskIds();
        }

        public HashSet<TaskId> SuspendedStandbyTaskIds()
        {
            return this.standby.PreviousTaskIds();
        }

        public StreamTask ActiveTask(TopicPartition partition)
        {
            return this.active.RunningTaskFor(partition);
        }

        public StandbyTask StandbyTask(TopicPartition partition)
        {
            return this.standby.RunningTaskFor(partition);
        }

        public Dictionary<TaskId, StreamTask> ActiveTasks()
        {
            return this.active.RunningTaskMap().ToDictionary(k => k.Key, v => v.Value);
        }

        public Dictionary<TaskId, StandbyTask> StandbyTasks()
        {
            return this.standby.RunningTaskMap().ToDictionary(k => k.Key, v => v.Value);
        }

        public void SetConsumer(IConsumer<byte[], byte[]> consumer)
        {
            this.consumer = consumer;
        }

        /**
         * @throws InvalidOperationException If store gets registered after initialized is already finished
         * @throws StreamsException if the store's change log does not contain the partition
         */
        public bool UpdateNewAndRestoringTasks()
        {
            this.active.InitializeNewTasks();
            this.standby.InitializeNewTasks();

            List<TopicPartition> restored = this.changelogReader.Restore(this.active);

            this.active.UpdateRestored(restored);

            if (this.active.AllTasksRunning())
            {
                var assignment = new HashSet<TopicPartition>(this.consumer.Assignment);
                this.logger.LogTrace("Resuming partitions {}", assignment);
                this.consumer.Resume(assignment);
                this.AssignStandbyPartitions();
                return this.standby.AllTasksRunning();
            }
            return false;
        }

        public bool HasActiveRunningTasks()
        {
            return this.active.HasRunningTasks();
        }

        public bool HasStandbyRunningTasks()
        {
            return this.standby.HasRunningTasks();
        }

        private void AssignStandbyPartitions()
        {
            var running = this.standby.running.Values.ToList();
            var checkpointedOffsets = new Dictionary<TopicPartition, long?>();
            foreach (StandbyTask standbyTask in running)
            {
                foreach (var checkpointedOffset in standbyTask.checkpointedOffsets)
                {
                    checkpointedOffsets.Add(checkpointedOffset.Key, checkpointedOffset.Value);
                }
            }

            this.restoreConsumer.Assign(checkpointedOffsets.Keys);

            foreach (var entry in checkpointedOffsets)
            {
                TopicPartition partition = entry.Key;
                var offset = entry.Value;
                if (offset.HasValue && offset.Value >= 0)
                {
                    this.restoreConsumer.Seek(new TopicPartitionOffset(partition, offset.Value));
                }
                else
                {
                    this.restoreConsumer.SeekToBeginning(new[] { partition });
                }
            }
        }

        public void SetClusterMetadata(Cluster cluster)
        {
            this.cluster = cluster;
        }

        public void SetPartitionsByHostState(Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHostState)
        {
            this.streamsMetadataState.OnChange(partitionsByHostState, this.cluster);
        }

        public void SetAssignmentMetadata(
            Dictionary<TaskId, HashSet<TopicPartition>> activeTasks,
            Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks)
        {
            this.assignedActiveTasks = activeTasks;
            this.assignedStandbyTasks = standbyTasks;
        }

        public void UpdateSubscriptionsFromAssignment(List<TopicPartition> partitions)
        {
            if (this.Builder().SourceTopicPattern() != null)
            {
                var assignedTopics = new HashSet<string>();
                foreach (TopicPartition topicPartition in partitions ?? Enumerable.Empty<TopicPartition>())
                {
                    assignedTopics.Add(topicPartition.Topic);
                }

                List<string> existingTopics = this.Builder().SubscriptionUpdates.GetUpdates();
                if (!existingTopics.All(et => assignedTopics.Contains(et)))
                {
                    assignedTopics.UnionWith(existingTopics);
                    this.Builder().UpdateSubscribedTopics(assignedTopics);
                }
            }
        }

        public void UpdateSubscriptionsFromMetadata(HashSet<string> topics)
        {
            if (this.Builder().SourceTopicPattern() != null)
            {
                List<string> existingTopics = this.Builder().SubscriptionUpdates.GetUpdates();
                if (!existingTopics.Equals(topics))
                {
                    this.Builder().UpdateSubscribedTopics(topics);
                }
            }
        }

        /**
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */
        public virtual int CommitAll()
        {
            var committed = this.active.Commit();
            return committed + this.standby.Commit();
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public int Process(DateTime now)
        {
            return this.active.Process(now);
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public int Punctuate()
        {
            return this.active.Punctuate();
        }

        /**
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */
        public int MaybeCommitActiveTasksPerUserRequested()
        {
            return this.active.MaybeCommitPerUserRequested();
        }

        public void MaybePurgeCommitedRecords()
        {
            // we do not check any possible exceptions since none of them are fatal
            // that should cause the application to fail, and we will try delete with
            // newer offsets anyways.
            if (this.deleteRecordsResult == null || this.deleteRecordsResult.All().IsCompleted)
            {
                if (this.deleteRecordsResult != null && this.deleteRecordsResult.All().IsFaulted)
                {
                    this.logger.LogDebug("Previous delete-records request has failed: {}. Try sending the new request now", this.deleteRecordsResult.LowWatermarks());
                }

                var recordsToDelete = new Dictionary<TopicPartition, RecordsToDelete>();
                foreach (var entry in this.active?.RecordsToDelete() ?? Enumerable.Empty<KeyValuePair<TopicPartition, long>>())
                {
                    recordsToDelete.Add(entry.Key, RecordsToDelete.BeforeOffset(entry.Value));
                }

                //deleteRecordsResult = adminClient.deleteRecords(recordsToDelete);

                this.logger.LogTrace("Sent delete-records request: {}", recordsToDelete);
            }
        }

        /**
         * Produces a string representation containing useful information about the TaskManager.
         * This is useful in debugging scenarios.
         *
         * @return A string representation of the TaskManager instance.
         */

        public override string ToString()
        {
            return this.ToString("");
        }

        public string ToString(string indent)
        {
            var builder = new StringBuilder();

            builder.Append("TaskManager\n");
            builder.Append(indent).Append("\tMetadataState:\n");
            builder.Append(this.streamsMetadataState.ToString(indent + "\t\t"));
            builder.Append(indent).Append("\tActive tasks:\n");
            builder.Append(this.active.ToString(indent + "\t\t"));
            builder.Append(indent).Append("\tStandby tasks:\n");
            builder.Append(this.standby.ToString(indent + "\t\t"));

            return builder.ToString();
        }
    }
}
