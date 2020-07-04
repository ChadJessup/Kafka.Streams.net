using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Errors;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Tasks;
using Kafka.Streams.Threads.Stream;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Tasks
{
    public class TaskManager : ITaskManager
    {
        // initialize the task list
        // activeTasks needs to be concurrent as it can be accessed
        // by QueryableState
        private readonly ILogger<TaskManager> log;
        private readonly IChangelogReader changelogReader;
        private Guid processId;
        private readonly string logPrefix;
        private readonly ActiveTaskCreator activeTaskCreator;
        private readonly StandbyTaskCreator standbyTaskCreator;
        private readonly InternalTopologyBuilder builder;
        private readonly IAdminClient adminClient;
        private readonly StateDirectory stateDirectory;
        private readonly ProcessingMode processingMode;

        private readonly Dictionary<TaskId, ITask> tasks = new Dictionary<TaskId, ITask>();
        // materializing this relationship because the lookup is on the hot path
        private readonly Dictionary<TopicPartition, ITask> partitionToTask = new Dictionary<TopicPartition, ITask>();

        private IConsumer<byte[], byte[]> mainConsumer;

        private readonly DeleteRecordsResult deleteRecordsResult;

        private bool rebalanceInProgress = false;  // if we are in the middle of a rebalance, it is not safe to commit

        // includes assigned & initialized tasks and unassigned tasks we locked temporarily during rebalance
        private readonly HashSet<TaskId> lockedTaskDirectories = new HashSet<TaskId>();

        IAdminClient ITaskManager.adminClient { get; }

        private TaskManager(IChangelogReader changelogReader,
                    Guid processId,
                    string logPrefix,
                    ActiveTaskCreator activeTaskCreator,
                    StandbyTaskCreator standbyTaskCreator,
                    InternalTopologyBuilder builder,
                    IAdminClient adminClient,
                    StateDirectory stateDirectory,
                    ProcessingMode processingMode)
        {
            this.changelogReader = changelogReader;
            this.processId = processId;
            this.logPrefix = logPrefix;
            this.activeTaskCreator = activeTaskCreator;
            this.standbyTaskCreator = standbyTaskCreator;
            this.builder = builder;
            this.adminClient = adminClient;
            this.stateDirectory = stateDirectory;
            this.processingMode = processingMode;
        }

        private void SetMainConsumer(IConsumer<byte[], byte[]> mainConsumer)
        {
            this.mainConsumer = mainConsumer;
        }

        private bool IsRebalanceInProgress()
        {
            return this.rebalanceInProgress;
        }

        private void HandleRebalanceStart(HashSet<string> subscribedTopics)
        {
            //builder.addSubscribedTopicsFromMetadata(subscribedTopics, logPrefix);

            this.TryToLockAllNonEmptyTaskDirectories();

            this.rebalanceInProgress = true;
        }

        private void HandleRebalanceComplete()
        {
            // we should pause consumer only within the listener since
            // before then the assignment has not been updated yet.
            this.mainConsumer.Pause(this.mainConsumer.Assignment);

            this.ReleaseLockedUnassignedTaskDirectories();

            this.rebalanceInProgress = false;
        }

        private void HandleCorruption(Dictionary<TaskId, List<TopicPartition>> taskWithChangelogs)
        {
            foreach (var entry in taskWithChangelogs)
            {
                TaskId taskId = entry.Key;
                ITask task = this.tasks[taskId];

                // this call is idempotent so even if the task is only CREATED we can still call it
                this.changelogReader.Remove(task.ChangelogPartitions());

                // mark corrupted partitions to not be checkpointed, and then close the task as dirty
                List<TopicPartition> corruptedPartitions = entry.Value;
                task.MarkChangelogAsCorrupted(corruptedPartitions);

                task.PrepareCloseDirty();
                task.CloseDirty();
                task.Revive();
            }
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         * @throws StreamsException fatal.LogError while creating / initializing the task
         *
         * public for upgrade testing only
         */
        public void HandleAssignment(Dictionary<TaskId, HashSet<TopicPartition>> activeTasks,
                                     Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks)
        {
            this.log.LogInformation("Handle new assignment with:\n" +
                         "\tNew active tasks: {}\n" +
                         "\tNew standby tasks: {}\n" +
                         "\tExisting active tasks: {}\n" +
                         "\tExisting standby tasks: {}",
                     activeTasks.Keys, standbyTasks.Keys, this.ActiveTaskIds(), this.StandbyTaskIds());

            Dictionary<TaskId, HashSet<TopicPartition>> activeTasksToCreate = new Dictionary<TaskId, HashSet<TopicPartition>>(activeTasks);
            Dictionary<TaskId, HashSet<TopicPartition>> standbyTasksToCreate = new Dictionary<TaskId, HashSet<TopicPartition>>(standbyTasks);

            // first rectify all existing tasks
            Dictionary<TaskId, Exception> taskCloseExceptions = new Dictionary<TaskId, Exception>();

            Dictionary<ITask, Dictionary<TopicPartition, long>> checkpointPerTask = new Dictionary<ITask, Dictionary<TopicPartition, long>>();
            Dictionary<TaskId, Dictionary<TopicPartition, OffsetAndMetadata>> consumedOffsetsAndMetadataPerTask = new Dictionary<TaskId, Dictionary<TopicPartition, OffsetAndMetadata>>();
            HashSet<ITask> additionalTasksForCommitting = new HashSet<ITask>();
            HashSet<ITask> dirtyTasks = new HashSet<ITask>();

            foreach (ITask task in this.tasks.Values)
            {
                if (activeTasks.ContainsKey(task.Id) && task.IsActive())
                {
                    task.Resume();
                    if (task.CommitNeeded())
                    {
                        additionalTasksForCommitting.Add(task);
                    }
                    activeTasksToCreate.Remove(task.Id);
                }
                else if (standbyTasks.ContainsKey(task.Id) && !task.IsActive())
                {
                    task.Resume();
                    standbyTasksToCreate.Remove(task.Id);
                }
                else /* we previously owned this task, and we don't have it anymore, or it has changed active/standby state */
                {
                    try
                    {
                        Dictionary<TopicPartition, long> checkpoint = task.PrepareCloseClean();
                        Dictionary<TopicPartition, OffsetAndMetadata> committableOffsets = task
                            .CommittableOffsetsAndMetadata();

                        checkpointPerTask.Add(task, checkpoint);
                        if (!committableOffsets.IsEmpty())
                        {
                            consumedOffsetsAndMetadataPerTask.Add(task.Id, committableOffsets);
                        }
                    }
                    catch (RuntimeException e)
                    {
                        string uncleanMessage = $"Failed to close task {task.Id} cleanly. Attempting to close remaining tasks before re-throwing:";

                        this.log.LogError(uncleanMessage, e);
                        taskCloseExceptions.Add(task.Id, e);
                        // We've already recorded the exception (which is the point of clean).
                        // Now, we should go ahead and complete the close because a half-closed task is no good to anyone.
                        dirtyTasks.Add(task);
                    }
                }
            }

            if (!consumedOffsetsAndMetadataPerTask.IsEmpty())
            {
                try
                {
                    foreach (ITask task in additionalTasksForCommitting)
                    {
                        task.PrepareCommit();
                        Dictionary<TopicPartition, OffsetAndMetadata> committableOffsets = task.CommittableOffsetsAndMetadata();
                        if (!committableOffsets.IsEmpty())
                        {
                            consumedOffsetsAndMetadataPerTask.Add(task.Id, committableOffsets);
                        }
                    }

                    this.CommitOffsetsOrTransaction(consumedOffsetsAndMetadataPerTask);

                    foreach (ITask task in additionalTasksForCommitting)
                    {
                        task.PostCommit();
                    }
                }
                catch (RuntimeException e)
                {
                    this.log.LogError("Failed to batch commit tasks, " +
                        "will close all tasks involved in this commit as dirty by the end", e);
                    dirtyTasks.AddRange(additionalTasksForCommitting);
                    dirtyTasks.AddRange(checkpointPerTask.Keys);

                    checkpointPerTask.Clear();
                    // Just.Add first taskId to re-throw by the end.
                    taskCloseExceptions.Add(consumedOffsetsAndMetadataPerTask.Keys.GetEnumerator().Current, e);
                }
            }

            foreach (var taskAndCheckpoint in checkpointPerTask)
            {
                ITask task = taskAndCheckpoint.Key;
                Dictionary<TopicPartition, long> checkpoint = taskAndCheckpoint.Value;

                try
                {
                    this.CompleteTaskCloseClean(task, checkpoint);
                    this.CleanUpTaskProducer(task, taskCloseExceptions);
                    this.tasks.Remove(task.Id);
                }
                catch (RuntimeException e)
                {
                    string uncleanMessage = string.Format("Failed to close task %s cleanly. Attempting to close remaining tasks before re-throwing:", task.Id);
                    this.log.LogError(uncleanMessage, e);
                    taskCloseExceptions.Add(task.Id, e);
                    // We've already recorded the exception (which is the point of clean).
                    // Now, we should go ahead and complete the close because a half-closed task is no good to anyone.
                    dirtyTasks.Add(task);
                }
            }

            foreach (ITask task in dirtyTasks)
            {
                this.CloseTaskDirty(task);
                this.CleanUpTaskProducer(task, taskCloseExceptions);
                this.tasks.Remove(task.Id);
            }

            if (!taskCloseExceptions.IsEmpty())
            {
                foreach (var entry in taskCloseExceptions)
                {
                    if (!(entry.Value is TaskMigratedException))
                    {
                        if (entry.Value is KafkaException)
                        {
                            this.log.LogError("Hit Kafka exception while closing for first task {}", entry.Key);
                            throw entry.Value;
                        }
                        else
                        {
                            throw new RuntimeException(
                                "Unexpected failure to close " + taskCloseExceptions.Count +
                                    " task(s) [" + taskCloseExceptions.Keys + "]. " +
                                    "First unexpected exception (for task " + entry.Key + ") follows.", entry.Value
                            );
                        }
                    }
                }

                var first = taskCloseExceptions.GetEnumerator().Current;
                // If all exceptions are task-migrated, we would just throw the first one.
                throw first.Value;
            }

            if (!activeTasksToCreate.IsEmpty())
            {
                foreach (ITask task in this.activeTaskCreator.CreateTasks(this.mainConsumer, activeTasksToCreate))
                {
                    this.AddNewTask(task);
                }
            }

            if (!standbyTasksToCreate.IsEmpty())
            {
                foreach (ITask task in this.standbyTaskCreator.CreateTasks(standbyTasksToCreate))
                {
                    this.AddNewTask(task);
                }
            }

            this.builder.AddSubscribedTopicsFromAssignment(
                activeTasks.Values.SelectMany(v => v).ToList(),
                this.logPrefix);
        }

        private void CleanUpTaskProducer(
            ITask task,
            Dictionary<TaskId, Exception> taskCloseExceptions)
        {
            if (task.IsActive())
            {
                try
                {
                    this.activeTaskCreator.CloseAndRemoveTaskProducerIfNeeded(task.Id);
                }
                catch (RuntimeException e)
                {
                    string uncleanMessage = string.Format("Failed to close task %s cleanly. Attempting to close remaining tasks before re-throwing:", task.Id);
                    this.log.LogError(uncleanMessage, e);
                    taskCloseExceptions.TryAdd(task.Id, e);
                }
            }
        }

        private void AddNewTask(ITask task)
        {
            this.tasks.Remove(task.Id, out var previous);
            this.tasks.Add(task.Id, task);

            if (previous != null)
            {
                throw new InvalidOperationException("Attempted to create a task that we already owned: " + task.Id);
            }

            foreach (TopicPartition topicPartition in task.InputPartitions())
            {
                this.partitionToTask.Add(topicPartition, task);
            }
        }

        /**
         * Tries to initialize any new or still-uninitialized tasks, then checks if they can/have completed restoration.
         *
         * @throws InvalidOperationException If store gets registered after initialized is already finished
         * @throws StreamsException if the store's change log does not contain the partition
         * @return {@code true} if all tasks are fully restored
         */
        private bool TryToCompleteRestoration()
        {
            bool allRunning = true;

            List<ITask> restoringTasks = new List<ITask>();
            foreach (ITask task in this.tasks.Values)
            {
                if (task.CurrentState == TaskState.CREATED)
                {
                    try
                    {
                        task.InitializeIfNeeded();
                    }
                    catch (Exception e)
                    {
                        // it is possible that if there are multiple threads within the instance that one thread
                        // trying to grab the task from the other, while the other has not released the lock since
                        // it did not participate in the rebalance. In this case we can just retry in the next iteration
                        this.log.LogDebug("Could not initialize {} due to the following exception; will retry", task.Id, e);
                        allRunning = false;
                    }
                }

                if (task.CurrentState == TaskState.RESTORING)
                {
                    restoringTasks.Add(task);
                }
            }

            if (allRunning && !restoringTasks.IsEmpty())
            {
                HashSet<TopicPartition> restored = new HashSet<TopicPartition>(); //this.changelogReader.CompletedChangelogs();
                foreach (ITask task in restoringTasks)
                {
                    if (restored.IsSupersetOf(task.ChangelogPartitions()))
                    {
                        try
                        {
                            task.CompleteRestoration();
                        }
                        catch (TimeoutException e)
                        {
                            this.log.LogDebug("Could not complete restoration for {} due to {}; will retry", task.Id, e);

                            allRunning = false;
                        }
                    }
                    else
                    {
                        // we found a restoring task that isn't done restoring, which is evidence that
                        // not all tasks are running
                        allRunning = false;
                    }
                }
            }

            if (allRunning)
            {
                // we can call resume multiple times since it is idempotent.
                this.mainConsumer.Resume(this.mainConsumer.Assignment);
            }

            return allRunning;
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        private void HandleRevocation(List<TopicPartition> revokedPartitions)
        {
            HashSet<TopicPartition> remainingPartitions = new HashSet<TopicPartition>(revokedPartitions);

            Dictionary<TaskId, Dictionary<TopicPartition, OffsetAndMetadata>> consumedOffsetsAndMetadataPerTask = new Dictionary<TaskId, Dictionary<TopicPartition, OffsetAndMetadata>>();

            HashSet<ITask> suspended = new HashSet<ITask>();
            foreach (ITask task in this.tasks.Values)
            {
                if (remainingPartitions.IsSupersetOf(task.InputPartitions()))
                {
                    task.PrepareSuspend();
                    Dictionary<TopicPartition, OffsetAndMetadata> committableOffsets = task.CommittableOffsetsAndMetadata();
                    if (!committableOffsets.IsEmpty())
                    {
                        consumedOffsetsAndMetadataPerTask.Add(task.Id, committableOffsets);
                    }
                    suspended.Add(task);
                }
                else
                {
                    if (task.IsActive() && task.CommitNeeded())
                    {
                        task.PrepareCommit();
                        Dictionary<TopicPartition, OffsetAndMetadata> committableOffsets = task.CommittableOffsetsAndMetadata();
                        if (!committableOffsets.IsEmpty())
                        {
                            consumedOffsetsAndMetadataPerTask.Add(task.Id, committableOffsets);
                        }
                    }
                }

                remainingPartitions.Except(task.InputPartitions());
            }

            if (!consumedOffsetsAndMetadataPerTask.IsEmpty())
            {
                this.CommitOffsetsOrTransaction(consumedOffsetsAndMetadataPerTask);
            }

            foreach (ITask task in this.tasks.Values)
            {
                if (consumedOffsetsAndMetadataPerTask.ContainsKey(task.Id))
                {
                    if (suspended.Contains(task))
                    {
                        task.Suspend();
                    }
                    else
                    {
                        task.PostCommit();
                    }
                }
            }

            if (!remainingPartitions.IsEmpty())
            {
                this.log.LogWarning("The following partitions {} are missing from the task partitions. It could potentially " +
                             "due to race condition of consumer detecting the heartbeat failure, or the tasks " +
                             "have been cleaned up by the handleAssignment callback.", remainingPartitions);
            }
        }

        /**
         * Closes active tasks as zombies, as these partitions have been lost and are no longer owned.
         * NOTE this method assumes that when it is called, EVERY task/partition has been lost and must
         * be closed as a zombie.
         *
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        private void HandleLostAll()
        {
            this.log.LogDebug("Closing lost active tasks as zombies.");

            var iterator = this.tasks.Values.GetEnumerator();
            while (iterator.MoveNext())
            {
                ITask task = iterator.Current;

                // Even though we've apparently dropped out of the group, we can continue safely to maintain our
                // standby tasks while we rejoin.
                if (task.IsActive())
                {
                    this.CloseTaskDirty(task);
                    this.tasks.Remove(task.Id);
                    try
                    {
                        this.activeTaskCreator.CloseAndRemoveTaskProducerIfNeeded(task.Id);
                    }
                    catch (Exception e)
                    {
                        this.log.LogWarning("Error closing task producer for " + task.Id + " while handling lostAll", e);
                    }
                }
            }

            if (this.processingMode == ProcessingMode.EXACTLY_ONCE_BETA)
            {
                this.activeTaskCreator.ReInitializeThreadProducer();
            }
        }

        /**
         * Co.Adde the offset total summed across all stores in a task. Includes offset sum for any tasks we own the
         * lock for, which includes assigned and unassigned tasks we locked in {@link #tryToLockAllNonEmptyTaskDirectories()}
         *
         * @return Map from task id to its total offset summed across all state stores
         */
        public Dictionary<TaskId, long> GetTaskOffsetSums()
        {
            Dictionary<TaskId, long> taskOffsetSums = new Dictionary<TaskId, long>();

            foreach (TaskId id in this.lockedTaskDirectories)
            {
                ITask task = this.tasks[id];
                if (task != null)
                {
                    if (task.IsActive() && task.CurrentState == TaskState.RUNNING)
                    {
                        taskOffsetSums.Add(id, task.LatestOffset);
                    }
                    else
                    {
                        taskOffsetSums.Add(id, this.SumOfChangelogOffsets(id, task.ChangelogOffsets()));
                    }
                }
                else
                {
                    FileInfo checkpointFile = this.stateDirectory.CheckpointFileFor(id);
                    try
                    {
                        if (checkpointFile.Exists)
                        {
                            taskOffsetSums.Add(id, this.SumOfChangelogOffsets(id, new OffsetCheckpoint(checkpointFile).Read()));
                        }
                    }
                    catch (IOException e)
                    {
                        this.log.LogWarning(string.Format("Exception caught while trying to read checkpoint for task %s:", id), e);
                    }
                }
            }

            return taskOffsetSums;
        }

        /**
         * Makes a weak attempt to lock all non-empty task directories in the state dir. We are responsible for co.Adding and
         * reporting the offset sum for any unassigned tasks we obtain the lock for in the upcoming rebalance. Tasks
         * that we locked but didn't own will be released at the end of the rebalance (unless of course we were
         * assigned the task as a result of the rebalance). This method should be idempotent.
         */
        private void TryToLockAllNonEmptyTaskDirectories()
        {
            foreach (var dir in this.stateDirectory.ListNonEmptyTaskDirectories())
            {
                try
                {
                    TaskId id = TaskId.Parse(dir.Name);
                    try
                    {
                        if (this.stateDirectory.Lock(id))
                        {
                            this.lockedTaskDirectories.Add(id);
                            if (!this.tasks.ContainsKey(id))
                            {
                                this.log.LogDebug("Temporarily locked unassigned task {} for the upcoming rebalance", id);
                            }
                        }
                    }
                    catch (IOException e)
                    {
                        // if for any reason we can't lock this task dir, just move on
                        this.log.LogWarning(string.Format("Exception caught while attempting to lock task %s:", id), e);
                    }
                }
                catch (TaskIdFormatException e)
                {
                    // ignore any unknown files that sit in the same directory
                }
            }
        }

        /**
         * We must release the lock for any unassigned tasks that we temporarily locked in preparation for a
         * rebalance in {@link #tryToLockAllNonEmptyTaskDirectories()}.
         */
        private void ReleaseLockedUnassignedTaskDirectories()
        {
            Exception? firstException = null;

            IEnumerator<TaskId> taskIdIterator = this.lockedTaskDirectories.GetEnumerator();
            while (taskIdIterator.MoveNext())
            {
                TaskId id = taskIdIterator.Current;
                if (!this.tasks.ContainsKey(id))
                {
                    try
                    {
                        this.stateDirectory.Unlock(id);
                        break;
                    }
                    catch (IOException e)
                    {
                        this.log.LogError($"Caught the following exception while trying to unlock task {id}", e);
                        firstException =
                            new StreamsException($"Failed to unlock task directory {id}", e);
                    }
                }
            }

            Exception? fatalException = firstException;
            if (fatalException != null)
            {
                throw fatalException;
            }
        }

        private long SumOfChangelogOffsets(TaskId id, Dictionary<TopicPartition, long> changelogOffsets)
        {
            long offsetSum = 0L;
            foreach (var changelogEntry in changelogOffsets)
            {
                long offset = changelogEntry.Value;

                offsetSum += offset;
                if (offsetSum < 0)
                {
                    this.log.LogWarning("Sum of changelog offsets for task {} overflowed, pinning to long.MAX_VALUE", id);
                    return long.MaxValue;
                }
            }

            return offsetSum;
        }

        private void CloseTaskDirty(ITask task)
        {
            task.PrepareCloseDirty();
            this.CleanupTask(task);
            task.CloseDirty();
        }

        private void CompleteTaskCloseClean(ITask task, Dictionary<TopicPartition, long> checkpoint)
        {
            this.CleanupTask(task);
            task.CloseClean(checkpoint);
        }

        // Note: this MUST be called *before* actually closing the task
        private void CleanupTask(ITask task)
        {
            // 1. remove the changelog partitions from changelog reader;
            // 2. remove the i.Add partitions from the materialized map;
            // 3. remove the task metrics from the metrics registry
            if (!task.ChangelogPartitions().IsEmpty())
            {
                this.changelogReader.Remove(task.ChangelogPartitions());
            }

            foreach (TopicPartition inputPartition in task.InputPartitions())
            {
                this.partitionToTask.Remove(inputPartition);
            }

            string threadId = Thread.CurrentThread.Name;
            //streamsMetrics.removeAllTaskLevelSensors(threadId, task.id.ToString());
        }

        private void Shutdown(bool clean)
        {
            Exception? firstException = null;

            Dictionary<ITask, Dictionary<TopicPartition, long>> checkpointPerTask = new Dictionary<ITask, Dictionary<TopicPartition, long>>();
            Dictionary<TaskId, Dictionary<TopicPartition, OffsetAndMetadata>> consumedOffsetsAndMetadataPerTask = new Dictionary<TaskId, Dictionary<TopicPartition, OffsetAndMetadata>>();

            foreach (ITask task in this.tasks.Values)
            {
                if (clean)
                {
                    try
                    {
                        Dictionary<TopicPartition, long> checkpoint = task.PrepareCloseClean();
                        Dictionary<TopicPartition, OffsetAndMetadata> committableOffsets = task.CommittableOffsetsAndMetadata();

                        checkpointPerTask.Add(task, checkpoint);
                        if (!committableOffsets.IsEmpty())
                        {
                            consumedOffsetsAndMetadataPerTask.Add(task.Id, committableOffsets);
                        }
                    }
                    catch (TaskMigratedException e)
                    {
                        // just ignore the exception as it doesn't matter during shutdown
                        this.CloseTaskDirty(task);
                    }
                    catch (RuntimeException e)
                    {
                        firstException = e;
                        this.CloseTaskDirty(task);
                    }
                }
                else
                {
                    this.CloseTaskDirty(task);
                }
            }

            if (clean && !consumedOffsetsAndMetadataPerTask.IsEmpty())
            {
                this.CommitOffsetsOrTransaction(consumedOffsetsAndMetadataPerTask);
            }

            foreach (var taskAndCheckpoint in checkpointPerTask)
            {
                ITask task = taskAndCheckpoint.Key;
                Dictionary<TopicPartition, long> checkpoint = taskAndCheckpoint.Value;
                try
                {
                    this.CompleteTaskCloseClean(task, checkpoint);
                }
                catch (RuntimeException e)
                {
                    firstException = e;
                    this.CloseTaskDirty(task);
                }
            }

            foreach (ITask task in this.tasks.Values)
            {
                if (task.IsActive())
                {
                    try
                    {
                        this.activeTaskCreator.CloseAndRemoveTaskProducerIfNeeded(task.Id);
                    }
                    catch (RuntimeException e)
                    {
                        if (clean)
                        {
                            firstException = e;
                        }
                        else
                        {
                            this.log.LogWarning("Ignoring an exception while closing task " + task.Id + " producer.", e);
                        }
                    }
                }
            }

            this.tasks.Clear();

            try
            {
                this.activeTaskCreator.CloseThreadProducerIfNeeded();
            }
            catch (RuntimeException e)
            {
                if (clean)
                {
                    firstException = e;
                }
                else
                {
                    this.log.LogWarning("Ignoring an exception while closing thread producer.", e);
                }
            }

            try
            {
                // this should be called after closing all tasks, to make sure we unlock the task dir for tasks that may
                // have still been in CREATED at the time of shutdown, since ITask#close will not do so
                this.ReleaseLockedUnassignedTaskDirectories();
            }
            catch (RuntimeException e)
            {
                firstException = e;
            }

            Exception? fatalException = firstException;

            if (fatalException != null)
            {
                throw new RuntimeException("Unexpected exception while closing task", fatalException);
            }
        }

        private HashSet<TaskId> ActiveTaskIds()
        {
            return this.ActiveTaskStream()
                .Select(t => t.Id)
                .ToHashSet();
        }

        private HashSet<TaskId> StandbyTaskIds()
        {
            return this.StandbyTaskStream()
                .Select(t => t.Id)
                .ToHashSet();
        }

        private ITask AddPartition(TopicPartition partition)
        {
            return this.partitionToTask[partition];
        }

        private Dictionary<TaskId, ITask> ActiveTaskMap()
        {
            return this.ActiveTaskStream()
                .ToDictionary(t => t.Id, t => t);
        }

        private List<ITask> ActiveTaskIterable()
        {
            return this.ActiveTaskStream().ToList();
        }

        private IEnumerable<ITask> ActiveTaskStream()
        {
            return this.tasks.Values.Where(t => t.IsActive());
        }

        private Dictionary<TaskId, ITask> StandbyTaskMap()
        {
            return this.StandbyTaskStream()
                .ToDictionary(t => t.Id, t => t);
        }

        private IEnumerable<ITask> StandbyTaskStream()
        {
            return this.tasks.Values.Where(t => !t.IsActive());
        }

        /**
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         * @return number of committed offsets, or -1 if we are in the middle of a rebalance and cannot commit
         */
        private int CommitAll()
        {
            return this.Commit(this.tasks.Values);
        }

        private int Commit(IEnumerable<ITask> tasks)
        {
            if (this.rebalanceInProgress)
            {
                return -1;
            }
            else
            {
                int committed = 0;
                Dictionary<TaskId, Dictionary<TopicPartition, OffsetAndMetadata>> consumedOffsetsAndMetadataPerTask =
                    new Dictionary<TaskId, Dictionary<TopicPartition, OffsetAndMetadata>>();

                foreach (ITask task in tasks)
                {
                    if (task.CommitNeeded())
                    {
                        task.PrepareCommit();
                        Dictionary<TopicPartition, OffsetAndMetadata> offsetAndMetadata = task.CommittableOffsetsAndMetadata();
                        if (!offsetAndMetadata.IsEmpty())
                        {
                            consumedOffsetsAndMetadataPerTask.Add(task.Id, offsetAndMetadata);
                        }
                    }
                }

                if (!consumedOffsetsAndMetadataPerTask.IsEmpty())
                {
                    this.CommitOffsetsOrTransaction(consumedOffsetsAndMetadataPerTask);
                }

                foreach (ITask task in tasks)
                {
                    if (task.CommitNeeded())
                    {
                        ++committed;
                        task.PostCommit();
                    }
                }

                return committed;
            }
        }

        /**
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */
        private int MaybeCommitActiveTasksPerUserRequested()
        {
            if (this.rebalanceInProgress)
            {
                return -1;
            }
            else
            {
                foreach (ITask task in this.ActiveTaskIterable())
                {
                    if (task.CommitRequested() && task.CommitNeeded())
                    {
                        return this.Commit(this.ActiveTaskIterable());
                    }
                }
                return 0;
            }
        }

        private void CommitOffsetsOrTransaction(Dictionary<TaskId, Dictionary<TopicPartition, OffsetAndMetadata>> offsetsPerTask)
        {
            if (this.processingMode == ProcessingMode.EXACTLY_ONCE_ALPHA)
            {
                foreach (var taskToCommit in offsetsPerTask)
                {
                    this.activeTaskCreator.StreamsProducerForTask(taskToCommit.Key)
                        .CommitTransaction(taskToCommit.Value, null);// this.mainConsumer.GroupMetadata());
                }
            }
            else
            {
                Dictionary<TopicPartition, OffsetAndMetadata> allOffsets = new Dictionary<TopicPartition, OffsetAndMetadata>(offsetsPerTask.Count);

                foreach (var kvp in offsetsPerTask)
                {
                    foreach (var val in kvp.Value)
                    {
                        allOffsets.Add(val.Key, val.Value);
                    }
                }

                if (this.processingMode == ProcessingMode.EXACTLY_ONCE_BETA)
                {
                    //this.activeTaskCreator.threadProducer.commitTransaction(allOffsets, this.mainConsumer.GroupMetadata());
                }
                else
                {
                    try
                    {
                        this.mainConsumer.Commit(allOffsets.Select(kvp => new TopicPartitionOffset(kvp.Key.Topic, kvp.Key.Partition, kvp.Value.offset)));
                    }
                    catch (CommitFailedException error)
                    {
                        throw new TaskMigratedException("Consumer committing offsets failed, " +
                            "indicating the corresponding thread is no longer part of the group", error);
                    }
                    catch (TimeoutException error)
                    {
                        // TODO KIP-447: we can consider treating it as non-fatal and retry on the thread level
                        throw new StreamsException("Timed out while committing offsets via consumer", error);
                    }
                    catch (KafkaException error)
                    {
                        throw new StreamsException("Error encountered committing offsets via consumer", error);
                    }
                }
            }
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        private int Process(int maxNumRecords, IClock time)
        {
            int totalProcessed = 0;

            long now = time.NowAsEpochMilliseconds;
            foreach (ITask task in this.ActiveTaskIterable())
            {
                try
                {
                    int processed = 0;
                    long then = now;
                    while (processed < maxNumRecords && task.Process(now))
                    {
                        processed++;
                    }

                    now = time.NowAsEpochMilliseconds;
                    totalProcessed += processed;
                    task.RecordProcessBatchTime(now - then);
                }
                catch (TaskMigratedException e)
                {
                    this.log.LogInformation("Failed to process stream task {} since it got migrated to another thread already. " +
                                 "Will trigger a new rebalance and close all tasks as zombies together.", task.Id);
                    throw;
                }
                catch (RuntimeException e)
                {
                    this.log.LogError("Failed to process stream task {} due to the following.LogError:", task.Id, e);
                    throw;
                }
            }

            return totalProcessed;
        }

        private void RecordTaskProcessRatio(long totalProcessLatencyMs)
        {
            foreach (ITask task in this.ActiveTaskIterable())
            {
                task.RecordProcessTimeRatioAndBufferSize(totalProcessLatencyMs);
            }
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        private int Punctuate()
        {
            int punctuated = 0;

            foreach (ITask task in this.ActiveTaskIterable())
            {
                try
                {
                    if (task.MaybePunctuateStreamTime())
                    {
                        punctuated++;
                    }
                    if (task.MaybePunctuateSystemTime())
                    {
                        punctuated++;
                    }
                }
                catch (TaskMigratedException e)
                {
                    this.log.LogInformation("Failed to punctuate stream task {} since it got migrated to another thread already. " +
                                 "Will trigger a new rebalance and close all tasks as zombies together.", task.Id);
                    throw;
                }
                catch (KafkaException e)
                {
                    this.log.LogError("Failed to punctuate stream task {} due to the following.LogError:", task.Id, e);
                    throw;
                }
            }

            return punctuated;
        }

        private void MaybePurgeCommittedRecords()
        {
            // we do not check any possible exceptions since none of them are fatal
            // that should cause the application to fail, and we will try delete with
            // newer offsets anyways.
            if (this.deleteRecordsResult == null || this.deleteRecordsResult.All().IsCompleted)
            {

                if (this.deleteRecordsResult != null && this.deleteRecordsResult.All().IsFaulted)
                {
                    this.log.LogDebug("Previous delete-records request has failed: {}. Try sending the new request now",
                              this.deleteRecordsResult.LowWatermarks());
                }

                Dictionary<TopicPartition, RecordsToDelete> recordsToDelete = new Dictionary<TopicPartition, RecordsToDelete>();
                foreach (ITask task in this.ActiveTaskIterable())
                {
                    foreach (var entry in task.PurgeableOffsets())
                    {
                        recordsToDelete.Add(entry.Key, RecordsToDelete.BeforeOffset(entry.Value));
                    }
                }
                if (!recordsToDelete.IsEmpty())
                {
                    //deleteRecordsResult = adminClient.DeleteRecords(recordsToDelete);
                    this.log.LogTrace("Sent delete-records request: {}", recordsToDelete);
                }
            }
        }

        /**
         * Produces a string representation containing useful.LogI.Formationrmation about the TaskManager.
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
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("TaskManager\n");
            stringBuilder.Append(indent).Append("\tMetadataState:\n");
            stringBuilder.Append(indent).Append("\tTasks:\n");
            foreach (ITask task in this.tasks.Values)
            {
                stringBuilder.Append(indent)
                             .Append("\t\t")
                             .Append(task.Id)
                             .Append(" ")
                             .Append(task.CurrentState)
                             .Append(" ")
                             .Append(task.GetType().Name)
                             .Append('(').Append(task.IsActive() ? "active" : "standby").Append(')');
            }
            return stringBuilder.ToString();
        }

        private HashSet<string> ProducerClientIds()
        {
            return this.activeTaskCreator.ProducerClientIds();
        }

        public StreamTask ActiveTask(TopicPartition partition)
        {
            throw new NotImplementedException();
        }

        HashSet<TaskId> ITaskManager.ActiveTaskIds()
        {
            throw new NotImplementedException();
        }

        public Dictionary<TaskId, StreamTask> ActiveTasks()
        {
            throw new NotImplementedException();
        }

        public InternalTopologyBuilder Builder()
        {
            throw new NotImplementedException();
        }

        public HashSet<TaskId> CachedTasksIds()
        {
            throw new NotImplementedException();
        }

        int ITaskManager.CommitAll()
        {
            throw new NotImplementedException();
        }

        public void CreateTasks(List<TopicPartition> assignment)
        {
            throw new NotImplementedException();
        }

        public IAdminClient GetAdminClient()
        {
            throw new NotImplementedException();
        }

        public bool HasActiveRunningTasks()
        {
            throw new NotImplementedException();
        }

        public bool HasStandbyRunningTasks()
        {
            throw new NotImplementedException();
        }

        int ITaskManager.MaybeCommitActiveTasksPerUserRequested()
        {
            throw new NotImplementedException();
        }

        public void MaybePurgeCommitedRecords()
        {
            throw new NotImplementedException();
        }

        public HashSet<TaskId> PrevActiveTaskIds()
        {
            throw new NotImplementedException();
        }

        public int Process(DateTime now)
        {
            throw new NotImplementedException();
        }

        int ITaskManager.Punctuate()
        {
            throw new NotImplementedException();
        }

        public void SetAssignmentMetadata(Dictionary<TaskId, HashSet<TopicPartition>> activeTasks, Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks)
        {
            throw new NotImplementedException();
        }

        public void SetClusterMetadata(Cluster cluster)
        {
            throw new NotImplementedException();
        }

        public void SetConsumer(IConsumer<byte[], byte[]> consumer)
        {
            throw new NotImplementedException();
        }

        public void SetPartitionsByHostState(Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHostState)
        {
            throw new NotImplementedException();
        }

        public void SetThreadClientId(string threadClientId)
        {
            throw new NotImplementedException();
        }

        void ITaskManager.Shutdown(bool clean)
        {
            throw new NotImplementedException();
        }

        public StandbyTask StandbyTask(TopicPartition partition)
        {
            throw new NotImplementedException();
        }

        HashSet<TaskId> ITaskManager.StandbyTaskIds()
        {
            throw new NotImplementedException();
        }

        public Dictionary<TaskId, StandbyTask> StandbyTasks()
        {
            throw new NotImplementedException();
        }

        public HashSet<TaskId> SuspendedActiveTaskIds()
        {
            throw new NotImplementedException();
        }

        public HashSet<TaskId> SuspendedStandbyTaskIds()
        {
            throw new NotImplementedException();
        }

        public void SuspendTasksAndState()
        {
            throw new NotImplementedException();
        }

        public bool UpdateNewAndRestoringTasks()
        {
            throw new NotImplementedException();
        }

        public void UpdateSubscriptionsFromAssignment(List<TopicPartition> partitions)
        {
            throw new NotImplementedException();
        }

        public void UpdateSubscriptionsFromMetadata(HashSet<string> topics)
        {
            throw new NotImplementedException();
        }
    }
}
