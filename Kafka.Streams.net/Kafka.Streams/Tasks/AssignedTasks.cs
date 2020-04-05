using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.Processors.Internals;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Kafka.Streams.Tasks
{
    public abstract class AssignedTasks<T>
        where T : ITask
    {
        protected ILogger<AssignedTasks<T>> logger { get; }
        private readonly string taskTypeName;
        private readonly Dictionary<TaskId, T> created = new Dictionary<TaskId, T>();
        private readonly Dictionary<TaskId, T> suspended = new Dictionary<TaskId, T>();
        private readonly HashSet<TaskId> previousActiveTasks = new HashSet<TaskId>();

        // IQ may access this map.
        public ConcurrentDictionary<TaskId, T> running = new ConcurrentDictionary<TaskId, T>();
        private readonly Dictionary<TopicPartition, T> runningByPartition = new Dictionary<TopicPartition, T>();

        public AssignedTasks(
            ILogger<AssignedTasks<T>> logger,
            string taskTypeName)
        {
            this.taskTypeName = taskTypeName;
            this.logger = logger;
        }

        public void AddNewTask(T task)
        {
            created.Add(task.id, task);
        }

        /**
         * @throws InvalidOperationException If store gets registered after initialized is already finished
         * @throws StreamsException if the store's change log does not contain the partition
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public void InitializeNewTasks()
        {
            if (created.Any())
            {
                logger.LogDebug("Initializing {}s {}", taskTypeName, created.Keys);
            }

            for (IEnumerator<KeyValuePair<TaskId, T>> it = created.GetEnumerator(); it.MoveNext();)
            {
                KeyValuePair<TaskId, T> entry = it.Current;
                try
                {
                    if (!entry.Value.InitializeStateStores() && typeof(T) == typeof(StreamTask) && entry.Value != null)
                    {
                        if (entry.Value is StreamTask valueAsTask)
                        {
                            logger.LogDebug("Transitioning {} {} to restoring", taskTypeName, entry.Key);
                            ((AssignedStreamsTasks)(object)this).AddToRestoring(valueAsTask);
                        }
                    }
                    else
                    {
                        TransitionToRunning(entry.Value);
                    }

                    created.Remove(it.Current.Key);
                }
                catch (LockException e)
                {
                    // made this trace as it will spam the logs in the poll loop.
                    logger.LogTrace("Could not create {} {} due to {}; will retry", taskTypeName, entry.Key, e.ToString());
                }
            }
        }

        public virtual bool AllTasksRunning()
        {
            return !created.Any() || !suspended.Any();
        }

        public RuntimeException Suspend()
        {
            logger.LogTrace($"Suspending running {taskTypeName} {RunningTaskIds()}");
            var firstException = new RuntimeException();

            firstException = Interlocked.Exchange(ref firstException, SuspendTasks(running.Values.ToList()));
            logger.LogTrace($"Close created {taskTypeName} {created.Keys}");

            firstException = Interlocked.Exchange(ref firstException, CloseNonRunningTasks(created.Values.ToList()));

            previousActiveTasks.Clear();
            previousActiveTasks.UnionWith(running.Keys);
            running.Clear();
            created.Clear();
            runningByPartition.Clear();

            return firstException;
        }

        private RuntimeException CloseNonRunningTasks(List<T> tasks)
        {
            RuntimeException exception = null;
            foreach (T task in tasks)
            {
                try
                {
                    task.Close(false, false);
                }
                catch (RuntimeException e)
                {
                    logger.LogError("Failed to close {}, {}", taskTypeName, task.id, e);
                    if (exception == null)
                    {
                        exception = e;
                    }
                }
            }

            return exception;
        }

        private RuntimeException SuspendTasks(List<T> tasks)
        {
            var firstException = new RuntimeException(null);

            for (IEnumerator<T> it = tasks.GetEnumerator(); it.MoveNext();)
            {
                T task = it.Current;
                try
                {

                    task.Suspend();
                    suspended.Add(task.id, task);
                }
                catch (TaskMigratedException closeAsZombieAndSwallow)
                {
                    // as we suspend a task, we are either shutting down or rebalancing, thus, we swallow and move on
                    logger.LogInformation($"Failed to suspend {taskTypeName} {task.id} since it got migrated to another thread already. " +
                            "Closing it as zombie and move on.");
                    firstException = Interlocked.Exchange(ref firstException, CloseZombieTask(task));

                    tasks.Remove(it.Current);
                }
                catch (RuntimeException e)
                {
                    logger.LogError($"Suspending {taskTypeName} {task.id} failed due to the following error: {e}");

                    firstException = Interlocked.Exchange(ref firstException, e);

                    try
                    {

                        task.Close(false, false);
                    }
                    catch (RuntimeException f)
                    {
                        logger.LogError("After suspending failed, closing the same {} {} failed again due to the following error:", taskTypeName, task.id, f);
                    }
                }
            }

            return firstException;
        }

        public RuntimeException CloseZombieTask(T task)
        {
            try
            {

                task.Close(false, true);
            }
            catch (RuntimeException e)
            {
                logger.LogWarning("Failed to close zombie {} {} due to {}; ignore and proceed.", taskTypeName, task.id, e.ToString());
                return e;
            }
            return null;
        }

        public bool HasRunningTasks()
        {
            return running.Any();
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public bool MaybeResumeSuspendedTask(TaskId taskId, HashSet<TopicPartition> partitions)
        {
            if (suspended.ContainsKey(taskId))
            {
                T task = suspended[taskId];
                logger.LogTrace("Found suspended {} {}", taskTypeName, taskId);
                if (task.partitions.Equals(partitions))
                {
                    suspended.Remove(taskId);
                    task.Resume();
                    try
                    {

                        TransitionToRunning(task);
                    }
                    catch (TaskMigratedException e)
                    {
                        // we need to catch migration exception internally since this function
                        // is triggered in the rebalance callback
                        logger.LogInformation(e, "Failed to resume {} {} since it got migrated to another thread already. " +
                                "Closing it as zombie before triggering a new rebalance.", taskTypeName, task.id);
                        RuntimeException fatalException = CloseZombieTask(task);
                        running.Remove(task.id, out var _);
                        if (fatalException != null)
                        {
                            throw fatalException;
                        }

                        throw;
                    }
                    logger.LogTrace("Resuming suspended {} {}", taskTypeName, task.id);
                    return true;
                }
                else
                {

                    logger.LogWarning("Couldn't resume task {} assigned partitions {}, task partitions {}", taskId, partitions, task.partitions);
                }
            }
            return false;
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public void TransitionToRunning(T task)
        {
            logger.LogDebug("Transitioning {} {} to running", taskTypeName, task.id);
            running.TryAdd(task.id, task);
            task.InitializeTopology();
            foreach (TopicPartition topicPartition in task.partitions)
            {
                runningByPartition.Add(topicPartition, task);
            }

            foreach (TopicPartition topicPartition in task.changelogPartitions)
            {
                runningByPartition.Add(topicPartition, task);
            }
        }

        public T RunningTaskFor(TopicPartition partition)
        {
            if (runningByPartition.TryGetValue(partition, out var task))
            {
                return task;
            }

            return default;
        }

        public HashSet<TaskId> RunningTaskIds()
        {
            return new HashSet<TaskId>(running.Keys);
        }

        public ConcurrentDictionary<TaskId, T> RunningTaskMap()
        {
            return running;
        }

        public override string ToString()
        {
            return ToString("");
        }

        public string ToString(string indent)
        {
            var builder = new StringBuilder();

            Describe(builder, running.Values.ToList(), indent, "Running:");
            Describe(builder, suspended.Values.ToList(), indent, "Suspended:");
            Describe(builder, created.Values.ToList(), indent, "New:");

            return builder.ToString();
        }

        public void Describe(
            StringBuilder builder,
            List<T> tasks,
            string indent,
            string name)
        {
            if (builder is null)
            {
                throw new ArgumentNullException(nameof(builder));
            }

            if (tasks is null)
            {
                throw new ArgumentNullException(nameof(tasks));
            }

            if (string.IsNullOrEmpty(indent))
            {
                throw new ArgumentException("message", nameof(indent));
            }

            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentException("message", nameof(name));
            }

            builder.Append(indent).Append(name);
            foreach (T t in tasks)
            {
                builder.Append(indent).Append(t.ToString(indent + "\t\t"));
            }

            builder.Append("\n");
        }

        public virtual List<T> AllTasks()
        {
            var tasks = new List<T>();

            tasks.AddRange(running.Values);
            tasks.AddRange(suspended.Values);
            tasks.AddRange(created.Values);

            return tasks;
        }

        public virtual HashSet<TaskId> AllAssignedTaskIds()
        {
            var taskIds = new HashSet<TaskId>();
            taskIds.UnionWith(running.Keys);
            taskIds.UnionWith(suspended.Keys);
            taskIds.UnionWith(created.Keys);
            return taskIds;
        }

        public virtual void Clear()
        {
            runningByPartition.Clear();
            running.Clear();
            created.Clear();
            suspended.Clear();
        }

        public HashSet<TaskId> PreviousTaskIds()
        {
            return previousActiveTasks;
        }

        /**
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */
        public int Commit()
        {
            var committed = 0;
            RuntimeException? firstException = null;
            for (IEnumerator<T> it = running.Values.GetEnumerator(); it.MoveNext();)
            {
                T task = it.Current;
                try
                {

                    if (task.commitNeeded)
                    {
                        task.Commit();
                        committed++;
                    }
                }
                catch (TaskMigratedException e)
                {
                    logger.LogInformation(e, $"Failed to commit {taskTypeName} {task.id} since it got migrated to another thread already. " +
                            "Closing it as zombie before triggering a new rebalance.");

                    RuntimeException fatalException = CloseZombieTask(task);
                    if (fatalException != null)
                    {
                        throw fatalException;
                    }

                    running.TryRemove(it.Current.id, out var _);

                    throw;
                }
                catch (RuntimeException t)
                {
                    logger.LogError("Failed to commit {} {} due to the following error:",
                            taskTypeName,
                            task.id,
                            t);

                    if (firstException == null)
                    {
                        firstException = t;
                    }
                }
            }

            if (firstException != null)
            {
                throw firstException;
            }

            return committed;
        }

        public void CloseNonAssignedSuspendedTasks(Dictionary<TaskId, HashSet<TopicPartition>> newAssignment)
        {
            if (newAssignment == null || !newAssignment.Any())
            {
                return;
            }

            IEnumerator<T> standByTaskIterator = suspended.Values.GetEnumerator();
            while (standByTaskIterator.MoveNext())
            {
                T suspendedTask = standByTaskIterator.Current;
                if (!newAssignment.ContainsKey(suspendedTask.id) || !suspendedTask.partitions.Equals(newAssignment[suspendedTask.id]))
                {
                    logger.LogDebug("Closing suspended and not re-assigned {} {}", taskTypeName, suspendedTask.id);
                    try
                    {

                        suspendedTask.CloseSuspended(true, false, null);
                    }
                    catch (Exception e)
                    {
                        logger.LogError("Failed to Remove suspended {} {} due to the following error:", taskTypeName, suspendedTask.id, e);
                    }
                    finally
                    {
                        // standByTaskIterator.Remove();
                    }
                }
            }
        }

        public void Close(bool clean)
        {
            var firstException = new RuntimeException(null);
            foreach (T task in AllTasks())
            {
                try
                {

                    task.Close(clean, false);
                }
                catch (TaskMigratedException e)
                {
                    logger.LogInformation(e, $"Failed to close {taskTypeName} {task.id} since it got migrated to another thread already. " +
                            "Closing it as zombie and move on.");

                    firstException = Interlocked.Exchange(ref firstException, CloseZombieTask(task));
                }
                catch (RuntimeException t)
                {
                    logger.LogError($"Failed while closing {task.GetType().Name} {task.id} due to the following error: {t}");

                    if (clean)
                    {
                        if (!CloseUnclean(task))
                        {
                            firstException = Interlocked.Exchange(ref firstException, t);
                        }
                    }
                    else
                    {
                        firstException = Interlocked.Exchange(ref firstException, t);
                    }
                }
            }

            Clear();

            RuntimeException fatalException = firstException;

            if (fatalException != null)
            {
                throw fatalException;
            }
        }

        private bool CloseUnclean(T task)
        {
            logger.LogInformation("Try to close {} {} unclean.", task.GetType().FullName, task.id);
            try
            {
                task.Close(false, false);
            }
            catch (RuntimeException fatalException)
            {
                logger.LogError("Failed while closing {} {} due to the following error:",
                    task.GetType().Name,
                    task.id,
                    fatalException);

                return false;
            }

            return true;
        }
    }
}