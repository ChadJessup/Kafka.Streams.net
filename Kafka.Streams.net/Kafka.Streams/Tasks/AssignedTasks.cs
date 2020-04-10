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
            this.created.Add(task.id, task);
        }

        /**
         * @throws InvalidOperationException If store gets registered after initialized is already finished
         * @throws StreamsException if the store's change log does not contain the partition
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public void InitializeNewTasks()
        {
            if (this.created.Any())
            {
                this.logger.LogDebug("Initializing {}s {}", this.taskTypeName, this.created.Keys);
            }

            for (IEnumerator<KeyValuePair<TaskId, T>> it = this.created.GetEnumerator(); it.MoveNext();)
            {
                KeyValuePair<TaskId, T> entry = it.Current;
                try
                {
                    if (!entry.Value.InitializeStateStores() && typeof(T) == typeof(StreamTask) && entry.Value != null)
                    {
                        if (entry.Value is StreamTask valueAsTask)
                        {
                            this.logger.LogDebug("Transitioning {} {} to restoring", this.taskTypeName, entry.Key);
                            ((AssignedStreamsTasks)(object)this).AddToRestoring(valueAsTask);
                        }
                    }
                    else
                    {
                        this.TransitionToRunning(entry.Value);
                    }

                    this.created.Remove(it.Current.Key);
                }
                catch (LockException e)
                {
                    // made this trace as it will spam the logs in the poll loop.
                    this.logger.LogTrace("Could not create {} {} due to {}; will retry", this.taskTypeName, entry.Key, e.ToString());
                }
            }
        }

        public virtual bool AllTasksRunning()
        {
            return !this.created.Any() || !this.suspended.Any();
        }

        public RuntimeException Suspend()
        {
            this.logger.LogTrace($"Suspending running {this.taskTypeName} {this.RunningTaskIds()}");
            var firstException = new RuntimeException();

            firstException = Interlocked.Exchange(ref firstException, this.SuspendTasks(this.running.Values.ToList()));
            this.logger.LogTrace($"Close created {this.taskTypeName} {this.created.Keys}");

            firstException = Interlocked.Exchange(ref firstException, this.CloseNonRunningTasks(this.created.Values.ToList()));

            this.previousActiveTasks.Clear();
            this.previousActiveTasks.UnionWith(this.running.Keys);
            this.running.Clear();
            this.created.Clear();
            this.runningByPartition.Clear();

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
                    this.logger.LogError("Failed to Close {}, {}", this.taskTypeName, task.id, e);
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
                    this.suspended.Add(task.id, task);
                }
                catch (TaskMigratedException closeAsZombieAndSwallow)
                {
                    // as we suspend a task, we are either shutting down or rebalancing, thus, we swallow and move on
                    this.logger.LogInformation($"Failed to suspend {this.taskTypeName} {task.id} since it got migrated to another thread already. " +
                            "Closing it as zombie and move on.");
                    firstException = Interlocked.Exchange(ref firstException, this.CloseZombieTask(task));

                    tasks.Remove(it.Current);
                }
                catch (RuntimeException e)
                {
                    this.logger.LogError($"Suspending {this.taskTypeName} {task.id} failed due to the following error: {e}");

                    firstException = Interlocked.Exchange(ref firstException, e);

                    try
                    {

                        task.Close(false, false);
                    }
                    catch (RuntimeException f)
                    {
                        this.logger.LogError("After suspending failed, closing the same {} {} failed again due to the following error:", this.taskTypeName, task.id, f);
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
                this.logger.LogWarning("Failed to Close zombie {} {} due to {}; ignore and proceed.", this.taskTypeName, task.id, e.ToString());
                return e;
            }
            return null;
        }

        public bool HasRunningTasks()
        {
            return this.running.Any();
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public bool MaybeResumeSuspendedTask(TaskId taskId, HashSet<TopicPartition> partitions)
        {
            if (this.suspended.ContainsKey(taskId))
            {
                T task = this.suspended[taskId];
                this.logger.LogTrace("Found suspended {} {}", this.taskTypeName, taskId);
                if (task.partitions.Equals(partitions))
                {
                    this.suspended.Remove(taskId);
                    task.Resume();
                    try
                    {

                        this.TransitionToRunning(task);
                    }
                    catch (TaskMigratedException e)
                    {
                        // we need to catch migration exception internally since this function
                        // is triggered in the rebalance callback
                        this.logger.LogInformation(e, "Failed to resume {} {} since it got migrated to another thread already. " +
                                "Closing it as zombie before triggering a new rebalance.", this.taskTypeName, task.id);
                        RuntimeException fatalException = this.CloseZombieTask(task);
                        this.running.Remove(task.id, out var _);
                        if (fatalException != null)
                        {
                            throw fatalException;
                        }

                        throw;
                    }
                    this.logger.LogTrace("Resuming suspended {} {}", this.taskTypeName, task.id);
                    return true;
                }
                else
                {

                    this.logger.LogWarning("Couldn't resume task {} assigned partitions {}, task partitions {}", taskId, partitions, task.partitions);
                }
            }
            return false;
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public void TransitionToRunning(T task)
        {
            this.logger.LogDebug("Transitioning {} {} to running", this.taskTypeName, task.id);
            this.running.TryAdd(task.id, task);
            task.InitializeTopology();
            foreach (TopicPartition topicPartition in task.partitions)
            {
                this.runningByPartition.Add(topicPartition, task);
            }

            foreach (TopicPartition topicPartition in task.changelogPartitions)
            {
                this.runningByPartition.Add(topicPartition, task);
            }
        }

        public T RunningTaskFor(TopicPartition partition)
        {
            if (this.runningByPartition.TryGetValue(partition, out var task))
            {
                return task;
            }

            return default;
        }

        public HashSet<TaskId> RunningTaskIds()
        {
            return new HashSet<TaskId>(this.running.Keys);
        }

        public ConcurrentDictionary<TaskId, T> RunningTaskMap()
        {
            return this.running;
        }

        public override string ToString()
        {
            return this.ToString("");
        }

        public string ToString(string indent)
        {
            var builder = new StringBuilder();

            this.Describe(builder, this.running.Values.ToList(), indent, "Running:");
            this.Describe(builder, this.suspended.Values.ToList(), indent, "Suspended:");
            this.Describe(builder, this.created.Values.ToList(), indent, "New:");

            return builder.ToString();
        }

        public void Describe(
            StringBuilder builder,
            List<T> tasks,
            string indent,
            string Name)
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

            if (string.IsNullOrEmpty(Name))
            {
                throw new ArgumentException("message", nameof(Name));
            }

            builder.Append(indent).Append(Name);
            foreach (T t in tasks)
            {
                builder.Append(indent).Append(t.ToString(indent + "\t\t"));
            }

            builder.Append("\n");
        }

        public virtual List<T> AllTasks()
        {
            var tasks = new List<T>();

            tasks.AddRange(this.running.Values);
            tasks.AddRange(this.suspended.Values);
            tasks.AddRange(this.created.Values);

            return tasks;
        }

        public virtual HashSet<TaskId> AllAssignedTaskIds()
        {
            var taskIds = new HashSet<TaskId>();
            taskIds.UnionWith(this.running.Keys);
            taskIds.UnionWith(this.suspended.Keys);
            taskIds.UnionWith(this.created.Keys);
            return taskIds;
        }

        public virtual void Clear()
        {
            this.runningByPartition.Clear();
            this.running.Clear();
            this.created.Clear();
            this.suspended.Clear();
        }

        public HashSet<TaskId> PreviousTaskIds()
        {
            return this.previousActiveTasks;
        }

        /**
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */
        public int Commit()
        {
            var committed = 0;
            RuntimeException? firstException = null;
            for (IEnumerator<T> it = this.running.Values.GetEnumerator(); it.MoveNext();)
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
                    this.logger.LogInformation(e, $"Failed to commit {this.taskTypeName} {task.id} since it got migrated to another thread already. " +
                            "Closing it as zombie before triggering a new rebalance.");

                    RuntimeException fatalException = this.CloseZombieTask(task);
                    if (fatalException != null)
                    {
                        throw fatalException;
                    }

                    this.running.TryRemove(it.Current.id, out var _);

                    throw;
                }
                catch (RuntimeException t)
                {
                    this.logger.LogError("Failed to commit {} {} due to the following error:",
                            this.taskTypeName,
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

            IEnumerator<T> standByTaskIterator = this.suspended.Values.GetEnumerator();
            while (standByTaskIterator.MoveNext())
            {
                T suspendedTask = standByTaskIterator.Current;
                if (!newAssignment.ContainsKey(suspendedTask.id) || !suspendedTask.partitions.Equals(newAssignment[suspendedTask.id]))
                {
                    this.logger.LogDebug("Closing suspended and not re-assigned {} {}", this.taskTypeName, suspendedTask.id);
                    try
                    {

                        suspendedTask.CloseSuspended(true, false, null);
                    }
                    catch (Exception e)
                    {
                        this.logger.LogError("Failed to Remove suspended {} {} due to the following error:", this.taskTypeName, suspendedTask.id, e);
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
            foreach (T task in this.AllTasks())
            {
                try
                {

                    task.Close(clean, false);
                }
                catch (TaskMigratedException e)
                {
                    this.logger.LogInformation(e, $"Failed to Close {this.taskTypeName} {task.id} since it got migrated to another thread already. " +
                            "Closing it as zombie and move on.");

                    firstException = Interlocked.Exchange(ref firstException, this.CloseZombieTask(task));
                }
                catch (RuntimeException t)
                {
                    this.logger.LogError($"Failed while closing {task.GetType().Name} {task.id} due to the following error: {t}");

                    if (clean)
                    {
                        if (!this.CloseUnclean(task))
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

            this.Clear();

            RuntimeException fatalException = firstException;

            if (fatalException != null)
            {
                throw fatalException;
            }
        }

        private bool CloseUnclean(T task)
        {
            this.logger.LogInformation("Try to Close {} {} unclean.", task.GetType().FullName, task.id);
            try
            {
                task.Close(false, false);
            }
            catch (RuntimeException fatalException)
            {
                this.logger.LogError("Failed while closing {} {} due to the following error:",
                    task.GetType().Name,
                    task.id,
                    fatalException);

                return false;
            }

            return true;
        }
    }
}