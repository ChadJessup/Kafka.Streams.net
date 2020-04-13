using Confluent.Kafka;
using Kafka.Streams.Processors.Internals;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Kafka.Streams.Tasks
{
    public class AssignedStreamsTasks : AssignedTasks<StreamTask>, IRestoringTasks
    {
        private readonly Dictionary<TaskId, StreamTask> restoring = new Dictionary<TaskId, StreamTask>();
        private readonly HashSet<TopicPartition> restoredPartitions = new HashSet<TopicPartition>();
        private readonly Dictionary<TopicPartition, StreamTask> restoringByPartition = new Dictionary<TopicPartition, StreamTask>();

        public AssignedStreamsTasks(ILogger<AssignedStreamsTasks> logger)
            : base(logger, "stream task")
        {
        }


        public StreamTask RestoringTaskFor(TopicPartition partition)
        {
            return this.restoringByPartition[partition];
        }

        public override List<StreamTask> AllTasks()
        {
            List<StreamTask> tasks = base.AllTasks();
            tasks.AddRange(this.restoring.Values);

            return tasks;
        }


        public override HashSet<TaskId> AllAssignedTaskIds()
        {
            HashSet<TaskId> taskIds = base.AllAssignedTaskIds();
            taskIds.UnionWith(this.restoring.Keys);
            return taskIds;
        }


        public override bool AllTasksRunning()
        {
            return base.AllTasksRunning() && !this.restoring.Any();
        }

        public RuntimeException CloseAllRestoringTasks()
        {
            RuntimeException exception = null;

            this.logger.LogTrace("Closing All restoring stream tasks {}", this.restoring.Keys);
            IEnumerator<StreamTask> restoringTaskIterator = this.restoring.Values.GetEnumerator();
            while (restoringTaskIterator.MoveNext())
            {
                StreamTask task = restoringTaskIterator.Current;
                this.logger.LogDebug("Closing restoring task {}", task.id);
                try
                {

                    task.CloseStateManager(true);
                }
                catch (RuntimeException e)
                {
                    this.logger.LogError("Failed to Remove restoring task {} due to the following error:", task.id, e);
                    if (exception == null)
                    {
                        exception = e;
                    }
                }
                finally
                {
                    //restoringTaskIterator.Remove();
                }
            }

            this.restoring.Clear();
            this.restoredPartitions.Clear();
            this.restoringByPartition.Clear();

            return exception;
        }

        public void UpdateRestored(List<TopicPartition> restored)
        {
            if (!restored.Any())
            {
                return;
            }

            this.logger.LogTrace("Stream task changelog partitions that have completed restoring so far: {}", restored);
            this.restoredPartitions.UnionWith(restored);
            for (IEnumerator<KeyValuePair<TaskId, StreamTask>> it = this.restoring.GetEnumerator(); it.MoveNext();)
            {
                KeyValuePair<TaskId, StreamTask> entry = it.Current;
                StreamTask task = entry.Value;
                if (this.restoredPartitions.All(p => task.changelogPartitions.Contains(p)))
                {
                    this.TransitionToRunning(task);
                    this.restoring.Remove(it.Current.Key);

                    this.logger.LogTrace("Stream task {} completed restoration as All its changelog partitions {} have been applied to restore state",
                        task.id,
                        task.changelogPartitions);
                }
                else
                {

                    if (this.logger.IsEnabled(LogLevel.Trace))
                    {
                        var outstandingPartitions = new HashSet<TopicPartition>(task.changelogPartitions);
                        outstandingPartitions.RemoveWhere(op => this.restoredPartitions.Contains(op));

                        this.logger.LogTrace("Stream task {} cannot resume processing yet since some of its changelog partitions have not completed restoring: {}",
                            task.id,
                            outstandingPartitions);
                    }
                }
            }
            if (this.AllTasksRunning())
            {
                this.restoredPartitions.Clear();
            }
        }

        public void AddToRestoring(StreamTask task)
        {
            this.restoring.Add(task.id, task);
            foreach (TopicPartition topicPartition in task.partitions)
            {
                this.restoringByPartition.Add(topicPartition, task);
            }

            foreach (TopicPartition topicPartition in task.changelogPartitions)
            {
                this.restoringByPartition.Add(topicPartition, task);
            }
        }

        /**
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */
        public int MaybeCommitPerUserRequested()
        {
            var committed = 0;
            RuntimeException firstException = null;

            for (IEnumerator<StreamTask> it = this.running.Values.GetEnumerator(); it.MoveNext();)
            {
                StreamTask task = it.Current;
                try
                {

                    if (task.commitRequested && task.commitNeeded)
                    {
                        task.Commit();
                        committed++;
                        this.logger.LogDebug("Committed active task {} per user request in", task.id);
                    }
                }
                catch (InvalidOperationException e)
                {
                    this.logger.LogInformation(e, "Failed to commit {} since it got migrated to another thread already. " +
                            "Closing it as zombie before triggering a new rebalance.", task.id);
                    RuntimeException fatalException = this.CloseZombieTask(task);
                    if (fatalException != null)
                    {
                        throw fatalException;
                    }

                    //it.Remove();

                    throw;
                }
                catch (Exception t)
                {
                    this.logger.LogError("Failed to commit StreamTask {} due to the following error:",
                            task.id,
                            t);
                    if (firstException == null)
                    {
                        //firstException = t;
                    }
                }
            }

            if (firstException != null)
            {
                throw firstException;
            }

            return committed;
        }

        /**
         * Returns a map of offsets up to which the records can be deleted; this function should only be called
         * after the commit call to make sure All consumed offsets are actually committed as well
         */
        public Dictionary<TopicPartition, long> RecordsToDelete()
        {
            var recordsToDelete = new Dictionary<TopicPartition, long>();
            foreach (var task in this.running.Values)
            {
                foreach (var record in task.PurgableOffsets())
                {
                    recordsToDelete.Add(record.Key, record.Value);
                }
            }

            return recordsToDelete;
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public int Process(DateTime now)
        {
            var processed = 0;

            IEnumerator<KeyValuePair<TaskId, StreamTask>> it = this.running.GetEnumerator();
            while (it.MoveNext())
            {
                StreamTask task = it.Current.Value;
                try
                {

                    if (task.IsProcessable(now) && task.Process())
                    {
                        processed++;
                    }
                }
                catch (TaskMigratedException e)
                {
                    this.logger.LogInformation(e, "Failed to process stream task {} since it got migrated to another thread already. " +
                            "Closing it as zombie before triggering a new rebalance.", task.id);
                    RuntimeException fatalException = this.CloseZombieTask(task);
                    if (fatalException != null)
                    {
                        throw fatalException;
                    }

                    //it.Remove();

                    throw;
                }
                catch (RuntimeException e)
                {
                    this.logger.LogError("Failed to process stream task {} due to the following error:", task.id, e);

                    throw;
                }
            }

            return processed;
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public int Punctuate()
        {
            var punctuated = 0;
            IEnumerator<KeyValuePair<TaskId, StreamTask>> it = this.running.GetEnumerator();
            while (it.MoveNext())
            {
                StreamTask task = it.Current.Value;
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
                    this.logger.LogInformation(e, "Failed to punctuate stream task {} since it got migrated to another thread already. " +
                            "Closing it as zombie before triggering a new rebalance.", task.id);

                    RuntimeException fatalException = this.CloseZombieTask(task);
                    if (fatalException != null)
                    {
                        throw fatalException;
                    }

                    // it.Remove();

                    throw;
                }
                catch (KafkaException e)
                {
                    this.logger.LogError("Failed to punctuate stream task {} due to the following error:", task.id, e);

                    throw;
                }
            }
            return punctuated;
        }

        public override void Clear()
        {
            base.Clear();
            this.restoring.Clear();
            this.restoringByPartition.Clear();
            this.restoredPartitions.Clear();
        }

        public new string ToString(string indent)
        {
            var builder = new StringBuilder();
            builder.Append(base.ToString(indent));
            this.Describe(builder, this.restoring.Values.ToList(), indent, "Restoring:");
            return builder.ToString();
        }
    }
}
