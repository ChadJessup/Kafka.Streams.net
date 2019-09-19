using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.Processor.Interfaces;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Kafka.Streams.Processor.Internals
{
    public class AssignedStreamsTasks : AssignedTasks<StreamTask>, IRestoringTasks
    {
        private Dictionary<TaskId, StreamTask> restoring = new Dictionary<TaskId, StreamTask>();
        private HashSet<TopicPartition> restoredPartitions = new HashSet<TopicPartition>();
        private Dictionary<TopicPartition, StreamTask> restoringByPartition = new Dictionary<TopicPartition, StreamTask>();
        private ILogger log;
        public AssignedStreamsTasks(LogContext logContext)
            : base(logContext, "stream task")
        {
        }


        public StreamTask restoringTaskFor(TopicPartition partition)
        {
            return restoringByPartition[partition];
        }


        List<StreamTask> allTasks()
        {
            List<StreamTask> tasks = base.allTasks();
            tasks.AddRange(restoring.Values);
            return tasks;
        }


        public HashSet<TaskId> allAssignedTaskIds()
        {
            HashSet<TaskId> taskIds = base.allAssignedTaskIds();
            taskIds.UnionWith(restoring.Keys);
            return taskIds;
        }


        bool allTasksRunning()
        {
            return base.allTasksRunning() && !restoring.Any();
        }

        public RuntimeException closeAllRestoringTasks()
        {
            RuntimeException exception = null;

            log.LogTrace("Closing all restoring stream tasks {}", restoring.Keys);
            IEnumerator<StreamTask> restoringTaskIterator = restoring.Values.GetEnumerator();
            while (restoringTaskIterator.MoveNext())
            {
                StreamTask task = restoringTaskIterator.Current;
                log.LogDebug("Closing restoring task {}", task.id);
                try
                {

                    task.closeStateManager(true);
                }
                catch (RuntimeException e)
                {
                    log.LogError("Failed to Remove restoring task {} due to the following error:", task.id, e);
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

            restoring.Clear();
            restoredPartitions.Clear();
            restoringByPartition.Clear();

            return exception;
        }

        public void updateRestored(List<TopicPartition> restored)
        {
            if (!restored.Any())
            {
                return;
            }

            log.LogTrace("Stream task changelog partitions that have completed restoring so far: {}", restored);
            restoredPartitions.UnionWith(restored);
            for (IEnumerator<KeyValuePair<TaskId, StreamTask>> it = restoring.GetEnumerator(); it.MoveNext();)
            {
                KeyValuePair<TaskId, StreamTask> entry = it.Current;
                StreamTask task = entry.Value;
                if (restoredPartitions.All(p => task.changelogPartitions.Contains(p)))
                {
                    transitionToRunning(task);
                    restoring.Remove(it.Current.Key);

                    log.LogTrace("Stream task {} completed restoration as all its changelog partitions {} have been applied to restore state",
                        task.id,
                        task.changelogPartitions);
                }
                else
                {

                    if (log.IsEnabled(LogLevel.Trace))
                    {
                        HashSet<TopicPartition> outstandingPartitions = new HashSet<TopicPartition>(task.changelogPartitions);
                        outstandingPartitions.RemoveWhere(op => restoredPartitions.Contains(op));

                        log.LogTrace("Stream task {} cannot resume processing yet since some of its changelog partitions have not completed restoring: {}",
                            task.id,
                            outstandingPartitions);
                    }
                }
            }
            if (allTasksRunning())
            {
                restoredPartitions.Clear();
            }
        }

        public void addToRestoring(StreamTask task)
        {
            restoring.Add(task.id, task);
            foreach (TopicPartition topicPartition in task.partitions)
            {
                restoringByPartition.Add(topicPartition, task);
            }

            foreach (TopicPartition topicPartition in task.changelogPartitions)
            {
                restoringByPartition.Add(topicPartition, task);
            }
        }

        /**
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */
        public int maybeCommitPerUserRequested()
        {
            int committed = 0;
            RuntimeException firstException = null;

            for (IEnumerator<StreamTask> it = running.Values.GetEnumerator(); it.MoveNext();)
            {
                StreamTask task = it.Current;
                try
                {

                    if (task.commitRequested && task.commitNeeded)
                    {
                        task.commit();
                        committed++;
                        log.LogDebug("Committed active task {} per user request in", task.id);
                    }
                }
                catch (InvalidOperationException e)
                {
                    log.LogInformation("Failed to commit {} since it got migrated to another thread already. " +
                            "Closing it as zombie before triggering a new rebalance.", task.id);
                    RuntimeException fatalException = closeZombieTask(task);
                    if (fatalException != null)
                    {
                        throw fatalException;
                    }

                    //it.Remove();

                    throw e;
                }
                catch (Exception t)
                {
                    log.LogError("Failed to commit StreamTask {} due to the following error:",
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
         * after the commit call to make sure all consumed offsets are actually committed as well
         */
        public Dictionary<TopicPartition, long> recordsToDelete()
        {
            Dictionary<TopicPartition, long> recordsToDelete = new Dictionary<TopicPartition, long>();
            //foreach (var task in running.Values)
            //{
            //    recordsToDelete.add(task.purgableOffsets());
            //}

            return recordsToDelete;
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public int process(long now)
        {
            int processed = 0;

            IEnumerator<KeyValuePair<TaskId, StreamTask>> it = running.GetEnumerator();
            while (it.MoveNext())
            {
                StreamTask task = it.Current.Value;
                try
                {

                    if (task.isProcessable(now) && task.process())
                    {
                        processed++;
                    }
                }
                catch (TaskMigratedException e)
                {
                    log.LogInformation("Failed to process stream task {} since it got migrated to another thread already. " +
                            "Closing it as zombie before triggering a new rebalance.", task.id);
                    RuntimeException fatalException = closeZombieTask(task);
                    if (fatalException != null)
                    {
                        throw fatalException;
                    }

                    //it.Remove();

                    throw e;
                }
                catch (RuntimeException e)
                {
                    log.LogError("Failed to process stream task {} due to the following error:", task.id, e);
                    throw e;
                }
            }

            return processed;
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public int punctuate()
        {
            int punctuated = 0;
            IEnumerator<KeyValuePair<TaskId, StreamTask>> it = running.GetEnumerator();
            while (it.MoveNext())
            {
                StreamTask task = it.Current.Value;
                try
                {

                    if (task.maybePunctuateStreamTime())
                    {
                        punctuated++;
                    }
                    if (task.maybePunctuateSystemTime())
                    {
                        punctuated++;
                    }
                }
                catch (TaskMigratedException e)
                {
                    log.LogInformation("Failed to punctuate stream task {} since it got migrated to another thread already. " +
                            "Closing it as zombie before triggering a new rebalance.", task.id);

                    RuntimeException fatalException = closeZombieTask(task);
                    if (fatalException != null)
                    {
                        throw fatalException;
                    }

                    // it.Remove();

                    throw e;
                }
                catch (KafkaException e)
                {
                    log.LogError("Failed to punctuate stream task {} due to the following error:", task.id, e);
                    throw e;
                }
            }
            return punctuated;
        }

        public override void clear()
        {
            base.clear();
            restoring.Clear();
            restoringByPartition.Clear();
            restoredPartitions.Clear();
        }

        public new string ToString(string indent)
        {
            StringBuilder builder = new StringBuilder();
            builder.Append(base.ToString(indent));
            describe(builder, restoring.Values.ToList(), indent, "Restoring:");
            return builder.ToString();
        }

        // for testing only

        List<StreamTask> restoringTasks()
        {
            return restoring.Values.ToList();
        }
    }
}