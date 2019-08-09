/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using Confluent.Kafka;
using Kafka.Streams.Errors;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Processor.Internals
{

    public class AssignedStreamsTasks : AssignedTasks<StreamTask>, RestoringTasks
    {
        private Dictionary<TaskId, StreamTask> restoring = new Dictionary<TaskId, StreamTask>();
        private HashSet<TopicPartition> restoredPartitions = new HashSet<TopicPartition>();
        private Dictionary<TopicPartition, StreamTask> restoringByPartition = new Dictionary<>();

        public AssignedStreamsTasks(LogContext logContext)
        {
            base(logContext, "stream task");
        }


        public StreamTask restoringTaskFor(TopicPartition partition)
        {
            return restoringByPartition[partition];
        }


        List<StreamTask> allTasks()
        {
            List<StreamTask> tasks = base.allTasks();
            tasks.AddAll(restoring.Values);
            return tasks;
        }


        HashSet<TaskId> allAssignedTaskIds()
        {
            HashSet<TaskId> taskIds = base.allAssignedTaskIds();
            taskIds.AddAll(restoring.Keys);
            return taskIds;
        }


        bool allTasksRunning()
        {
            return base.allTasksRunning() && restoring.isEmpty();
        }

        RuntimeException closeAllRestoringTasks()
        {
            RuntimeException exception = null;

            log.LogTrace("Closing all restoring stream tasks {}", restoring.Keys);
            IEnumerator<StreamTask> restoringTaskIterator = restoring.Values.iterator();
            while (restoringTaskIterator.hasNext())
            {
                StreamTask task = restoringTaskIterator.next();
                log.LogDebug("Closing restoring task {}", task.id);
                try
                {

                    task.closeStateManager(true);
                }
                catch (RuntimeException e)
                {
                    log.LogError("Failed to Remove restoring task {} due to the following error:", task.id(), e);
                    if (exception == null)
                    {
                        exception = e;
                    }
                }
                finally
                {

                    restoringTaskIterator.Remove();
                }
            }

            restoring.clear();
            restoredPartitions.clear();
            restoringByPartition.clear();

            return exception;
        }

        void updateRestored(List<TopicPartition> restored)
        {
            if (restored.isEmpty())
            {
                return;
            }
            log.LogTrace("Stream task changelog partitions that have completed restoring so far: {}", restored);
            restoredPartitions.AddAll(restored);
            for (IEnumerator<KeyValuePair<TaskId, StreamTask>> it = restoring.iterator(); it.hasNext();)
            {
                KeyValuePair<TaskId, StreamTask> entry = it.next();
                StreamTask task = entry.Value;
                if (restoredPartitions.containsAll(task.changelogPartitions()))
                {
                    transitionToRunning(task);
                    it.Remove();
                    log.LogTrace("Stream task {} completed restoration as all its changelog partitions {} have been applied to restore state",
                        task.id(),
                        task.changelogPartitions());
                }
                else
                {

                    if (log.isTraceEnabled())
                    {
                        HashSet<TopicPartition> outstandingPartitions = new HashSet<>(task.changelogPartitions());
                        outstandingPartitions.removeAll(restoredPartitions);
                        log.LogTrace("Stream task {} cannot resume processing yet since some of its changelog partitions have not completed restoring: {}",
                            task.id(),
                            outstandingPartitions);
                    }
                }
            }
            if (allTasksRunning())
            {
                restoredPartitions.clear();
            }
        }

        void addToRestoring(StreamTask task)
        {
            restoring.Add(task.id, task);
            foreach (TopicPartition topicPartition in task.partitions)
            {
                restoringByPartition.Add(topicPartition, task);
            }
            foreach (TopicPartition topicPartition in task.changelogPartitions())
            {
                restoringByPartition.Add(topicPartition, task);
            }
        }

        /**
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */
        int maybeCommitPerUserRequested()
        {
            int committed = 0;
            RuntimeException firstException = null;

            for (IEnumerator<StreamTask> it = running.iterator(); it.hasNext();)
            {
                StreamTask task = it.next();
                try
                {

                    if (task.commitRequested() && task.commitNeeded())
                    {
                        task.commit();
                        committed++;
                        log.LogDebug("Committed active task {} per user request in", task.id());
                    }
                }
                catch (TaskMigratedException e)
                {
                    log.LogInformation("Failed to commit {} since it got migrated to another thread already. " +
                            "Closing it as zombie before triggering a new rebalance.", task.id());
                    RuntimeException fatalException = closeZombieTask(task);
                    if (fatalException != null)
                    {
                        throw fatalException;
                    }
                    it.Remove();
                    throw e;
                }
                catch (RuntimeException t)
                {
                    log.LogError("Failed to commit StreamTask {} due to the following error:",
                            task.id(),
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

        /**
         * Returns a map of offsets up to which the records can be deleted; this function should only be called
         * after the commit call to make sure all consumed offsets are actually committed as well
         */
        Dictionary<TopicPartition, long> recordsToDelete()
        {
            Dictionary<TopicPartition, long> recordsToDelete = new Dictionary<>();
            foreach (StreamTask task in running.Values)
            {
                recordsToDelete.putAll(task.purgableOffsets());
            }

            return recordsToDelete;
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        int process(long now)
        {
            int processed = 0;

            IEnumerator<KeyValuePair<TaskId, StreamTask>> it = running.iterator();
            while (it.hasNext())
            {
                StreamTask task = it.next().Value;
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
                            "Closing it as zombie before triggering a new rebalance.", task.id());
                    RuntimeException fatalException = closeZombieTask(task);
                    if (fatalException != null)
                    {
                        throw fatalException;
                    }
                    it.Remove();
                    throw e;
                }
                catch (RuntimeException e)
                {
                    log.LogError("Failed to process stream task {} due to the following error:", task.id(), e);
                    throw e;
                }
            }

            return processed;
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        int punctuate()
        {
            int punctuated = 0;
            IEnumerator<KeyValuePair<TaskId, StreamTask>> it = running.iterator();
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
                            "Closing it as zombie before triggering a new rebalance.", task.id());
                    RuntimeException fatalException = closeZombieTask(task);
                    if (fatalException != null)
                    {
                        throw fatalException;
                    }
                    it.Remove();
                    throw e;
                }
                catch (KafkaException e)
                {
                    log.LogError("Failed to punctuate stream task {} due to the following error:", task.id(), e);
                    throw e;
                }
            }
            return punctuated;
        }

        void clear()
        {
            base.clear();
            restoring.clear();
            restoringByPartition.clear();
            restoredPartitions.clear();
        }

        public override string ToString(string indent)
        {
            StringBuilder builder = new StringBuilder();
            builder.Append(base.ToString(indent));
            describe(builder, restoring.Values, indent, "Restoring:");
            return builder.ToString();
        }

        // for testing only

        List<StreamTask> restoringTasks()
        {
            return restoring.Values;
        }
    }
}