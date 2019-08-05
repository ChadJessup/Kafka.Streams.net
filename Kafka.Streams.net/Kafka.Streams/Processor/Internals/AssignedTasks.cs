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
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{

    abstract class AssignedTasks<T>
        where T : Task
    {
        ILogger log;
        private string taskTypeName;
        private Dictionary<TaskId, T> created = new HashMap<>();
        private Dictionary<TaskId, T> suspended = new HashMap<>();
        private HashSet<TaskId> previousActiveTasks = new HashSet<>();

        // IQ may access this map.
        Dictionary<TaskId, T> running = new ConcurrentHashMap<>();
        private Dictionary<TopicPartition, T> runningByPartition = new HashMap<>();

        AssignedTasks(LogContext logContext,
                      string taskTypeName)
        {
            this.taskTypeName = taskTypeName;
            this.log = logContext.logger(GetType());
        }

        void addNewTask(T task)
        {
            created.Add(task.id(), task);
        }

        /**
         * @throws InvalidOperationException If store gets registered after initialized is already finished
         * @throws StreamsException if the store's change log does not contain the partition
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        void initializeNewTasks()
        {
            if (!created.isEmpty())
            {
                log.LogDebug("Initializing {}s {}", taskTypeName, created.keySet());
            }
            for (Iterator<Map.Entry<TaskId, T>> it = created.entrySet().iterator(); it.hasNext();)
            {
                Map.Entry<TaskId, T> entry = it.next();
                try
                {

                    if (!entry.Value.initializeStateStores())
                    {
                        log.LogDebug("Transitioning {} {} to restoring", taskTypeName, entry.Key);
                        ((AssignedStreamsTasks)this).AddToRestoring((StreamTask)entry.Value);
                    }
                    else
                    {

                        transitionToRunning(entry.Value);
                    }
                    it.Remove();
                }
                catch (LockException e)
                {
                    // made this trace as it will spam the logs in the poll loop.
                    log.LogTrace("Could not create {} {} due to {}; will retry", taskTypeName, entry.Key, e.ToString());
                }
            }
        }

        bool allTasksRunning()
        {
            return created.isEmpty() && suspended.isEmpty();
        }

        Collection<T> running()
        {
            return running.values();
        }

        RuntimeException suspend()
        {
            AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
            log.LogTrace("Suspending running {} {}", taskTypeName, runningTaskIds());
            firstException.compareAndSet(null, suspendTasks(running.values()));
            log.LogTrace("Close created {} {}", taskTypeName, created.keySet());
            firstException.compareAndSet(null, closeNonRunningTasks(created.values()));
            previousActiveTasks.clear();
            previousActiveTasks.AddAll(running.keySet());
            running.clear();
            created.clear();
            runningByPartition.clear();
            return firstException[];
        }

        private RuntimeException closeNonRunningTasks(Collection<T> tasks)
        {
            RuntimeException exception = null;
            foreach (T task in tasks)
            {
                try
                {

                    task.close(false, false);
                }
                catch (RuntimeException e)
                {
                    log.LogError("Failed to close {}, {}", taskTypeName, task.id(), e);
                    if (exception == null)
                    {
                        exception = e;
                    }
                }
            }
            return exception;
        }

        private RuntimeException suspendTasks(Collection<T> tasks)
        {
            AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
            for (Iterator<T> it = tasks.iterator(); it.hasNext();)
            {
                T task = it.next();
                try
                {

                    task.suspend();
                    suspended.Add(task.id(), task);
                }
                catch (TaskMigratedException closeAsZombieAndSwallow)
                {
                    // as we suspend a task, we are either shutting down or rebalancing, thus, we swallow and move on
                    log.LogInformation("Failed to suspend {} {} since it got migrated to another thread already. " +
                            "Closing it as zombie and move on.", taskTypeName, task.id());
                    firstException.compareAndSet(null, closeZombieTask(task));
                    it.Remove();
                }
                catch (RuntimeException e)
                {
                    log.LogError("Suspending {} {} failed due to the following error:", taskTypeName, task.id(), e);
                    firstException.compareAndSet(null, e);
                    try
                    {

                        task.close(false, false);
                    }
                    catch (RuntimeException f)
                    {
                        log.LogError("After suspending failed, closing the same {} {} failed again due to the following error:", taskTypeName, task.id(), f);
                    }
                }
            }
            return firstException[];
        }

        RuntimeException closeZombieTask(T task)
        {
            try
            {

                task.close(false, true);
            }
            catch (RuntimeException e)
            {
                log.LogWarning("Failed to close zombie {} {} due to {}; ignore and proceed.", taskTypeName, task.id(), e.ToString());
                return e;
            }
            return null;
        }

        bool hasRunningTasks()
        {
            return !running.isEmpty();
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        bool maybeResumeSuspendedTask(TaskId taskId, HashSet<TopicPartition> partitions)
        {
            if (suspended.ContainsKey(taskId))
            {
                T task = suspended[taskId];
                log.LogTrace("Found suspended {} {}", taskTypeName, taskId);
                if (task.partitions().Equals(partitions))
                {
                    suspended.Remove(taskId);
                    task.resume();
                    try
                    {

                        transitionToRunning(task);
                    }
                    catch (TaskMigratedException e)
                    {
                        // we need to catch migration exception internally since this function
                        // is triggered in the rebalance callback
                        log.LogInformation("Failed to resume {} {} since it got migrated to another thread already. " +
                                "Closing it as zombie before triggering a new rebalance.", taskTypeName, task.id());
                        RuntimeException fatalException = closeZombieTask(task);
                        running.Remove(task.id());
                        if (fatalException != null)
                        {
                            throw fatalException;
                        }
                        throw e;
                    }
                    log.LogTrace("Resuming suspended {} {}", taskTypeName, task.id());
                    return true;
                }
                else
                {

                    log.LogWarning("Couldn't resume task {} assigned partitions {}, task partitions {}", taskId, partitions, task.partitions());
                }
            }
            return false;
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        void transitionToRunning(T task)
        {
            log.LogDebug("Transitioning {} {} to running", taskTypeName, task.id());
            running.Add(task.id(), task);
            task.initializeTopology();
            foreach (TopicPartition topicPartition in task.partitions())
            {
                runningByPartition.Add(topicPartition, task);
            }
            foreach (TopicPartition topicPartition in task.changelogPartitions())
            {
                runningByPartition.Add(topicPartition, task);
            }
        }

        T runningTaskFor(TopicPartition partition)
        {
            return runningByPartition[partition];
        }

        HashSet<TaskId> runningTaskIds()
        {
            return running.keySet();
        }

        Dictionary<TaskId, T> runningTaskMap()
        {
            return Collections.unmodifiableMap(running);
        }


        public string ToString()
        {
            return ToString("");
        }

        public string ToString(string indent)
        {
            StringBuilder builder = new StringBuilder();
            describe(builder, running.values(), indent, "Running:");
            describe(builder, suspended.values(), indent, "Suspended:");
            describe(builder, created.values(), indent, "New:");
            return builder.ToString();
        }

        void describe(StringBuilder builder,
                      Collection<T> tasks,
                      string indent,
                      string name)
        {
            builder.Append(indent).Append(name);
            foreach (T t in tasks)
            {
                builder.Append(indent).Append(t.ToString(indent + "\t\t"));
            }
            builder.Append("\n");
        }

        List<T> allTasks()
        {
            List<T> tasks = new List<T>();
            tasks.AddAll(running.values());
            tasks.AddAll(suspended.values());
            tasks.AddAll(created.values());
            return tasks;
        }

        HashSet<TaskId> allAssignedTaskIds()
        {
            HashSet<TaskId> taskIds = new HashSet<>();
            taskIds.AddAll(running.keySet());
            taskIds.AddAll(suspended.keySet());
            taskIds.AddAll(created.keySet());
            return taskIds;
        }

        void clear()
        {
            runningByPartition.clear();
            running.clear();
            created.clear();
            suspended.clear();
        }

        HashSet<TaskId> previousTaskIds()
        {
            return previousActiveTasks;
        }

        /**
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */
        int commit()
        {
            int committed = 0;
            RuntimeException firstException = null;
            for (Iterator<T> it = running().iterator(); it.hasNext();)
            {
                T task = it.next();
                try
                {

                    if (task.commitNeeded())
                    {
                        task.commit();
                        committed++;
                    }
                }
                catch (TaskMigratedException e)
                {
                    log.LogInformation("Failed to commit {} {} since it got migrated to another thread already. " +
                            "Closing it as zombie before triggering a new rebalance.", taskTypeName, task.id());
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
                    log.LogError("Failed to commit {} {} due to the following error:",
                            taskTypeName,
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

        void closeNonAssignedSuspendedTasks(Dictionary<TaskId, HashSet<TopicPartition>> newAssignment)
        {
            Iterator<T> standByTaskIterator = suspended.values().iterator();
            while (standByTaskIterator.hasNext())
            {
                T suspendedTask = standByTaskIterator.next();
                if (!newAssignment.ContainsKey(suspendedTask.id()) || !suspendedTask.partitions().Equals(newAssignment[suspendedTask.id()])
    {
                    log.LogDebug("Closing suspended and not re-assigned {} {}", taskTypeName, suspendedTask.id());
                    try
                    {

                        suspendedTask.closeSuspended(true, false, null);
                    }
                    catch (Exception e)
                    {
                        log.LogError("Failed to Remove suspended {} {} due to the following error:", taskTypeName, suspendedTask.id(), e);
                    }
                    finally
                    {

                        standByTaskIterator.Remove();
                    }
                }
            }
        }

        void close(bool clean)
        {
            AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
            foreach (T task in allTasks())
            {
                try
                {

                    task.close(clean, false);
                }
                catch (TaskMigratedException e)
                {
                    log.LogInformation("Failed to close {} {} since it got migrated to another thread already. " +
                            "Closing it as zombie and move on.", taskTypeName, task.id());
                    firstException.compareAndSet(null, closeZombieTask(task));
                }
                catch (RuntimeException t)
                {
                    log.LogError("Failed while closing {} {} due to the following error:",
                              task.GetType().getSimpleName(),
                              task.id(),
                              t);
                    if (clean)
                    {
                        if (!closeUnclean(task))
                        {
                            firstException.compareAndSet(null, t);
                        }
                    }
                    else
                    {

                        firstException.compareAndSet(null, t);
                    }
                }
            }

            clear();

            RuntimeException fatalException = firstException[];
            if (fatalException != null)
            {
                throw fatalException;
            }
        }

        private bool closeUnclean(T task)
        {
            log.LogInformation("Try to close {} {} unclean.", task.GetType().getSimpleName(), task.id());
            try
            {

                task.close(false, false);
            }
            catch (RuntimeException fatalException)
            {
                log.LogError("Failed while closing {} {} due to the following error:",
                    task.GetType().getSimpleName(),
                    task.id(),
                    fatalException);
                return false;
            }

            return true;
        }
    }
}