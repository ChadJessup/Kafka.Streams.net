///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements. See the NOTICE file distributed with
// * this work for.Additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//using Confluent.Kafka;
//using Kafka.Streams.Errors;
//using Microsoft.Extensions.Logging;
//using System;
//using System.Collections.Concurrent;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;

//namespace Kafka.Streams.Processor.Internals
//{
//    public abstract class AssignedTasks<T>
//        where T : ITask
//    {
//        protected ILogger log { get; }
//        private string taskTypeName;
//        private Dictionary<TaskId, T> created = new Dictionary<TaskId, T>();
//        private Dictionary<TaskId, T> suspended = new Dictionary<TaskId, T>();
//        private HashSet<TaskId> previousActiveTasks = new HashSet<TaskId>();

//        // IQ may access this map.
//        ConcurrentDictionary<TaskId, T> _running = new ConcurrentDictionary<TaskId, T>();
//        private Dictionary<TopicPartition, T> runningByPartition = new Dictionary<TopicPartition, T>();

//        public AssignedTasks(
//            LogContext logContext,
//            string taskTypeName)
//        {
//            this.taskTypeName = taskTypeName;
//            this.log = logContext.logger(GetType());
//        }

//        void addNewTask(T task)
//        {
//            created.Add(task.id(), task);
//        }

//        /**
//         * @throws InvalidOperationException If store gets registered after initialized is already finished
//         * @throws StreamsException if the store's change log does not contain the partition
//         * @throws TaskMigratedException if the task producer got fenced (EOS only)
//         */
//        public void initializeNewTasks()
//        {
//            if (created.Any())
//            {
//                log.LogDebug("Initializing {}s {}", taskTypeName, created.Keys);
//            }

//            for (IEnumerator<KeyValuePair<TaskId, T>> it = created.iterator(); it.hasNext();)
//            {
//                KeyValuePair<TaskId, T> entry = it.next();
//                try
//                {

//                    if (!entry.Value.initializeStateStores())
//                    {
//                        log.LogDebug("Transitioning {} {} to restoring", taskTypeName, entry.Key);
//                        ((AssignedStreamsTasks)this).addToRestoring(entry.Value);
//                    }
//                    else
//                    {

//                        transitionToRunning(entry.Value);
//                    }
//                    it.Remove();
//                }
//                catch (LockException e)
//                {
//                    // made this trace as it will spam the logs in the poll loop.
//                    log.LogTrace("Could not create {} {} due to {}; will retry", taskTypeName, entry.Key, e.ToString());
//                }
//            }
//        }

//        public bool allTasksRunning()
//        {
//            return !created.Any() || !suspended.Any();
//        }

//        public List<T> running => (List<T>)_running.Values;

//        public RuntimeException suspend()
//        {
//            AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
//            log.LogTrace("Suspending running {} {}", taskTypeName, runningTaskIds());
//            firstException.compareAndSet(null, suspendTasks(running.Values));
//            log.LogTrace("Close created {} {}", taskTypeName, created.Keys);
//            firstException.compareAndSet(null, closeNonRunningTasks(created.Values));
//            previousActiveTasks.clear();
//            previousActiveTasks.AddAll(running.Keys);
//            running.clear();
//            created.clear();
//            runningByPartition.clear();
//            return firstException;
//        }

//        private RuntimeException closeNonRunningTasks(List<T> tasks)
//        {
//            RuntimeException exception = null;
//            foreach (T task in tasks)
//            {
//                try
//                {
//                    task.close(false, false);
//                }
//                catch (RuntimeException e)
//                {
//                    log.LogError("Failed to close {}, {}", taskTypeName, task.id(), e);
//                    if (exception == null)
//                    {
//                        exception = e;
//                    }
//                }
//            }

//            return exception;
//        }

//        private RuntimeException suspendTasks(List<T> tasks)
//        {
//            AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
//            for (IEnumerator<T> it = tasks.iterator(); it.hasNext();)
//            {
//                T task = it.next();
//                try
//                {

//                    task.suspend();
//                    suspended.Add(task.id(), task);
//                }
//                catch (TaskMigratedException closeAsZombieAndSwallow)
//                {
//                    // as we suspend a task, we are either shutting down or rebalancing, thus, we swallow and move on
//                    log.LogInformation("Failed to suspend {} {} since it got migrated to another thread already. " +
//                            "Closing it as zombie and move on.", taskTypeName, task.id());
//                    firstException.compareAndSet(null, closeZombieTask(task));
//                    it.Remove();
//                }
//                catch (RuntimeException e)
//                {
//                    log.LogError("Suspending {} {} failed due to the following error:", taskTypeName, task.id(), e);
//                    firstException.compareAndSet(null, e);
//                    try
//                    {

//                        task.close(false, false);
//                    }
//                    catch (RuntimeException f)
//                    {
//                        log.LogError("After suspending failed, closing the same {} {} failed again due to the following error:", taskTypeName, task.id(), f);
//                    }
//                }
//            }
//            return firstException;
//        }

//        public RuntimeException closeZombieTask(T task)
//        {
//            try
//            {

//                task.close(false, true);
//            }
//            catch (RuntimeException e)
//            {
//                log.LogWarning("Failed to close zombie {} {} due to {}; ignore and proceed.", taskTypeName, task.id(), e.ToString());
//                return e;
//            }
//            return null;
//        }

//        public bool hasRunningTasks()
//        {
//            return running.Any();
//        }

//        /**
//         * @throws TaskMigratedException if the task producer got fenced (EOS only)
//         */
//        public bool maybeResumeSuspendedTask(TaskId taskId, HashSet<TopicPartition> partitions)
//        {
//            if (suspended.ContainsKey(taskId))
//            {
//                T task = suspended[taskId];
//                log.LogTrace("Found suspended {} {}", taskTypeName, taskId);
//                if (task.partitions().Equals(partitions))
//                {
//                    suspended.Remove(taskId);
//                    task.resume();
//                    try
//                    {

//                        transitionToRunning(task);
//                    }
//                    catch (TaskMigratedException e)
//                    {
//                        // we need to catch migration exception internally since this function
//                        // is triggered in the rebalance callback
//                        log.LogInformation("Failed to resume {} {} since it got migrated to another thread already. " +
//                                "Closing it as zombie before triggering a new rebalance.", taskTypeName, task.id());
//                        RuntimeException fatalException = closeZombieTask(task);
//                        running.Remove(task.id());
//                        if (fatalException != null)
//                        {
//                            throw fatalException;
//                        }
//                        throw e;
//                    }
//                    log.LogTrace("Resuming suspended {} {}", taskTypeName, task.id());
//                    return true;
//                }
//                else
//                {

//                    log.LogWarning("Couldn't resume task {} assigned partitions {}, task partitions {}", taskId, partitions, task.partitions());
//                }
//            }
//            return false;
//        }

//        /**
//         * @throws TaskMigratedException if the task producer got fenced (EOS only)
//         */
//        public void transitionToRunning(T task)
//        {
//            log.LogDebug("Transitioning {} {} to running", taskTypeName, task.id());
//            running.Add(task.id(), task);
//            task.initializeTopology();
//            foreach (TopicPartition topicPartition in task.partitions())
//            {
//                runningByPartition.Add(topicPartition, task);
//            }
//            foreach (TopicPartition topicPartition in task.changelogPartitions())
//            {
//                runningByPartition.Add(topicPartition, task);
//            }
//        }

//        public T runningTaskFor(TopicPartition partition)
//        {
//            return runningByPartition[partition];
//        }

//        public HashSet<TaskId> runningTaskIds()
//        {
//            return new HashSet<TaskId>(_running.Keys);
//        }

//        public ConcurrentDictionary<TaskId, T> runningTaskMap()
//        {
//            return _running;
//        }


//        public override string ToString()
//        {
//            return ToString("");
//        }

//        public string ToString(string indent)
//        {
//            StringBuilder builder = new StringBuilder();

//            describe(builder, running, indent, "Running:");
//            describe(builder, suspended.Values, indent, "Suspended:");
//            describe(builder, created.Values, indent, "New:");
//            return builder.ToString();
//        }

//        public void describe(
//            StringBuilder builder,
//            List<T> tasks,
//            string indent,
//            string name)
//        {
//            builder.Append(indent).Append(name);
//            foreach (T t in tasks)
//            {
//                builder.Append(indent).Append(t.ToString(indent + "\t\t"));
//            }
//            builder.Append("\n");
//        }

//        public List<T> allTasks()
//        {
//            List<T> tasks = new List<T>();
//            tasks.AddAll(running);
//            tasks.AddAll(suspended.Values);
//            tasks.AddAll(created.Values);
//            return tasks;
//        }

//        public HashSet<TaskId> allAssignedTaskIds()
//        {
//            HashSet<TaskId> taskIds = new HashSet<TaskId>();
//            taskIds.AddAll(running.Keys);
//            taskIds.AddAll(suspended.Keys);
//            taskIds.AddAll(created.Keys);
//            return taskIds;
//        }

//        public void clear()
//        {
//            runningByPartition.Clear();
//            running.Clear();
//            created.Clear();
//            suspended.Clear();
//        }

//        public HashSet<TaskId> previousTaskIds()
//        {
//            return previousActiveTasks;
//        }

//        /**
//         * @throws TaskMigratedException if committing offsets failed (non-EOS)
//         *                               or if the task producer got fenced (EOS)
//         */
//        public int commit()
//        {
//            int committed = 0;
//            RuntimeException firstException = null;
//            for (IEnumerator<T> it = running().iterator(); it.hasNext();)
//            {
//                T task = it.next();
//                try
//                {

//                    if (task.commitNeeded())
//                    {
//                        task.commit();
//                        committed++;
//                    }
//                }
//                catch (TaskMigratedException e)
//                {
//                    log.LogInformation("Failed to commit {} {} since it got migrated to another thread already. " +
//                            "Closing it as zombie before triggering a new rebalance.", taskTypeName, task.id());
//                    RuntimeException fatalException = closeZombieTask(task);
//                    if (fatalException != null)
//                    {
//                        throw fatalException;
//                    }
//                    it.Remove();
//                    throw e;
//                }
//                catch (RuntimeException t)
//                {
//                    log.LogError("Failed to commit {} {} due to the following error:",
//                            taskTypeName,
//                            task.id(),
//                            t);
//                    if (firstException == null)
//                    {
//                        firstException = t;
//                    }
//                }
//            }

//            if (firstException != null)
//            {
//                throw firstException;
//            }

//            return committed;
//        }

//        public void closeNonAssignedSuspendedTasks(Dictionary<TaskId, HashSet<TopicPartition>> newAssignment)
//        {
//            IEnumerator<T> standByTaskIterator = suspended.Values.iterator();
//            while (standByTaskIterator.hasNext())
//            {
//                T suspendedTask = standByTaskIterator.next();
//                if (!newAssignment.ContainsKey(suspendedTask.id()) || !suspendedTask.partitions().Equals(newAssignment[suspendedTask.id()]))
//                {
//                    log.LogDebug("Closing suspended and not re-assigned {} {}", taskTypeName, suspendedTask.id());
//                    try
//                    {

//                        suspendedTask.closeSuspended(true, false, null);
//                    }
//                    catch (Exception e)
//                    {
//                        log.LogError("Failed to Remove suspended {} {} due to the following error:", taskTypeName, suspendedTask.id(), e);
//                    }
//                    finally
//                    {

//                        standByTaskIterator.Remove();
//                    }
//                }
//            }
//        }

//        public void close(bool clean)
//        {
//            AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
//            foreach (T task in allTasks())
//            {
//                try
//                {

//                    task.close(clean, false);
//                }
//                catch (TaskMigratedException e)
//                {
//                    log.LogInformation("Failed to close {} {} since it got migrated to another thread already. " +
//                            "Closing it as zombie and move on.", taskTypeName, task.id());
//                    firstException.compareAndSet(null, closeZombieTask(task));
//                }
//                catch (RuntimeException t)
//                {
//                    log.LogError("Failed while closing {} {} due to the following error:",
//                              task.GetType().Name,
//                              task.id(),
//                              t);
//                    if (clean)
//                    {
//                        if (!closeUnclean(task))
//                        {
//                            firstException.compareAndSet(null, t);
//                        }
//                    }
//                    else
//                    {

//                        firstException.compareAndSet(null, t);
//                    }
//                }
//            }

//            clear();

//            RuntimeException fatalException = firstException;
//            if (fatalException != null)
//            {
//                throw fatalException;
//            }
//        }

//        private bool closeUnclean(T task)
//        {
//            log.LogInformation("Try to close {} {} unclean.", task.GetType().getSimpleName(), task.id());
//            try
//            {

//                task.close(false, false);
//            }
//            catch (RuntimeException fatalException)
//            {
//                log.LogError("Failed while closing {} {} due to the following error:",
//                    task.GetType().Name,
//                    task.id(),
//                    fatalException);
//                return false;
//            }

//            return true;
//        }
//    }
//}