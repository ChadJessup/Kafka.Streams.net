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
namespace Kafka.Streams.Processor.Internals;





using Kafka.Common.Cluster;
using Kafka.Common.TopicPartition;
using Kafka.Common.Utils.LogContext;



















public class TaskManager
{

    // initialize the task list
    // activeTasks needs to be concurrent as it can be accessed
    // by QueryableState
    private ILogger log;
    private UUID processId;
    private AssignedStreamsTasks active;
    private AssignedStandbyTasks standby;
    private ChangelogReader changelogReader;
    private string logPrefix;
    private IConsumer<byte[], byte[]> restoreConsumer;
    private StreamThread.AbstractTaskCreator<StreamTask> taskCreator;
    private StreamThread.AbstractTaskCreator<StandbyTask> standbyTaskCreator;
    private StreamsMetadataState streamsMetadataState;

    Admin adminClient;
    private DeleteRecordsResult deleteRecordsResult;

    // following information is updated during rebalance phase by the partition assignor
    private Cluster cluster;
    private Dictionary<TaskId, HashSet<TopicPartition>> assignedActiveTasks;
    private Dictionary<TaskId, HashSet<TopicPartition>> assignedStandbyTasks;

    private IConsumer<byte[], byte[]> consumer;

    TaskManager(ChangelogReader changelogReader,
                UUID processId,
                string logPrefix,
                IConsumer<byte[], byte[]> restoreConsumer,
                StreamsMetadataState streamsMetadataState,
                StreamThread.AbstractTaskCreator<StreamTask> taskCreator,
                StreamThread.AbstractTaskCreator<StandbyTask> standbyTaskCreator,
                Admin adminClient,
                AssignedStreamsTasks active,
                AssignedStandbyTasks standby)
{
        this.changelogReader = changelogReader;
        this.processId = processId;
        this.logPrefix = logPrefix;
        this.streamsMetadataState = streamsMetadataState;
        this.restoreConsumer = restoreConsumer;
        this.taskCreator = taskCreator;
        this.standbyTaskCreator = standbyTaskCreator;
        this.active = active;
        this.standby = standby;

        LogContext logContext = new LogContext(logPrefix);

        this.log = logContext.logger(GetType());

        this.adminClient = adminClient;
    }

    void createTasks(List<TopicPartition> assignment)
{
        if (consumer == null)
{
            throw new InvalidOperationException(logPrefix + "consumer has not been initialized while.Adding stream tasks. This should not happen.");
        }

        // do this first as we may have suspended standby tasks that
        // will become active or vice versa
        standby.closeNonAssignedSuspendedTasks(assignedStandbyTasks);
        active.closeNonAssignedSuspendedTasks(assignedActiveTasks);

       .AddStreamTasks(assignment);
       .AddStandbyTasks();
        // Pause all the partitions until the underlying state store is ready for all the active tasks.
        log.LogTrace("Pausing partitions: {}", assignment);
        consumer.pause(assignment);
    }

    private void addStreamTasks(List<TopicPartition> assignment)
{
        if (assignedActiveTasks.isEmpty())
{
            return;
        }
        Dictionary<TaskId, HashSet<TopicPartition>> newTasks = new Dictionary<>();
        // collect newly assigned tasks and reopen re-assigned tasks
        log.LogDebug("Adding assigned tasks as active: {}", assignedActiveTasks);
        foreach (KeyValuePair<TaskId, HashSet<TopicPartition>> entry in assignedActiveTasks)
{
            TaskId taskId = entry.Key;
            HashSet<TopicPartition> partitions = entry.Value;

            if (assignment.containsAll(partitions))
{
                try
{

                    if (!active.maybeResumeSuspendedTask(taskId, partitions))
{
                        newTasks.Add(taskId, partitions);
                    }
                } catch (StreamsException e)
{
                    log.LogError("Failed to resume an active task {} due to the following error:", taskId, e);
                    throw e;
                }
            } else
{

                log.LogWarning("Task {} owned partitions {} are not contained in the assignment {}", taskId, partitions, assignment);
            }
        }

        if (newTasks.isEmpty())
{
            return;
        }

        // CANNOT FIND RETRY AND BACKOFF LOGIC
        // create all newly assigned tasks (guard against race condition with other thread via backoff and retry)
        // => other thread will call removeSuspendedTasks(); eventually
        log.LogTrace("New active tasks to be created: {}", newTasks);

        foreach (StreamTask task in taskCreator.createTasks(consumer, newTasks))
{
            active.AddNewTask(task);
        }
    }

    private void addStandbyTasks()
{
        Dictionary<TaskId, HashSet<TopicPartition>> assignedStandbyTasks = this.assignedStandbyTasks;
        if (assignedStandbyTasks.isEmpty())
{
            return;
        }
        log.LogDebug("Adding assigned standby tasks {}", assignedStandbyTasks);
        Dictionary<TaskId, HashSet<TopicPartition>> newStandbyTasks = new Dictionary<>();
        // collect newly assigned standby tasks and reopen re-assigned standby tasks
        foreach (KeyValuePair<TaskId, HashSet<TopicPartition>> entry in assignedStandbyTasks)
{
            TaskId taskId = entry.Key;
            HashSet<TopicPartition> partitions = entry.Value;
            if (!standby.maybeResumeSuspendedTask(taskId, partitions))
{
                newStandbyTasks.Add(taskId, partitions);
            }
        }

        if (newStandbyTasks.isEmpty())
{
            return;
        }

        // create all newly assigned standby tasks (guard against race condition with other thread via backoff and retry)
        // => other thread will call removeSuspendedStandbyTasks(); eventually
        log.LogTrace("New standby tasks to be created: {}", newStandbyTasks);

        foreach (StandbyTask task in standbyTaskCreator.createTasks(consumer, newStandbyTasks))
{
            standby.AddNewTask(task);
        }
    }

    HashSet<TaskId> activeTaskIds()
{
        return active.allAssignedTaskIds();
    }

    HashSet<TaskId> standbyTaskIds()
{
        return standby.allAssignedTaskIds();
    }

    public HashSet<TaskId> prevActiveTaskIds()
{
        return active.previousTaskIds();
    }

    /**
     * Returns ids of tasks whose states are kept on the local storage.
     */
    public HashSet<TaskId> cachedTasksIds()
{
        // A client could contain some inactive tasks whose states are still kept on the local storage in the following scenarios:
        // 1) the client is actively maintaining standby tasks by maintaining their states from the change log.
        // 2) the client has just got some tasks migrated out of itself to other clients while these task states
        //    have not been cleaned up yet (this can happen in a rolling bounce upgrade, for example).

        HashSet<TaskId> tasks = new HashSet<>();

        FileInfo[] stateDirs = taskCreator.stateDirectory().listTaskDirectories();
        if (stateDirs != null)
{
            foreach (FileInfo dir in stateDirs)
{
                try
{

                    TaskId id = TaskId.parse(dir.getName());
                    // if the checkpoint file exists, the state is valid.
                    if (new FileInfo(dir, StateManagerUtil.CHECKPOINT_FILE_NAME).exists())
{
                        tasks.Add(id);
                    }
                } catch (TaskIdFormatException e)
{
                    // there may be some unknown files that sits in the same directory,
                    // we should ignore these files instead trying to delete them as well
                }
            }
        }

        return tasks;
    }

    public UUID processId()
{
        return processId;
    }

    InternalTopologyBuilder builder()
{
        return taskCreator.builder();
    }

    /**
     * Similar to shutdownTasksAndState, however does not close the task managers, in the hope that
     * soon the tasks will be assigned again
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    void suspendTasksAndState()  {
        log.LogDebug("Suspending all active tasks {} and standby tasks {}", active.runningTaskIds(), standby.runningTaskIds());

        AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        firstException.compareAndSet(null, active.suspend());
        // close all restoring tasks as well and then reset changelog reader;
        // for those restoring and still assigned tasks, they will be re-created
        // in.AddStreamTasks.
        firstException.compareAndSet(null, active.closeAllRestoringTasks());
        changelogReader.reset();

        firstException.compareAndSet(null, standby.suspend());

        // Remove the changelog partitions from restore consumer
        restoreConsumer.unsubscribe();

        Exception exception = firstException[];
        if (exception != null)
{
            throw new StreamsException(logPrefix + "failed to suspend stream tasks", exception);
        }
    }

    void shutdown(bool clean)
{
        AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        log.LogDebug("Shutting down all active tasks {}, standby tasks {}, suspended tasks {}, and suspended standby tasks {}", active.runningTaskIds(), standby.runningTaskIds(),
                  active.previousTaskIds(), standby.previousTaskIds());

        try
{

            active.close(clean);
        } catch (RuntimeException fatalException)
{
            firstException.compareAndSet(null, fatalException);
        }
        standby.close(clean);

        // Remove the changelog partitions from restore consumer
        try
{

            restoreConsumer.unsubscribe();
        } catch (RuntimeException fatalException)
{
            firstException.compareAndSet(null, fatalException);
        }
        taskCreator.close();
        standbyTaskCreator.close();

        RuntimeException fatalException = firstException[];
        if (fatalException != null)
{
            throw fatalException;
        }
    }

    Admin getAdminClient()
{
        return adminClient;
    }

    HashSet<TaskId> suspendedActiveTaskIds()
{
        return active.previousTaskIds();
    }

    HashSet<TaskId> suspendedStandbyTaskIds()
{
        return standby.previousTaskIds();
    }

    StreamTask activeTask(TopicPartition partition)
{
        return active.runningTaskFor(partition);
    }

    StandbyTask standbyTask(TopicPartition partition)
{
        return standby.runningTaskFor(partition);
    }

    Dictionary<TaskId, StreamTask> activeTasks()
{
        return active.runningTaskMap();
    }

    Dictionary<TaskId, StandbyTask> standbyTasks()
{
        return standby.runningTaskMap();
    }

    void setConsumer(IConsumer<byte[], byte[]> consumer)
{
        this.consumer = consumer;
    }

    /**
     * @throws InvalidOperationException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     */
    bool updateNewAndRestoringTasks()
{
        active.initializeNewTasks();
        standby.initializeNewTasks();

        List<TopicPartition> restored = changelogReader.restore(active);

        active.updateRestored(restored);

        if (active.allTasksRunning())
{
            HashSet<TopicPartition> assignment = consumer.assignment();
            log.LogTrace("Resuming partitions {}", assignment);
            consumer.resume(assignment);
            assignStandbyPartitions();
            return standby.allTasksRunning();
        }
        return false;
    }

    bool hasActiveRunningTasks()
{
        return active.hasRunningTasks();
    }

    bool hasStandbyRunningTasks()
{
        return standby.hasRunningTasks();
    }

    private void assignStandbyPartitions()
{
        List<StandbyTask> running = standby.running();
        Dictionary<TopicPartition, long> checkpointedOffsets = new Dictionary<>();
        foreach (StandbyTask standbyTask in running)
{
            checkpointedOffsets.putAll(standbyTask.checkpointedOffsets());
        }

        restoreConsumer.assign(checkpointedOffsets.Keys);
        foreach (KeyValuePair<TopicPartition, long> entry in checkpointedOffsets)
{
            TopicPartition partition = entry.Key;
            long offset = entry.Value;
            if (offset >= 0)
{
                restoreConsumer.seek(partition, offset);
            } else
{

                restoreConsumer.seekToBeginning(singleton(partition));
            }
        }
    }

    public void setClusterMetadata(Cluster cluster)
{
        this.cluster = cluster;
    }

    public void setPartitionsByHostState(Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHostState)
{
        this.streamsMetadataState.onChange(partitionsByHostState, cluster);
    }

    public void setAssignmentMetadata(Dictionary<TaskId, HashSet<TopicPartition>> activeTasks,
                                      Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks)
{
        this.assignedActiveTasks = activeTasks;
        this.assignedStandbyTasks = standbyTasks;
    }

    public void updateSubscriptionsFromAssignment(List<TopicPartition> partitions)
{
        if (builder().sourceTopicPattern() != null)
{
            HashSet<string> assignedTopics = new HashSet<>();
            foreach (TopicPartition topicPartition in partitions)
{
                assignedTopics.Add(topicPartition.Topic);
            }

            List<string> existingTopics = builder().subscriptionUpdates().getUpdates();
            if (!existingTopics.containsAll(assignedTopics))
{
                assignedTopics.AddAll(existingTopics);
                builder().updateSubscribedTopics(assignedTopics, logPrefix);
            }
        }
    }

    public void updateSubscriptionsFromMetadata(HashSet<string> topics)
{
        if (builder().sourceTopicPattern() != null)
{
            List<string> existingTopics = builder().subscriptionUpdates().getUpdates();
            if (!existingTopics.Equals(topics))
{
                builder().updateSubscribedTopics(topics, logPrefix);
            }
        }
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    int commitAll()
{
        int committed = active.commit();
        return committed + standby.commit();
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    int process(long now)
{
        return active.process(now);
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    int punctuate()
{
        return active.punctuate();
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    int maybeCommitActiveTasksPerUserRequested()
{
        return active.maybeCommitPerUserRequested();
    }

    void maybePurgeCommitedRecords()
{
        // we do not check any possible exceptions since none of them are fatal
        // that should cause the application to fail, and we will try delete with
        // newer offsets anyways.
        if (deleteRecordsResult == null || deleteRecordsResult.all().isDone())
{

            if (deleteRecordsResult != null && deleteRecordsResult.all().isCompletedExceptionally())
{
                log.LogDebug("Previous delete-records request has failed: {}. Try sending the new request now", deleteRecordsResult.lowWatermarks());
            }

            Dictionary<TopicPartition, RecordsToDelete> recordsToDelete = new Dictionary<>();
            foreach (KeyValuePair<TopicPartition, long> entry in active.recordsToDelete())
{
                recordsToDelete.Add(entry.Key, RecordsToDelete.beforeOffset(entry.Value));
            }
            deleteRecordsResult = adminClient.deleteRecords(recordsToDelete);

            log.LogTrace("Sent delete-records request: {}", recordsToDelete);
        }
    }

    /**
     * Produces a string representation containing useful information about the TaskManager.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the TaskManager instance.
     */

    public string ToString()
{
        return ToString("");
    }

    public string ToString(string indent)
{
        StringBuilder builder = new StringBuilder();
        builder.Append("TaskManager\n");
        builder.Append(indent).Append("\tMetadataState:\n");
        builder.Append(streamsMetadataState.ToString(indent + "\t\t"));
        builder.Append(indent).Append("\tActive tasks:\n");
        builder.Append(active.ToString(indent + "\t\t"));
        builder.Append(indent).Append("\tStandby tasks:\n");
        builder.Append(standby.ToString(indent + "\t\t"));
        return builder.ToString();
    }

    // the following functions are for testing only
    Dictionary<TaskId, HashSet<TopicPartition>> assignedActiveTasks()
{
        return assignedActiveTasks;
    }

    Dictionary<TaskId, HashSet<TopicPartition>> assignedStandbyTasks()
{
        return assignedStandbyTasks;
    }
}
