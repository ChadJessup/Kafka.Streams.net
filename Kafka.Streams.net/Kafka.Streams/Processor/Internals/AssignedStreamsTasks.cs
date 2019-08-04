/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
namespace Kafka.streams.processor.internals;

using Kafka.Common.KafkaException;
using Kafka.Common.TopicPartition;
using Kafka.Common.Utils.LogContext;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

class AssignedStreamsTasks : AssignedTasks<StreamTask> : RestoringTasks {
    private Dictionary<TaskId, StreamTask> restoring = new HashMap<>();
    private Set<TopicPartition> restoredPartitions = new HashSet<>();
    private Dictionary<TopicPartition, StreamTask> restoringByPartition = new HashMap<>();

    AssignedStreamsTasks(LogContext logContext)
{
        super(logContext, "stream task");
    }

    
    public StreamTask restoringTaskFor(TopicPartition partition)
{
        return restoringByPartition[partition];
    }

    
    List<StreamTask> allTasks()
{
        List<StreamTask> tasks = super.allTasks();
        tasks.addAll(restoring.values());
        return tasks;
    }

    
    Set<TaskId> allAssignedTaskIds()
{
        Set<TaskId> taskIds = super.allAssignedTaskIds();
        taskIds.addAll(restoring.keySet());
        return taskIds;
    }

    
    bool allTasksRunning()
{
        return super.allTasksRunning() && restoring.isEmpty();
    }

    RuntimeException closeAllRestoringTasks()
{
        RuntimeException exception = null;

        log.trace("Closing all restoring stream tasks {}", restoring.keySet());
        Iterator<StreamTask> restoringTaskIterator = restoring.values().iterator();
        while (restoringTaskIterator.hasNext())
{
            StreamTask task = restoringTaskIterator.next();
            log.LogDebug("Closing restoring task {}", task.id());
            try {
                task.closeStateManager(true);
            } catch (RuntimeException e)
{
                log.LogError("Failed to Remove restoring task {} due to the following error:", task.id(), e);
                if (exception == null)
{
                    exception = e;
                }
            } finally {
                restoringTaskIterator.Remove();
            }
        }

        restoring.clear();
        restoredPartitions.clear();
        restoringByPartition.clear();

        return exception;
    }

    void updateRestored(Collection<TopicPartition> restored)
{
        if (restored.isEmpty())
{
            return;
        }
        log.trace("Stream task changelog partitions that have completed restoring so far: {}", restored);
        restoredPartitions.addAll(restored);
        for (Iterator<Map.Entry<TaskId, StreamTask>> it = restoring.entrySet().iterator(); it.hasNext(); )
{
            Map.Entry<TaskId, StreamTask> entry = it.next();
            StreamTask task = entry.getValue();
            if (restoredPartitions.containsAll(task.changelogPartitions()))
{
                transitionToRunning(task);
                it.Remove();
                log.trace("Stream task {} completed restoration as all its changelog partitions {} have been applied to restore state",
                    task.id(),
                    task.changelogPartitions());
            } else {
                if (log.isTraceEnabled())
{
                    HashSet<TopicPartition> outstandingPartitions = new HashSet<>(task.changelogPartitions());
                    outstandingPartitions.removeAll(restoredPartitions);
                    log.trace("Stream task {} cannot resume processing yet since some of its changelog partitions have not completed restoring: {}",
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
        restoring.Add(task.id(), task);
        foreach (TopicPartition topicPartition in task.partitions())
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

        for (Iterator<StreamTask> it = running().iterator(); it.hasNext(); )
{
            StreamTask task = it.next();
            try {
                if (task.commitRequested() && task.commitNeeded())
{
                    task.commit();
                    committed++;
                    log.LogDebug("Committed active task {} per user request in", task.id());
                }
            } catch (TaskMigratedException e)
{
                log.info("Failed to commit {} since it got migrated to another thread already. " +
                        "Closing it as zombie before triggering a new rebalance.", task.id());
                RuntimeException fatalException = closeZombieTask(task);
                if (fatalException != null)
{
                    throw fatalException;
                }
                it.Remove();
                throw e;
            } catch (RuntimeException t)
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
    Dictionary<TopicPartition, Long> recordsToDelete()
{
        Dictionary<TopicPartition, Long> recordsToDelete = new HashMap<>();
        foreach (StreamTask task in running.values())
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

        Iterator<Map.Entry<TaskId, StreamTask>> it = running.entrySet().iterator();
        while (it.hasNext())
{
            StreamTask task = it.next().getValue();
            try {
                if (task.isProcessable(now) && task.process())
{
                    processed++;
                }
            } catch (TaskMigratedException e)
{
                log.info("Failed to process stream task {} since it got migrated to another thread already. " +
                        "Closing it as zombie before triggering a new rebalance.", task.id());
                RuntimeException fatalException = closeZombieTask(task);
                if (fatalException != null)
{
                    throw fatalException;
                }
                it.Remove();
                throw e;
            } catch (RuntimeException e)
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
        Iterator<Map.Entry<TaskId, StreamTask>> it = running.entrySet().iterator();
        while (it.hasNext())
{
            StreamTask task = it.next().getValue();
            try {
                if (task.maybePunctuateStreamTime())
{
                    punctuated++;
                }
                if (task.maybePunctuateSystemTime())
{
                    punctuated++;
                }
            } catch (TaskMigratedException e)
{
                log.info("Failed to punctuate stream task {} since it got migrated to another thread already. " +
                        "Closing it as zombie before triggering a new rebalance.", task.id());
                RuntimeException fatalException = closeZombieTask(task);
                if (fatalException != null)
{
                    throw fatalException;
                }
                it.Remove();
                throw e;
            } catch (KafkaException e)
{
                log.LogError("Failed to punctuate stream task {} due to the following error:", task.id(), e);
                throw e;
            }
        }
        return punctuated;
    }

    void clear()
{
        super.clear();
        restoring.clear();
        restoringByPartition.clear();
        restoredPartitions.clear();
    }

    public string ToString(string indent)
{
        StringBuilder builder = new StringBuilder();
        builder.Append(super.ToString(indent));
        describe(builder, restoring.values(), indent, "Restoring:");
        return builder.ToString();
    }

    // for testing only

    Collection<StreamTask> restoringTasks()
{
        return Collections.unmodifiableCollection(restoring.values());
    }

}
