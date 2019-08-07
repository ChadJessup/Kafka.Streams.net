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





using Kafka.Common.PartitionInfo;
using Kafka.Common.TopicPartition;
using Kafka.Common.errors.TimeoutException;
using Kafka.Common.Utils.LogContext;
using System.Collections.Generic;
using Confluent.Kafka;

public class StoreChangelogReader : ChangelogReader
{


    private ILogger log;
    private IConsumer<byte[], byte[]> restoreConsumer;
    private StateRestoreListener userStateRestoreListener;
    private Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
    private Dictionary<string, List<PartitionInfo>> partitionInfo = new HashMap<>();
    private Dictionary<TopicPartition, StateRestorer> stateRestorers = new HashMap<>();
    private HashSet<TopicPartition> needsRestoring = new HashSet<>();
    private HashSet<TopicPartition> needsInitializing = new HashSet<>();
    private HashSet<TopicPartition> completedRestorers = new HashSet<>();
    private TimeSpan pollTime;

    public StoreChangelogReader(IConsumer<byte[], byte[]> restoreConsumer,
                                TimeSpan pollTime,
                                StateRestoreListener userStateRestoreListener,
                                LogContext logContext)
{
        this.restoreConsumer = restoreConsumer;
        this.pollTime = pollTime;
        this.log = logContext.logger(GetType());
        this.userStateRestoreListener = userStateRestoreListener;
    }


    public void register(StateRestorer restorer)
{
        if (!stateRestorers.ContainsKey(restorer.partition()))
{
            restorer.setUserRestoreListener(userStateRestoreListener);
            stateRestorers.Add(restorer.partition(), restorer);

            log.LogTrace("Added restorer for changelog {}", restorer.partition());
        }

        needsInitializing.Add(restorer.partition());
    }

    public Collection<TopicPartition> restore(RestoringTasks active)
{
        if (!needsInitializing.isEmpty())
{
            initialize(active);
        }

        if (needsRestoring.isEmpty())
{
            restoreConsumer.unsubscribe();
            return completed();
        }

        try
{

            ConsumerRecords<byte[], byte[]> records = restoreConsumer.poll(pollTime);

            foreach (TopicPartition partition in needsRestoring)
{
                StateRestorer restorer = stateRestorers[partition];
                long pos = processNext(records.records(partition), restorer, endOffsets[partition]);
                restorer.setRestoredOffset(pos);
                if (restorer.hasCompleted(pos, endOffsets[partition]))
{
                    restorer.restoreDone();
                    endOffsets.Remove(partition);
                    completedRestorers.Add(partition);
                }
            }
        } catch (InvalidOffsetException recoverableException)
{
            log.LogWarning("Restoring StreamTasks failed. Deleting StreamTasks stores to recreate from scratch.", recoverableException);
            HashSet<TopicPartition> partitions = recoverableException.partitions();
            foreach (TopicPartition partition in partitions)
{
                StreamTask task = active.restoringTaskFor(partition);
                log.LogInformation("Reinitializing StreamTask {} for changelog {}", task, partition);

                needsInitializing.Remove(partition);
                needsRestoring.Remove(partition);

                StateRestorer restorer = stateRestorers[partition];
                restorer.setCheckpointOffset(StateRestorer.NO_CHECKPOINT);
                task.reinitializeStateStoresForPartitions(recoverableException.partitions());
            }
            restoreConsumer.seekToBeginning(partitions);
        }

        needsRestoring.removeAll(completedRestorers);

        if (needsRestoring.isEmpty())
{
            restoreConsumer.unsubscribe();
        }

        return completed();
    }

    private void initialize(RestoringTasks active)
{
        if (!restoreConsumer.subscription().isEmpty())
{
            throw new StreamsException("Restore consumer should not be subscribed to any topics (" + restoreConsumer.subscription() + ")");
        }

        // first refresh the changelog partition information from brokers, since initialize is only called when
        // the needsInitializing map is not empty, meaning we do not know the metadata for some of them yet
        refreshChangelogInfo();

        HashSet<TopicPartition> initializable = new HashSet<>();
        foreach (TopicPartition topicPartition in needsInitializing)
{
            if (hasPartition(topicPartition))
{
                initializable.Add(topicPartition);
            }
        }

        // try to fetch end offsets for the initializable restorers and Remove any partitions
        // where we already have all of the data
        try
{

            endOffsets.putAll(restoreConsumer.endOffsets(initializable));
        } catch (TimeoutException e)
{
            // if timeout exception gets thrown we just give up this time and retry in the next run loop
            log.LogDebug("Could not fetch end offset for {}; will fall back to partition by partition fetching", initializable);
            return;
        }

        IEnumerator<TopicPartition> iter = initializable.iterator();
        while (iter.hasNext())
{
            TopicPartition topicPartition = iter.next();
            long endOffset = endOffsets[topicPartition];

            // offset should not be null; but since the consumer API does not guarantee it
            // we.Add this check just in case
            if (endOffset != null)
{
                StateRestorer restorer = stateRestorers[topicPartition];
                if (restorer.checkpoint() >= endOffset)
{
                    restorer.setRestoredOffset(restorer.checkpoint());
                    iter.Remove();
                    completedRestorers.Add(topicPartition);
                } else if (restorer.offsetLimit() == 0 || endOffset == 0)
{
                    restorer.setRestoredOffset(0);
                    iter.Remove();
                    completedRestorers.Add(topicPartition);
                } else
{

                    restorer.setEndingOffset(endOffset);
                }
                needsInitializing.Remove(topicPartition);
            } else
{

                log.LogInformation("End offset cannot be found form the returned metadata; removing this partition from the current run loop");
                iter.Remove();
            }
        }

        // set up restorer for those initializable
        if (!initializable.isEmpty())
{
            startRestoration(initializable, active);
        }
    }

    private void startRestoration(Set<TopicPartition> initialized,
                                  RestoringTasks active)
{
        log.LogDebug("Start restoring state stores from changelog topics {}", initialized);

        HashSet<TopicPartition> assignment = new HashSet<TopicPartition>(restoreConsumer.assignment());
        assignment.addAll(initialized);
        restoreConsumer.assign(assignment);

        List<StateRestorer> needsPositionUpdate = new List<StateRestorer>();

        foreach (TopicPartition partition in initialized)
{
            StateRestorer restorer = stateRestorers[partition];
            if (restorer.checkpoint() != StateRestorer.NO_CHECKPOINT)
{
                log.LogTrace("Found checkpoint {} from changelog {} for store {}.", restorer.checkpoint(), partition, restorer.storeName());

                restoreConsumer.seek(partition, restorer.checkpoint());
                logRestoreOffsets(partition,
                        restorer.checkpoint(),
                        endOffsets[partition];
                restorer.setStartingOffset(restoreConsumer.position(partition));
                restorer.restoreStarted();
            } else
{

                log.LogTrace("Did not find checkpoint from changelog {} for store {}, rewinding to beginning.", partition, restorer.storeName());

                restoreConsumer.seekToBeginning(Collections.singletonList(partition));
                needsPositionUpdate.Add(restorer);
            }
        }

        foreach (StateRestorer restorer in needsPositionUpdate)
{
            TopicPartition partition = restorer.partition();

            // If checkpoint does not exist it means the task was not shutdown gracefully before;
            // and in this case if EOS is turned on we should wipe out the state and re-initialize the task
            StreamTask task = active.restoringTaskFor(partition);
            if (task.isEosEnabled())
{
                log.LogInformation("No checkpoint found for task {} state store {} changelog {} with EOS turned on. " +
                        "Reinitializing the task and restore its state from the beginning.", task.id, restorer.storeName(), partition);

                needsInitializing.Remove(partition);
                initialized.Remove(partition);
                restorer.setCheckpointOffset(restoreConsumer.position(partition));

                task.reinitializeStateStoresForPartitions(Collections.singleton(partition));
            } else
{

                log.LogInformation("Restoring task {}'s state store {} from beginning of the changelog {} ", task.id, restorer.storeName(), partition);

                long position = restoreConsumer.position(restorer.partition());
                logRestoreOffsets(restorer.partition(),
                        position,
                        endOffsets[restorer.partition()]);
                restorer.setStartingOffset(position);
                restorer.restoreStarted();
            }
        }

        needsRestoring.AddAll(initialized);
    }

    private void logRestoreOffsets(TopicPartition partition,
                                   long startingOffset,
                                   long endOffset)
{
        log.LogDebug("Restoring partition {} from offset {} to endOffset {}",
                  partition,
                  startingOffset,
                  endOffset);
    }

    private Collection<TopicPartition> completed()
{
        return completedRestorers;
    }

    private void refreshChangelogInfo()
{
        try
{

            partitionInfo.putAll(restoreConsumer.listTopics());
        } catch (TimeoutException e)
{
            log.LogDebug("Could not fetch topic metadata within the timeout, will retry in the next run loop");
        }
    }


    public Dictionary<TopicPartition, long> restoredOffsets()
{
        Dictionary<TopicPartition, long> restoredOffsets = new HashMap<>();
        foreach (KeyValuePair<TopicPartition, StateRestorer> entry in stateRestorers)
{
            StateRestorer restorer = entry.Value;
            if (restorer.isPersistent())
{
                restoredOffsets.Add(entry.Key, restorer.restoredOffset());
            }
        }
        return restoredOffsets;
    }


    public void reset()
{
        partitionInfo.clear();
        stateRestorers.clear();
        needsRestoring.clear();
        endOffsets.clear();
        needsInitializing.clear();
        completedRestorers.clear();
    }

    private long processNext(List<ConsumeResult<byte[], byte[]>> records,
                             StateRestorer restorer,
                             long endOffset)
{
        List<ConsumeResult<byte[], byte[]>> restoreRecords = new List<>();
        long nextPosition = -1;
        int numberRecords = records.size();
        int numberRestored = 0;
        long lastRestoredOffset = -1;
        foreach (ConsumeResult<byte[], byte[]> record in records)
{
            long offset = record.offset();
            if (restorer.hasCompleted(offset, endOffset))
{
                nextPosition = record.offset();
                break;
            }
            lastRestoredOffset = offset;
            numberRestored++;
            if (record.key() != null)
{
                restoreRecords.Add(record);
            }
        }


        // if we have changelog topic then we should have restored all records in the list
        // otherwise if we did not fully restore to that point we need to set nextPosition
        // to the position of the restoreConsumer and we'll cause a TaskMigratedException exception
        if (nextPosition == -1 || (restorer.offsetLimit() == long.MaxValue && numberRecords != numberRestored))
{
            nextPosition = restoreConsumer.position(restorer.partition());
        }

        if (!restoreRecords.isEmpty())
{
            restorer.restore(restoreRecords);
            restorer.restoreBatchCompleted(lastRestoredOffset, records.size());

            log.LogTrace("Restored from {} to {} with {} records, ending offset is {}, next starting position is {}",
                    restorer.partition(), restorer.storeName(), records.size(), lastRestoredOffset, nextPosition);
        }

        return nextPosition;
    }



    private bool hasPartition(TopicPartition topicPartition)
{
        List<PartitionInfo> partitions = partitionInfo[topicPartition.Topic];

        if (partitions == null)
{
            return false;
        }

        foreach (PartitionInfo partition in partitions)
{
            if (partition.partition() == topicPartition.partition())
{
                return true;
            }
        }

        return false;
    }
}
