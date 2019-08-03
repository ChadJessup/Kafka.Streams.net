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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
using Kafka.Common.PartitionInfo;
using Kafka.Common.TopicPartition;
using Kafka.Common.errors.TimeoutException;
using Kafka.Common.Utils.LogContext;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StoreChangelogReader implements ChangelogReader {

    private Logger log;
    private Consumer<byte[], byte[]> restoreConsumer;
    private StateRestoreListener userStateRestoreListener;
    private Dictionary<TopicPartition, Long> endOffsets = new HashMap<>();
    private Dictionary<string, List<PartitionInfo>> partitionInfo = new HashMap<>();
    private Dictionary<TopicPartition, StateRestorer> stateRestorers = new HashMap<>();
    private Set<TopicPartition> needsRestoring = new HashSet<>();
    private Set<TopicPartition> needsInitializing = new HashSet<>();
    private Set<TopicPartition> completedRestorers = new HashSet<>();
    private Duration pollTime;

    public StoreChangelogReader(Consumer<byte[], byte[]> restoreConsumer,
                                Duration pollTime,
                                StateRestoreListener userStateRestoreListener,
                                LogContext logContext) {
        this.restoreConsumer = restoreConsumer;
        this.pollTime = pollTime;
        this.log = logContext.logger(GetType());
        this.userStateRestoreListener = userStateRestoreListener;
    }

    @Override
    public void register(StateRestorer restorer) {
        if (!stateRestorers.containsKey(restorer.partition())) {
            restorer.setUserRestoreListener(userStateRestoreListener);
            stateRestorers.put(restorer.partition(), restorer);

            log.trace("Added restorer for changelog {}", restorer.partition());
        }

        needsInitializing.add(restorer.partition());
    }

    public Collection<TopicPartition> restore(RestoringTasks active) {
        if (!needsInitializing.isEmpty()) {
            initialize(active);
        }

        if (needsRestoring.isEmpty()) {
            restoreConsumer.unsubscribe();
            return completed();
        }

        try {
            ConsumerRecords<byte[], byte[]> records = restoreConsumer.poll(pollTime);

            for (TopicPartition partition : needsRestoring) {
                StateRestorer restorer = stateRestorers.get(partition);
                long pos = processNext(records.records(partition), restorer, endOffsets.get(partition));
                restorer.setRestoredOffset(pos);
                if (restorer.hasCompleted(pos, endOffsets.get(partition))) {
                    restorer.restoreDone();
                    endOffsets.remove(partition);
                    completedRestorers.add(partition);
                }
            }
        } catch (InvalidOffsetException recoverableException) {
            log.warn("Restoring StreamTasks failed. Deleting StreamTasks stores to recreate from scratch.", recoverableException);
            Set<TopicPartition> partitions = recoverableException.partitions();
            for (TopicPartition partition : partitions) {
                StreamTask task = active.restoringTaskFor(partition);
                log.info("Reinitializing StreamTask {} for changelog {}", task, partition);

                needsInitializing.remove(partition);
                needsRestoring.remove(partition);

                StateRestorer restorer = stateRestorers.get(partition);
                restorer.setCheckpointOffset(StateRestorer.NO_CHECKPOINT);
                task.reinitializeStateStoresForPartitions(recoverableException.partitions());
            }
            restoreConsumer.seekToBeginning(partitions);
        }

        needsRestoring.removeAll(completedRestorers);

        if (needsRestoring.isEmpty()) {
            restoreConsumer.unsubscribe();
        }

        return completed();
    }

    private void initialize(RestoringTasks active) {
        if (!restoreConsumer.subscription().isEmpty()) {
            throw new StreamsException("Restore consumer should not be subscribed to any topics (" + restoreConsumer.subscription() + ")");
        }

        // first refresh the changelog partition information from brokers, since initialize is only called when
        // the needsInitializing map is not empty, meaning we do not know the metadata for some of them yet
        refreshChangelogInfo();

        Set<TopicPartition> initializable = new HashSet<>();
        for (TopicPartition topicPartition : needsInitializing) {
            if (hasPartition(topicPartition)) {
                initializable.add(topicPartition);
            }
        }

        // try to fetch end offsets for the initializable restorers and remove any partitions
        // where we already have all of the data
        try {
            endOffsets.putAll(restoreConsumer.endOffsets(initializable));
        } catch (TimeoutException e) {
            // if timeout exception gets thrown we just give up this time and retry in the next run loop
            log.debug("Could not fetch end offset for {}; will fall back to partition by partition fetching", initializable);
            return;
        }

        Iterator<TopicPartition> iter = initializable.iterator();
        while (iter.hasNext()) {
            TopicPartition topicPartition = iter.next();
            Long endOffset = endOffsets.get(topicPartition);

            // offset should not be null; but since the consumer API does not guarantee it
            // we add this check just in case
            if (endOffset != null) {
                StateRestorer restorer = stateRestorers.get(topicPartition);
                if (restorer.checkpoint() >= endOffset) {
                    restorer.setRestoredOffset(restorer.checkpoint());
                    iter.remove();
                    completedRestorers.add(topicPartition);
                } else if (restorer.offsetLimit() == 0 || endOffset == 0) {
                    restorer.setRestoredOffset(0);
                    iter.remove();
                    completedRestorers.add(topicPartition);
                } else {
                    restorer.setEndingOffset(endOffset);
                }
                needsInitializing.remove(topicPartition);
            } else {
                log.info("End offset cannot be found form the returned metadata; removing this partition from the current run loop");
                iter.remove();
            }
        }

        // set up restorer for those initializable
        if (!initializable.isEmpty()) {
            startRestoration(initializable, active);
        }
    }

    private void startRestoration(Set<TopicPartition> initialized,
                                  RestoringTasks active) {
        log.debug("Start restoring state stores from changelog topics {}", initialized);

        Set<TopicPartition> assignment = new HashSet<>(restoreConsumer.assignment());
        assignment.addAll(initialized);
        restoreConsumer.assign(assignment);

        List<StateRestorer> needsPositionUpdate = new ArrayList<>();

        for (TopicPartition partition : initialized) {
            StateRestorer restorer = stateRestorers.get(partition);
            if (restorer.checkpoint() != StateRestorer.NO_CHECKPOINT) {
                log.trace("Found checkpoint {} from changelog {} for store {}.", restorer.checkpoint(), partition, restorer.storeName());

                restoreConsumer.seek(partition, restorer.checkpoint());
                logRestoreOffsets(partition,
                        restorer.checkpoint(),
                        endOffsets.get(partition));
                restorer.setStartingOffset(restoreConsumer.position(partition));
                restorer.restoreStarted();
            } else {
                log.trace("Did not find checkpoint from changelog {} for store {}, rewinding to beginning.", partition, restorer.storeName());

                restoreConsumer.seekToBeginning(Collections.singletonList(partition));
                needsPositionUpdate.add(restorer);
            }
        }

        for (StateRestorer restorer : needsPositionUpdate) {
            TopicPartition partition = restorer.partition();

            // If checkpoint does not exist it means the task was not shutdown gracefully before;
            // and in this case if EOS is turned on we should wipe out the state and re-initialize the task
            StreamTask task = active.restoringTaskFor(partition);
            if (task.isEosEnabled()) {
                log.info("No checkpoint found for task {} state store {} changelog {} with EOS turned on. " +
                        "Reinitializing the task and restore its state from the beginning.", task.id, restorer.storeName(), partition);

                needsInitializing.remove(partition);
                initialized.remove(partition);
                restorer.setCheckpointOffset(restoreConsumer.position(partition));

                task.reinitializeStateStoresForPartitions(Collections.singleton(partition));
            } else {
                log.info("Restoring task {}'s state store {} from beginning of the changelog {} ", task.id, restorer.storeName(), partition);

                long position = restoreConsumer.position(restorer.partition());
                logRestoreOffsets(restorer.partition(),
                        position,
                        endOffsets.get(restorer.partition()));
                restorer.setStartingOffset(position);
                restorer.restoreStarted();
            }
        }

        needsRestoring.addAll(initialized);
    }

    private void logRestoreOffsets(TopicPartition partition,
                                   long startingOffset,
                                   Long endOffset) {
        log.debug("Restoring partition {} from offset {} to endOffset {}",
                  partition,
                  startingOffset,
                  endOffset);
    }

    private Collection<TopicPartition> completed() {
        return completedRestorers;
    }

    private void refreshChangelogInfo() {
        try {
            partitionInfo.putAll(restoreConsumer.listTopics());
        } catch (TimeoutException e) {
            log.debug("Could not fetch topic metadata within the timeout, will retry in the next run loop");
        }
    }

    @Override
    public Dictionary<TopicPartition, Long> restoredOffsets() {
        Dictionary<TopicPartition, Long> restoredOffsets = new HashMap<>();
        for (Map.Entry<TopicPartition, StateRestorer> entry : stateRestorers.entrySet()) {
            StateRestorer restorer = entry.getValue();
            if (restorer.isPersistent()) {
                restoredOffsets.put(entry.getKey(), restorer.restoredOffset());
            }
        }
        return restoredOffsets;
    }

    @Override
    public void reset() {
        partitionInfo.clear();
        stateRestorers.clear();
        needsRestoring.clear();
        endOffsets.clear();
        needsInitializing.clear();
        completedRestorers.clear();
    }

    private long processNext(List<ConsumerRecord<byte[], byte[]>> records,
                             StateRestorer restorer,
                             Long endOffset) {
        List<ConsumerRecord<byte[], byte[]>> restoreRecords = new ArrayList<>();
        long nextPosition = -1;
        int numberRecords = records.size();
        int numberRestored = 0;
        long lastRestoredOffset = -1;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            long offset = record.offset();
            if (restorer.hasCompleted(offset, endOffset)) {
                nextPosition = record.offset();
                break;
            }
            lastRestoredOffset = offset;
            numberRestored++;
            if (record.key() != null) {
                restoreRecords.add(record);
            }
        }


        // if we have changelog topic then we should have restored all records in the list
        // otherwise if we did not fully restore to that point we need to set nextPosition
        // to the position of the restoreConsumer and we'll cause a TaskMigratedException exception
        if (nextPosition == -1 || (restorer.offsetLimit() == Long.MAX_VALUE && numberRecords != numberRestored)) {
            nextPosition = restoreConsumer.position(restorer.partition());
        }

        if (!restoreRecords.isEmpty()) {
            restorer.restore(restoreRecords);
            restorer.restoreBatchCompleted(lastRestoredOffset, records.size());

            log.trace("Restored from {} to {} with {} records, ending offset is {}, next starting position is {}",
                    restorer.partition(), restorer.storeName(), records.size(), lastRestoredOffset, nextPosition);
        }

        return nextPosition;
    }



    private bool hasPartition(TopicPartition topicPartition) {
        List<PartitionInfo> partitions = partitionInfo.get(topicPartition.topic());

        if (partitions == null) {
            return false;
        }

        for (PartitionInfo partition : partitions) {
            if (partition.partition() == topicPartition.partition()) {
                return true;
            }
        }

        return false;
    }
}
