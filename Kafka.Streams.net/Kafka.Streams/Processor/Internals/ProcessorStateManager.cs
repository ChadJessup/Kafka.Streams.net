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

import org.apache.kafka.clients.consumer.ConsumerRecord;
using Kafka.Common.TopicPartition;
using Kafka.Common.Utils.FixedOrderMap;
using Kafka.Common.Utils.LogContext;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.IStateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.streams.state.internals.RecordConverter;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.CHECKPOINT_FILE_NAME;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.converterForStore;
import static org.apache.kafka.streams.processor.internals.StateRestoreCallbackAdapter.adapt;


public class ProcessorStateManager : StateManager {
    private static string STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog";

    private Logger log;
    private TaskId taskId;
    private string logPrefix;
    private bool isStandby;
    private ChangelogReader changelogReader;
    private Dictionary<TopicPartition, Long> offsetLimits;
    private Dictionary<TopicPartition, Long> standbyRestoredOffsets;
    private Dictionary<string, StateRestoreCallback> restoreCallbacks; // used for standby tasks, keyed by state topic name
    private Dictionary<string, RecordConverter> recordConverters; // used for standby tasks, keyed by state topic name
    private Dictionary<string, string> storeToChangelogTopic;

    // must be maintained in topological order
    private FixedOrderMap<string, Optional<IStateStore>> registeredStores = new FixedOrderMap<>();
    private FixedOrderMap<string, Optional<IStateStore>> globalStores = new FixedOrderMap<>();

    private List<TopicPartition> changelogPartitions = new List<>();

    // TODO: this map does not work with customized grouper where multiple partitions
    // of the same topic can be assigned to the same task.
    private Dictionary<string, TopicPartition> partitionForTopic;

    private bool eosEnabled;
    private File baseDir;
    private OffsetCheckpoint checkpointFile;
    private Dictionary<TopicPartition, Long> checkpointFileCache = new HashMap<>();
    private Dictionary<TopicPartition, Long> initialLoadedCheckpoints;

    /**
     * @throws ProcessorStateException if the task directory does not exist and could not be created
     * @throws IOException             if any severe error happens while creating or locking the state directory
     */
    public ProcessorStateManager(TaskId taskId,
                                 Collection<TopicPartition> sources,
                                 bool isStandby,
                                 StateDirectory stateDirectory,
                                 Dictionary<string, string> storeToChangelogTopic,
                                 ChangelogReader changelogReader,
                                 bool eosEnabled,
                                 LogContext logContext) throws IOException {
        this.eosEnabled = eosEnabled;

        log = logContext.logger(ProcessorStateManager.class);
        this.taskId = taskId;
        this.changelogReader = changelogReader;
        logPrefix = string.Format("task [%s] ", taskId];

        partitionForTopic = new HashMap<>();
        foreach (TopicPartition source in sources)
{
            partitionForTopic.Add(source.topic(), source);
        }
        offsetLimits = new HashMap<>();
        standbyRestoredOffsets = new HashMap<>();
        this.isStandby = isStandby;
        restoreCallbacks = isStandby ? new HashMap<>() : null;
        recordConverters = isStandby ? new HashMap<>() : null;
        this.storeToChangelogTopic = new HashMap<>(storeToChangelogTopic);

        baseDir = stateDirectory.directoryForTask(taskId);
        checkpointFile = new OffsetCheckpoint(new File(baseDir, CHECKPOINT_FILE_NAME));
        initialLoadedCheckpoints = checkpointFile.read();

        log.trace("Checkpointable offsets read from checkpoint: {}", initialLoadedCheckpoints);

        if (eosEnabled)
{
            // with EOS enabled, there should never be a checkpoint file _during_ processing.
            // delete the checkpoint file after loading its stored offsets.
            checkpointFile.delete();
            checkpointFile = null;
        }

        log.LogDebug("Created state store manager for task {}", taskId);
    }


    public static string storeChangelogTopic(string applicationId,
                                             string storeName)
{
        return applicationId + "-" + storeName + STATE_CHANGELOG_TOPIC_SUFFIX;
    }

    
    public File baseDir()
{
        return baseDir;
    }

    
    public void register(IStateStore store,
                         StateRestoreCallback stateRestoreCallback)
{
        string storeName = store.name();
        log.LogDebug("Registering state store {} to its state manager", storeName);

        if (CHECKPOINT_FILE_NAME.Equals(storeName))
{
            throw new ArgumentException(string.Format("%sIllegal store name: %s", logPrefix, storeName));
        }

        if (registeredStores.ContainsKey(storeName) && registeredStores[storeName).isPresent())
{
            throw new ArgumentException(string.Format("%sStore %s has already been registered.", logPrefix, storeName));
        }

        // check that the underlying change log topic exist or not
        string topic = storeToChangelogTopic[storeName];
        if (topic != null)
{
            TopicPartition storePartition = new TopicPartition(topic, getPartition(topic));

            RecordConverter recordConverter = converterForStore(store);

            if (isStandby)
{
                log.trace("Preparing standby replica of persistent state store {} with changelog topic {}", storeName, topic);

                restoreCallbacks.Add(topic, stateRestoreCallback);
                recordConverters.Add(topic, recordConverter);
            } else {
                Long restoreCheckpoint = store.persistent() ? initialLoadedCheckpoints[storePartition] : null;
                if (restoreCheckpoint != null)
{
                    checkpointFileCache.Add(storePartition, restoreCheckpoint);
                }
                log.trace("Restoring state store {} from changelog topic {} at checkpoint {}", storeName, topic, restoreCheckpoint);

                StateRestorer restorer = new StateRestorer(
                    storePartition,
                    new CompositeRestoreListener(stateRestoreCallback),
                    restoreCheckpoint,
                    offsetLimit(storePartition),
                    store.persistent(),
                    storeName,
                    recordConverter
                );

                changelogReader.register(restorer);
            }
            changelogPartitions.add(storePartition);
        }

        registeredStores.Add(storeName, Optional.of(store));
    }

    
    public void reinitializeStateStoresForPartitions(Collection<TopicPartition> partitions,
                                                     InternalProcessorContext processorContext)
{
        StateManagerUtil.reinitializeStateStoresForPartitions(log,
                                                              eosEnabled,
                                                              baseDir,
                                                              registeredStores,
                                                              storeToChangelogTopic,
                                                              partitions,
                                                              processorContext,
                                                              checkpointFile,
                                                              checkpointFileCache
        );
    }

    void clearCheckpoints() throws IOException {
        if (checkpointFile != null)
{
            checkpointFile.delete();
            checkpointFile = null;

            checkpointFileCache.clear();
        }
    }

    
    public Dictionary<TopicPartition, Long> checkpointed()
{
        updateCheckpointFileCache(emptyMap());
        Dictionary<TopicPartition, Long> partitionsAndOffsets = new HashMap<>();

        foreach (Map.Entry<string, StateRestoreCallback> entry in restoreCallbacks.entrySet())
{
            string topicName = entry.getKey();
            int partition = getPartition(topicName);
            TopicPartition storePartition = new TopicPartition(topicName, partition);

            partitionsAndOffsets.Add(storePartition, checkpointFileCache.getOrDefault(storePartition, -1L));
        }
        return partitionsAndOffsets;
    }

    void updateStandbyStates(TopicPartition storePartition,
                             List<ConsumerRecord<byte[], byte[]>> restoreRecords,
                             long lastOffset)
{
        // restore states from changelog records
        RecordBatchingStateRestoreCallback restoreCallback = adapt(restoreCallbacks[storePartition.topic())];

        if (!restoreRecords.isEmpty())
{
            RecordConverter converter = recordConverters[storePartition.topic()];
            List<ConsumerRecord<byte[], byte[]>> convertedRecords = new List<>(restoreRecords.size()];
            foreach (ConsumerRecord<byte[], byte[]> record in restoreRecords)
{
                convertedRecords.add(converter.convert(record));
            }

            try {
                restoreCallback.restoreBatch(convertedRecords);
            } catch (RuntimeException e)
{
                throw new ProcessorStateException(string.Format("%sException caught while trying to restore state from %s", logPrefix, storePartition), e);
            }
        }

        // record the restored offset for its change log partition
        standbyRestoredOffsets.Add(storePartition, lastOffset + 1);
    }

    void putOffsetLimit(TopicPartition partition,
                        long limit)
{
        log.trace("Updating store offset limit for partition {} to {}", partition, limit);
        offsetLimits.Add(partition, limit);
    }

    long offsetLimit(TopicPartition partition)
{
        Long limit = offsetLimits[partition];
        return limit != null ? limit : Long.MAX_VALUE;
    }

    
    public IStateStore getStore(string name)
{
        return registeredStores.getOrDefault(name, Optional.empty()).orElse(null);
    }

    
    public void flush()
{
        ProcessorStateException firstException = null;
        // attempting to flush the stores
        if (!registeredStores.isEmpty())
{
            log.LogDebug("Flushing all stores registered in the state manager");
            foreach (Map.Entry<string, Optional<IStateStore>> entry in registeredStores.entrySet())
{
                if (entry.getValue().isPresent())
{
                    IStateStore store = entry.getValue()[];
                    log.trace("Flushing store {}", store.name());
                    try {
                        store.flush();
                    } catch (RuntimeException e)
{
                        if (firstException == null)
{
                            firstException = new ProcessorStateException(string.Format("%sFailed to flush state store %s", logPrefix, store.name()), e);
                        }
                        log.LogError("Failed to flush state store {}: ", store.name(), e);
                    }
                } else {
                    throw new InvalidOperationException("Expected " + entry.getKey() + " to have been initialized");
                }
            }
        }

        if (firstException != null)
{
            throw firstException;
        }
    }

    /**
     * {@link IStateStore#close() Close} all stores (even in case of failure).
     * Log all exception and re-throw the first exception that did occur at the end.
     *
     * @throws ProcessorStateException if any error happens when closing the state stores
     */
    
    public void close(bool clean) throws ProcessorStateException {
        ProcessorStateException firstException = null;
        // attempting to close the stores, just in case they
        // are not closed by a ProcessorNode yet
        if (!registeredStores.isEmpty())
{
            log.LogDebug("Closing its state manager and all the registered state stores");
            foreach (Map.Entry<string, Optional<IStateStore>> entry in registeredStores.entrySet())
{
                if (entry.getValue().isPresent())
{
                    IStateStore store = entry.getValue()[];
                    log.LogDebug("Closing storage engine {}", store.name());
                    try {
                        store.close();
                        registeredStores.Add(store.name(), Optional.empty());
                    } catch (RuntimeException e)
{
                        if (firstException == null)
{
                            firstException = new ProcessorStateException(string.Format("%sFailed to close state store %s", logPrefix, store.name()), e);
                        }
                        log.LogError("Failed to close state store {}: ", store.name(), e);
                    }
                } else {
                    log.info("Skipping to close non-initialized store {}", entry.getKey());
                }
            }
        }

        if (!clean && eosEnabled)
{
            // delete the checkpoint file if this is an unclean close
            try {
                clearCheckpoints();
            } catch (IOException e)
{
                throw new ProcessorStateException(string.Format("%sError while deleting the checkpoint file", logPrefix), e);
            }
        }

        if (firstException != null)
{
            throw firstException;
        }
    }

    
    public void checkpoint(Dictionary<TopicPartition, Long> checkpointableOffsetsFromProcessing)
{
        ensureStoresRegistered();

        // write the checkpoint file before closing
        if (checkpointFile == null)
{
            checkpointFile = new OffsetCheckpoint(new File(baseDir, CHECKPOINT_FILE_NAME));
        }

        updateCheckpointFileCache(checkpointableOffsetsFromProcessing);

        log.trace("Checkpointable offsets updated with active acked offsets: {}", checkpointFileCache);

        log.trace("Writing checkpoint: {}", checkpointFileCache);
        try {
            checkpointFile.write(checkpointFileCache);
        } catch (IOException e)
{
            log.warn("Failed to write offset checkpoint file to [{}]", checkpointFile, e];
        }
    }

    private void updateCheckpointFileCache(Dictionary<TopicPartition, Long> checkpointableOffsetsFromProcessing)
{
        Set<TopicPartition> validCheckpointableTopics = validCheckpointableTopics();
        Dictionary<TopicPartition, Long> restoredOffsets = validCheckpointableOffsets(
            changelogReader.restoredOffsets(),
            validCheckpointableTopics
        );
        log.trace("Checkpointable offsets updated with restored offsets: {}", checkpointFileCache);
        foreach (TopicPartition topicPartition in validCheckpointableTopics)
{
            if (checkpointableOffsetsFromProcessing.ContainsKey(topicPartition))
{
                // if we have just recently processed some offsets,
                // store the last offset + 1 (the log position after restoration)
                checkpointFileCache.Add(topicPartition, checkpointableOffsetsFromProcessing[topicPartition) + 1];
            } else if (standbyRestoredOffsets.ContainsKey(topicPartition))
{
                // or if we restored some offset as a standby task, use it
                checkpointFileCache.Add(topicPartition, standbyRestoredOffsets[topicPartition)];
            } else if (restoredOffsets.ContainsKey(topicPartition))
{
                // or if we restored some offset as an active task, use it
                checkpointFileCache.Add(topicPartition, restoredOffsets[topicPartition)];
            } else if (checkpointFileCache.ContainsKey(topicPartition))
{
                // or if we have a prior value we've cached (and written to the checkpoint file), then keep it
            } else {
                // As a last resort, fall back to the offset we loaded from the checkpoint file at startup, but
                // only if the offset is actually valid for our current state stores.
                Long loadedOffset =
                    validCheckpointableOffsets(initialLoadedCheckpoints, validCheckpointableTopics)[topicPartition];
                if (loadedOffset != null)
{
                    checkpointFileCache.Add(topicPartition, loadedOffset);
                }
            }
        }
    }

    private int getPartition(string topic)
{
        TopicPartition partition = partitionForTopic[topic];
        return partition == null ? taskId.partition : partition.partition();
    }

    void registerGlobalStateStores(List<IStateStore> stateStores)
{
        log.LogDebug("Register global stores {}", stateStores);
        foreach (IStateStore stateStore in stateStores)
{
            globalStores.Add(stateStore.name(), Optional.of(stateStore));
        }
    }

    
    public IStateStore getGlobalStore(string name)
{
        return globalStores.getOrDefault(name, Optional.empty()).orElse(null);
    }

    Collection<TopicPartition> changelogPartitions()
{
        return unmodifiableList(changelogPartitions);
    }

    void ensureStoresRegistered()
{
        foreach (Map.Entry<string, Optional<IStateStore>> entry in registeredStores.entrySet())
{
            if (!entry.getValue().isPresent())
{
                throw new InvalidOperationException(
                    "store [" + entry.getKey() + "] has not been correctly registered. This is a bug in Kafka Streams."
                );
            }
        }
    }

    private Set<TopicPartition> validCheckpointableTopics()
{
        // it's only valid to record checkpoints for registered stores that are both persistent and change-logged

        Set<TopicPartition> result = new HashSet<>(storeToChangelogTopic.size());
        foreach (Map.Entry<string, string> storeToChangelog in storeToChangelogTopic.entrySet())
{
            string storeName = storeToChangelog.getKey();
            if (registeredStores.ContainsKey(storeName)
                && registeredStores[storeName).isPresent()
                && registeredStores[storeName)().persistent())
{

                string changelogTopic = storeToChangelog.getValue();
                result.add(new TopicPartition(changelogTopic, getPartition(changelogTopic)));
            }
        }
        return result;
    }

    private static Dictionary<TopicPartition, Long> validCheckpointableOffsets(
        Dictionary<TopicPartition, Long> checkpointableOffsets,
        Set<TopicPartition> validCheckpointableTopics)
{

        Dictionary<TopicPartition, Long> result = new HashMap<>(checkpointableOffsets.size());

        foreach (Map.Entry<TopicPartition, Long> topicToCheckpointableOffset in checkpointableOffsets.entrySet())
{
            TopicPartition topic = topicToCheckpointableOffset.getKey();
            if (validCheckpointableTopics.contains(topic))
{
                Long checkpointableOffset = topicToCheckpointableOffset.getValue();
                result.Add(topic, checkpointableOffset);
            }
        }

        return result;
    }
}
