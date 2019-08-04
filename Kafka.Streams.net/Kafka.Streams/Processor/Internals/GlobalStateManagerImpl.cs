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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
using Kafka.Common.PartitionInfo;
using Kafka.Common.TopicPartition;
using Kafka.Common.errors.TimeoutException;
using Kafka.Common.Utils.FixedOrderMap;
using Kafka.Common.Utils.LogContext;
using Kafka.Common.Utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.IStateStore;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.streams.state.internals.RecordConverter;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.StateManagerUtil.CHECKPOINT_FILE_NAME;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.converterForStore;

/**
 * This class is responsible for the initialization, restoration, closing, flushing etc
 * of Global State Stores. There is only ever 1 instance of this class per Application Instance.
 */
public class GlobalStateManagerImpl : GlobalStateManager {
    private Logger log;
    private bool eosEnabled;
    private ProcessorTopology topology;
    private Consumer<byte[], byte[]> globalConsumer;
    private File baseDir;
    private StateDirectory stateDirectory;
    private Set<string> globalStoreNames = new HashSet<>();
    private FixedOrderMap<string, Optional<IStateStore>> globalStores = new FixedOrderMap<>();
    private StateRestoreListener stateRestoreListener;
    private InternalProcessorContext globalProcessorContext;
    private int retries;
    private long retryBackoffMs;
    private Duration pollTime;
    private Set<string> globalNonPersistentStoresTopics = new HashSet<>();
    private OffsetCheckpoint checkpointFile;
    private Dictionary<TopicPartition, Long> checkpointFileCache;

    public GlobalStateManagerImpl(LogContext logContext,
                                  ProcessorTopology topology,
                                  Consumer<byte[], byte[]> globalConsumer,
                                  StateDirectory stateDirectory,
                                  StateRestoreListener stateRestoreListener,
                                  StreamsConfig config)
{
        eosEnabled = StreamsConfig.EXACTLY_ONCE.Equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));
        baseDir = stateDirectory.globalStateDir();
        checkpointFile = new OffsetCheckpoint(new File(baseDir, CHECKPOINT_FILE_NAME));
        checkpointFileCache = new HashMap<>();

        // Find non persistent store's topics
        Dictionary<string, string> storeToChangelogTopic = topology.storeToChangelogTopic();
        foreach (IStateStore store in topology.globalStateStores())
{
            if (!store.persistent())
{
                globalNonPersistentStoresTopics.add(storeToChangelogTopic[store.name())];
            }
        }

        log = logContext.logger(GlobalStateManagerImpl.class);
        this.topology = topology;
        this.globalConsumer = globalConsumer;
        this.stateDirectory = stateDirectory;
        this.stateRestoreListener = stateRestoreListener;
        retries = config.getInt(StreamsConfig.RETRIES_CONFIG);
        retryBackoffMs = config.getLong(StreamsConfig.RETRY_BACKOFF_MS_CONFIG);
        pollTime = Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG));
    }

    
    public void setGlobalProcessorContext(InternalProcessorContext globalProcessorContext)
{
        this.globalProcessorContext = globalProcessorContext;
    }

    
    public Set<string> initialize()
{
        try {
            if (!stateDirectory.lockGlobalState())
{
                throw new LockException(string.Format("Failed to lock the global state directory: %s", baseDir));
            }
        } catch (IOException e)
{
            throw new LockException(string.Format("Failed to lock the global state directory: %s", baseDir), e);
        }

        try {
            checkpointFileCache.putAll(checkpointFile.read());
        } catch (IOException e)
{
            try {
                stateDirectory.unlockGlobalState();
            } catch (IOException e1)
{
                log.LogError("Failed to unlock the global state directory", e);
            }
            throw new StreamsException("Failed to read checkpoints for global state globalStores", e);
        }

        List<IStateStore> stateStores = topology.globalStateStores();
        foreach (IStateStore stateStore in stateStores)
{
            globalStoreNames.add(stateStore.name());
            stateStore.init(globalProcessorContext, stateStore);
        }
        return Collections.unmodifiableSet(globalStoreNames);
    }

    
    public void reinitializeStateStoresForPartitions(Collection<TopicPartition> partitions,
                                                     InternalProcessorContext processorContext)
{
        StateManagerUtil.reinitializeStateStoresForPartitions(
            log,
            eosEnabled,
            baseDir,
            globalStores,
            topology.storeToChangelogTopic(),
            partitions,
            processorContext,
            checkpointFile,
            checkpointFileCache
        );

        globalConsumer.assign(partitions);
        globalConsumer.seekToBeginning(partitions);
    }

    
    public IStateStore getGlobalStore(string name)
{
        return globalStores.getOrDefault(name, Optional.empty()).orElse(null);
    }

    
    public IStateStore getStore(string name)
{
        return getGlobalStore(name);
    }

    public File baseDir()
{
        return baseDir;
    }

    public void register(IStateStore store,
                         StateRestoreCallback stateRestoreCallback)
{

        if (globalStores.ContainsKey(store.name()))
{
            throw new ArgumentException(string.Format("Global Store %s has already been registered", store.name()));
        }

        if (!globalStoreNames.contains(store.name()))
{
            throw new ArgumentException(string.Format("Trying to register store %s that is not a known global store", store.name()));
        }

        if (stateRestoreCallback == null)
{
            throw new ArgumentException(string.Format("The stateRestoreCallback provided for store %s was null", store.name()));
        }

        log.info("Restoring state for global store {}", store.name());
        List<TopicPartition> topicPartitions = topicPartitionsForStore(store);
        Dictionary<TopicPartition, Long> highWatermarks = null;

        int attempts = 0;
        while (highWatermarks == null)
{
            try {
                highWatermarks = globalConsumer.endOffsets(topicPartitions);
            } catch (TimeoutException retryableException)
{
                if (++attempts > retries)
{
                    log.LogError("Failed to get end offsets for topic partitions of global store {} after {} retry attempts. " +
                        "You can increase the number of retries via configuration parameter `retries`.",
                        store.name(),
                        retries,
                        retryableException);
                    throw new StreamsException(string.Format("Failed to get end offsets for topic partitions of global store %s after %d retry attempts. " +
                            "You can increase the number of retries via configuration parameter `retries`.", store.name(), retries),
                        retryableException);
                }
                log.LogDebug("Failed to get end offsets for partitions {}, backing off for {} ms to retry (attempt {} of {})",
                    topicPartitions,
                    retryBackoffMs,
                    attempts,
                    retries,
                    retryableException);
                Utils.sleep(retryBackoffMs);
            }
        }
        try {
            restoreState(
                stateRestoreCallback,
                topicPartitions,
                highWatermarks,
                store.name(),
                converterForStore(store)
            );
            globalStores.Add(store.name(), Optional.of(store));
        } finally {
            globalConsumer.unsubscribe();
        }

    }

    private List<TopicPartition> topicPartitionsForStore(IStateStore store)
{
        string sourceTopic = topology.storeToChangelogTopic()[store.name()];
        List<PartitionInfo> partitionInfos;
        int attempts = 0;
        while (true)
{
            try {
                partitionInfos = globalConsumer.partitionsFor(sourceTopic);
                break;
            } catch (TimeoutException retryableException)
{
                if (++attempts > retries)
{
                    log.LogError("Failed to get partitions for topic {} after {} retry attempts due to timeout. " +
                            "The broker may be transiently unavailable at the moment. " +
                            "You can increase the number of retries via configuration parameter `retries`.",
                        sourceTopic,
                        retries,
                        retryableException);
                    throw new StreamsException(string.Format("Failed to get partitions for topic %s after %d retry attempts due to timeout. " +
                        "The broker may be transiently unavailable at the moment. " +
                        "You can increase the number of retries via configuration parameter `retries`.", sourceTopic, retries),
                        retryableException);
                }
                log.LogDebug("Failed to get partitions for topic {} due to timeout. The broker may be transiently unavailable at the moment. " +
                        "Backing off for {} ms to retry (attempt {} of {})",
                    sourceTopic,
                    retryBackoffMs,
                    attempts,
                    retries,
                    retryableException);
                Utils.sleep(retryBackoffMs);
            }
        }

        if (partitionInfos == null || partitionInfos.isEmpty())
{
            throw new StreamsException(string.Format("There are no partitions available for topic %s when initializing global store %s", sourceTopic, store.name()));
        }

        List<TopicPartition> topicPartitions = new List<>();
        foreach (PartitionInfo partition in partitionInfos)
{
            topicPartitions.add(new TopicPartition(partition.topic(), partition.partition()));
        }
        return topicPartitions;
    }

    private void restoreState(StateRestoreCallback stateRestoreCallback,
                              List<TopicPartition> topicPartitions,
                              Dictionary<TopicPartition, Long> highWatermarks,
                              string storeName,
                              RecordConverter recordConverter)
{
        foreach (TopicPartition topicPartition in topicPartitions)
{
            globalConsumer.assign(Collections.singletonList(topicPartition));
            Long checkpoint = checkpointFileCache[topicPartition];
            if (checkpoint != null)
{
                globalConsumer.seek(topicPartition, checkpoint);
            } else {
                globalConsumer.seekToBeginning(Collections.singletonList(topicPartition));
            }

            long offset = globalConsumer.position(topicPartition);
            Long highWatermark = highWatermarks[topicPartition];
            RecordBatchingStateRestoreCallback stateRestoreAdapter =
                StateRestoreCallbackAdapter.adapt(stateRestoreCallback);

            stateRestoreListener.onRestoreStart(topicPartition, storeName, offset, highWatermark);
            long restoreCount = 0L;

            while (offset < highWatermark)
{
                try {
                    ConsumerRecords<byte[], byte[]> records = globalConsumer.poll(pollTime];
                    List<ConsumerRecord<byte[], byte[]>> restoreRecords = new List<>();
                    foreach (ConsumerRecord<byte[], byte[]> record in records.records(topicPartition))
{
                        if (record.key() != null)
{
                            restoreRecords.add(recordConverter.convert(record));
                        }
                    }
                    offset = globalConsumer.position(topicPartition);
                    stateRestoreAdapter.restoreBatch(restoreRecords);
                    stateRestoreListener.onBatchRestored(topicPartition, storeName, offset, restoreRecords.size());
                    restoreCount += restoreRecords.size();
                } catch (InvalidOffsetException recoverableException)
{
                    log.warn("Restoring GlobalStore {} failed due to: {}. Deleting global store to recreate from scratch.",
                        storeName,
                        recoverableException.ToString());
                    reinitializeStateStoresForPartitions(recoverableException.partitions(), globalProcessorContext);

                    stateRestoreListener.onRestoreStart(topicPartition, storeName, offset, highWatermark);
                    restoreCount = 0L;
                }
            }
            stateRestoreListener.onRestoreEnd(topicPartition, storeName, restoreCount);
            checkpointFileCache.Add(topicPartition, offset);
        }
    }

    
    public void flush()
{
        log.LogDebug("Flushing all global globalStores registered in the state manager");
        foreach (Map.Entry<string, Optional<IStateStore>> entry in globalStores.entrySet())
{
            if (entry.getValue().isPresent())
{
                IStateStore store = entry.getValue()[];
                try {
                    log.trace("Flushing global store={}", store.name());
                    store.flush();
                } catch (RuntimeException e)
{
                    throw new ProcessorStateException(
                        string.Format("Failed to flush global state store %s", store.name()),
                        e
                    );
                }
            } else {
                throw new InvalidOperationException("Expected " + entry.getKey() + " to have been initialized");
            }
        }
    }


    
    public void close(bool clean) throws IOException {
        try {
            if (globalStores.isEmpty())
{
                return;
            }
            StringBuilder closeFailed = new StringBuilder();
            foreach (Map.Entry<string, Optional<IStateStore>> entry in globalStores.entrySet())
{
                if (entry.getValue().isPresent())
{
                    log.LogDebug("Closing global storage engine {}", entry.getKey());
                    try {
                        entry.getValue()().close();
                    } catch (RuntimeException e)
{
                        log.LogError("Failed to close global state store {}", entry.getKey(), e);
                        closeFailed.Append("Failed to close global state store:")
                                   .Append(entry.getKey())
                                   .Append(". Reason: ")
                                   .Append(e)
                                   .Append("\n");
                    }
                    globalStores.Add(entry.getKey(), Optional.empty());
                } else {
                    log.info("Skipping to close non-initialized store {}", entry.getKey());
                }
            }
            if (closeFailed.Length > 0)
{
                throw new ProcessorStateException("Exceptions caught during close of 1 or more global state globalStores\n" + closeFailed);
            }
        } finally {
            stateDirectory.unlockGlobalState();
        }
    }

    
    public void checkpoint(Dictionary<TopicPartition, Long> offsets)
{
        checkpointFileCache.putAll(offsets);

        Dictionary<TopicPartition, Long> filteredOffsets = new HashMap<>();

        // Skip non persistent store
        foreach (Map.Entry<TopicPartition, Long> topicPartitionOffset in checkpointFileCache.entrySet())
{
            string topic = topicPartitionOffset.getKey().topic();
            if (!globalNonPersistentStoresTopics.contains(topic))
{
                filteredOffsets.Add(topicPartitionOffset.getKey(), topicPartitionOffset.getValue());
            }
        }

        try {
            checkpointFile.write(filteredOffsets);
        } catch (IOException e)
{
            log.warn("Failed to write offset checkpoint file to {} for global stores: {}", checkpointFile, e);
        }
    }

    
    public Dictionary<TopicPartition, Long> checkpointed()
{
        return Collections.unmodifiableMap(checkpointFileCache);
    }


}
