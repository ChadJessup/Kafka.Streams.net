using Confluent.Kafka;
using Kafka.streams;
using Kafka.Streams.Processor;
using Kafka.Streams.Processor.Internals;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Errors;
using Kafka.Streams.Processor.Interfaces;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Kafka.Streams.Processor.Internals
{
/**
 * This class is responsible for the initialization, restoration, closing, flushing etc
 * of Global State Stores. There is only ever 1 instance of this class per Application Instance.
 */
public class GlobalStateManagerImpl : GlobalStateManager
    {
    private ILogger log;
    private bool eosEnabled;
    private ProcessorTopology topology;
    private IConsumer<byte[], byte[]> globalConsumer;
    private FileInfo baseDir;
    private StateDirectory stateDirectory;
    private HashSet<string> globalStoreNames = new HashSet<string>();
    private FixedOrderMap<string, Optional<IStateStore>> globalStores = new FixedOrderMap<>();
    private StateRestoreListener stateRestoreListener;
    private InternalProcessorContext globalProcessorContext;
    private int retries;
    private long retryBackoffMs;
    private TimeSpan pollTime;
    private HashSet<string> globalNonPersistentStoresTopics = new HashSet<string>();
    private OffsetCheckpoint checkpointFile;
    private Dictionary<TopicPartition, long> checkpointFileCache;

    public GlobalStateManagerImpl(LogContext logContext,
                                  ProcessorTopology topology,
                                  IConsumer<byte[], byte[]> globalConsumer,
                                  StateDirectory stateDirectory,
                                  StateRestoreListener stateRestoreListener,
                                  StreamsConfig config)
{
        eosEnabled = StreamsConfig.EXACTLY_ONCE.Equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));
        baseDir = stateDirectory.globalStateDir();
        checkpointFile = new OffsetCheckpoint(new File(baseDir, CHECKPOINT_FILE_NAME));
        checkpointFileCache = new Dictionary<TopicPartition, long>();

        // Find non persistent store's topics
        Dictionary<string, string> storeToChangelogTopic = topology.storeToChangelogTopic();
        foreach (IStateStore store in topology.globalStateStores())
{
            if (!store.persistent())
{
                globalNonPersistentStoresTopics.Add(storeToChangelogTopic[store.name]);
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

    public HashSet<string> initialize()
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
            globalStoreNames.Add(stateStore.name());
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
        Dictionary<TopicPartition, long> highWatermarks = null;

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
        string sourceTopic = topology.storeToChangelogTopic()[store.name());
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

        if (partitionInfos == null || !partitionInfos.Any())
{
            throw new StreamsException(string.Format("There are no partitions available for topic %s when initializing global store %s", sourceTopic, store.name()));
        }

        List<TopicPartition> topicPartitions = new List<TopicPartition>();
        foreach (PartitionInfo partition in partitionInfos)
{
            topicPartitions.Add(new TopicPartition(partition.topic(), partition.partition()));
        }
        return topicPartitions;
    }

    private void restoreState(
        StateRestoreCallback stateRestoreCallback,
        List<TopicPartition> topicPartitions,
        Dictionary<TopicPartition, long> highWatermarks,
        string storeName,
        RecordConverter recordConverter)
{
        foreach (TopicPartition topicPartition in topicPartitions)
{
            globalConsumer.assign(Collections.singletonList(topicPartition));
            long checkpoint = checkpointFileCache[topicPartition];
            if (checkpoint != null)
{
                globalConsumer.seek(topicPartition, checkpoint);
            } else {
                globalConsumer.seekToBeginning(Collections.singletonList(topicPartition));
            }

            long offset = globalConsumer.position(topicPartition);
            long highWatermark = highWatermarks[topicPartition];
            RecordBatchingStateRestoreCallback stateRestoreAdapter =
                StateRestoreCallbackAdapter.adapt(stateRestoreCallback);

            stateRestoreListener.onRestoreStart(topicPartition, storeName, offset, highWatermark);
            long restoreCount = 0L;

            while (offset < highWatermark)
{
                try {
                    ConsumerRecords<byte[], byte[]> records = globalConsumer.poll(pollTime);
                    List<ConsumerRecord<byte[], byte[]>> restoreRecords = new List<>();
                    foreach (ConsumerRecord<byte[], byte[]> record in records.records(topicPartition))
{
                        if (record.key() != null)
{
                            restoreRecords.Add(recordConverter.convert(record));
                        }
                    }
                    offset = globalConsumer.position(topicPartition);
                    stateRestoreAdapter.restoreBatch(restoreRecords);
                    stateRestoreListener.onBatchRestored(topicPartition, storeName, offset, restoreRecords.size());
                    restoreCount += restoreRecords.size();
                } catch (Exception recoverableException) //InvalidOffsetException
                {
                    log.LogWarning("Restoring GlobalStore {} failed due to: {}. Deleting global store to recreate from scratch.",
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
                IStateStore store = entry.getValue()[);
                try {
                    log.trace("Flushing global store={}", store.name());
                    store.flush();
                } catch (Exception e)
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

    public void close(bool clean)
    {
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


    public void checkpoint(Dictionary<TopicPartition, long> offsets)
{
        checkpointFileCache.putAll(offsets);

        Dictionary<TopicPartition, long> filteredOffsets = new Dictionary<TopicPartition, long>();

        // Skip non persistent store
        foreach (Map.Entry<TopicPartition, long> topicPartitionOffset in checkpointFileCache.entrySet())
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
            log.LogWarning("Failed to write offset checkpoint file to {} for global stores: {}", checkpointFile, e);
        }
    }


    public Dictionary<TopicPartition, long> checkpointed()
{
        return Collections.unmodifiableMap(checkpointFileCache);
    }


}
}