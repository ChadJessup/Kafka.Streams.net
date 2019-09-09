//using Confluent.Kafka;
//using Kafka.Streams;
//using Kafka.Streams.Processor;
//using Kafka.Streams.Processor.Internals;
//using Kafka.Streams.State.Internals;
//using Kafka.Streams.Errors;
//using Kafka.Streams.Processor.Interfaces;
//using Microsoft.Extensions.Logging;
//using System;
//using System.Collections.Generic;
//using System.IO;
//using System.Linq;
//using System.Text;
//using Kafka.Common;
//using Kafka.Common.Extensions;
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.Processor.Internals
//{
//    /**
//     * This is responsible for the initialization, restoration, closing, flushing etc
//     * of Global State Stores. There is only ever 1 instance of this per Application Instance.
//     */
//    public class GlobalStateManagerImpl<K, V> : IGlobalStateManager<K, V>
//    {
//        private ILogger log;
//        private bool eosEnabled;
//        private ProcessorTopology<K, V> topology;
//        private IConsumer<byte[], byte[]> globalConsumer;
//        private FileInfo baseDir;
//        private StateDirectory stateDirectory;
//        private HashSet<string> globalStoreNames = new HashSet<string>();
//        private Dictionary<string, IStateStore?> globalStores = new Dictionary<string, IStateStore?>();
//        private IStateRestoreListener stateRestoreListener;
//        private IInternalProcessorContext<K, V> globalProcessorContext;
//        private int retries;
//        private long retryBackoffMs;
//        private TimeSpan pollTime;
//        private HashSet<string> globalNonPersistentStoresTopics = new HashSet<string>();
//        private OffsetCheckpoint checkpointFile;
//        private Dictionary<TopicPartition, long> checkpointFileCache;

//        DirectoryInfo IStateManager.baseDir { get; }

//        public GlobalStateManagerImpl(
//            LogContext logContext,
//            ProcessorTopology<K, V> topology,
//            IConsumer<byte[], byte[]> globalConsumer,
//            StateDirectory stateDirectory,
//            IStateRestoreListener stateRestoreListener,
//            StreamsConfig config)
//        {
//            eosEnabled = StreamsConfig.EXACTLY_ONCE.Equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));
//            baseDir = stateDirectory.globalStateDir();
//            //            checkpointFile = new OffsetCheckpoint(new FileInfo(Path.Combine(baseDir, StateManagerUtil.CHECKPOINT_FILE_NAME)));
//            checkpointFileCache = new Dictionary<TopicPartition, long>();

//            // Find non persistent store's topics
//            //          Dictionary<string, string> storeToChangelogTopic = topology.storeToChangelogTopic();
//            //foreach (IStateStore store in topology.globalStateStores())
//            //{
//            //    if (!store.persistent())
//            //    {
//            //        globalNonPersistentStoresTopics.Add(storeToChangelogTopic[store.name]);
//            //    }
//            //}

//            //            log = logContext.logger(GlobalStateManagerImpl);
//            this.topology = topology;
//            this.globalConsumer = globalConsumer;
//            this.stateDirectory = stateDirectory;
//            this.stateRestoreListener = stateRestoreListener;
//            //retries = config.GetInt(StreamsConfig.RETRIES_CONFIG);
//            //retryBackoffMs = config.getLong(StreamsConfig.RETRY_BACKOFF_MS_CONFIG);
//            //pollTime = Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG));
//        }

//        public void setGlobalProcessorContext(IInternalProcessorContext<K, V> globalProcessorContext)
//        {
//            this.globalProcessorContext = globalProcessorContext;
//        }

//        public HashSet<string> initialize()
//        {
//            try
//            {

//                if (!stateDirectory.lockGlobalState())
//                {
//                    throw new LockException(string.Format("Failed to lock the global state directory: %s", baseDir));
//                }
//            }
//            catch (IOException e)
//            {
//                //                throw new LockException(string.Format("Failed to lock the global state directory: %s", baseDir), e);
//            }

//            try
//            {

//                //              checkpointFileCache.putAll(checkpointFile.read());
//            }
//            catch (IOException e)
//            {
//                try
//                {

//                    stateDirectory.unlockGlobalState();
//                }
//                catch (IOException e1)
//                {
//                    log.LogError("Failed to unlock the global state directory", e);
//                }
//                throw new StreamsException("Failed to read checkpoints for global state globalStores", e);
//            }

//            //        List<IStateStore> stateStores = topology.globalStateStores();
//            //foreach (IStateStore stateStore in stateStores)
//            //{
//            //    globalStoreNames.Add(stateStore.name);
//            //    stateStore.init(globalProcessorContext, stateStore);
//            //}

//            return globalStoreNames;
//        }


//        public void reinitializeStateStoresForPartitions(List<TopicPartition> partitions,
//                                                         IInternalProcessorContext<K, V> processorContext)
//        {
//            StateManagerUtil.reinitializeStateStoresForPartitions(
//                log,
//                eosEnabled,
//                null, //baseDir,
//                globalStores,
//                null, //topology.storeToChangelogTopic(),
//                partitions,
//                processorContext,
//                checkpointFile,
//                checkpointFileCache
//            );

//            //globalConsumer.assign(partitions);
//            //globalConsumer.seekToBeginning(partitions);
//        }


//        public IStateStore getGlobalStore(string name)
//        {
//            return null; // globalStores.getOrDefault(name, Optional.empty()).orElse(null);
//        }


//        public IStateStore getStore(string name)
//        {
//            return getGlobalStore(name);
//        }

//        public void register(
//            IStateStore store,
//            IStateRestoreCallback stateRestoreCallback)
//        {

//            if (globalStores.ContainsKey(store.name))
//            {
//                throw new System.ArgumentException(string.Format("Global Store %s has already been registered", store.name));
//            }

//            if (!globalStoreNames.Contains(store.name))
//            {
//                throw new System.ArgumentException(string.Format("Trying to register store %s that is not a known global store", store.name));
//            }

//            if (stateRestoreCallback == null)
//            {
//                throw new System.ArgumentException(string.Format("The stateRestoreCallback provided for store %s was null", store.name));
//            }

//            log.LogInformation("Restoring state for global store {}", store.name);
//            List<TopicPartition> topicPartitions = topicPartitionsForStore(store);
//            Dictionary<TopicPartition, long> highWatermarks = null;

//            int attempts = 0;
//            while (highWatermarks == null)
//            {
//                try
//                {

//                    //                    highWatermarks = globalConsumer.endOffsets(topicPartitions);
//                }
//                catch (TimeoutException retryableException)
//                {
//                    if (++attempts > retries)
//                    {
//                        log.LogError("Failed to get end offsets for topic partitions of global store {} after {} retry attempts. " +
//                            "You can increase the number of retries via configuration parameter `retries`.",
//                            store.name,
//                            retries,
//                            retryableException);
//                        throw new StreamsException(string.Format("Failed to get end offsets for topic partitions of global store %s after %d retry attempts. " +
//                                "You can increase the number of retries via configuration parameter `retries`.", store.name, retries),
//                            retryableException);
//                    }
//                    log.LogDebug("Failed to get end offsets for partitions {}, backing off for {} ms to retry (attempt {} of {})",
//                        topicPartitions,
//                        retryBackoffMs,
//                        attempts,
//                        retries,
//                        retryableException);
//                    //                  Utils.sleep(retryBackoffMs);
//                }
//            }
//            try
//            {

//                restoreState(
//                    stateRestoreCallback,
//                    topicPartitions,
//                    highWatermarks,
//                    store.name,
//                    null //converterForStore(store)
//                );
//                //                globalStores.Add(store.name, Optional.of(store));
//            }
//            finally
//            {

//                //              globalConsumer.unsubscribe();
//            }

//        }

//        private List<TopicPartition> topicPartitionsForStore(IStateStore store)
//        {
//            //string sourceTopic = topology.storeToChangelogTopic()[store.name];
//            //List<PartitionInfo> partitionInfos;
//            int attempts = 0;
//            while (true)
//            {
//                try
//                {

//                    //partitionInfos = globalConsumer.partitionsFor(sourceTopic);
//                    break;
//                }
//                catch (TimeoutException retryableException)
//                {
//                    if (++attempts > retries)
//                    {
//                        log.LogError("Failed to get partitions for topic {} after {} retry attempts due to timeout. " +
//                                "The broker may be transiently unavailable at the moment. " +
//                                "You can increase the number of retries via configuration parameter `retries`.",
//                            null, //sourceTopic,
//                            retries,
//                            retryableException);
//                        //throw new StreamsException(string.Format("Failed to get partitions for topic %s after %d retry attempts due to timeout. " +
//                        //    "The broker may be transiently unavailable at the moment. " +
//                        //    "You can increase the number of retries via configuration parameter `retries`.", sourceTopic, retries),
//                        //    retryableException);
//                    }
//                    log.LogDebug("Failed to get partitions for topic {} due to timeout. The broker may be transiently unavailable at the moment. " +
//                            "Backing off for {} ms to retry (attempt {} of {})",
//                        null, //sourceTopic,
//                        retryBackoffMs,
//                        attempts,
//                        retries,
//                        retryableException);
//                    //                    Utils.sleep(retryBackoffMs);
//                }
//            }

//            //if (partitionInfos == null || !partitionInfos.Any())
//            //{
//            //    throw new StreamsException(string.Format("There are no partitions available for topic %s when initializing global store %s", sourceTopic, store.name));
//            //}

//            List<TopicPartition> topicPartitions = new List<TopicPartition>();
//            //foreach (PartitionInfo partition in partitionInfos)
//            //{
//            //    topicPartitions.Add(new TopicPartition(partition.Topic, partition.partition()));
//            //}

//            return topicPartitions;
//        }

//        private void restoreState(
//            IStateRestoreCallback stateRestoreCallback,
//            List<TopicPartition> topicPartitions,
//            Dictionary<TopicPartition, long> highWatermarks,
//            string storeName,
//            IRecordConverter recordConverter)
//        {
//            foreach (TopicPartition topicPartition in topicPartitions)
//            {
//                globalConsumer.Assign(topicPartition);
//                long checkpoint = checkpointFileCache[topicPartition];

//                if (checkpoint != null)
//                {
//                    globalConsumer.Seek(new TopicPartitionOffset(topicPartition, checkpoint));
//                }
//                else
//                {
//                    globalConsumer.Seek(new TopicPartitionOffset(topicPartition, new Offset(0)));
//                }

//                long offset = globalConsumer.Position(topicPartition);
//                long highWatermark = highWatermarks[topicPartition];
//                IRecordBatchingStateRestoreCallback stateRestoreAdapter =
//                    StateRestoreCallbackAdapter.adapt(stateRestoreCallback);

//                stateRestoreListener.onRestoreStart(topicPartition, storeName, offset, highWatermark);
//                long restoreCount = 0L;

//                while (offset < highWatermark)
//                {
//                    try
//                    {
//                        ConsumerRecords<byte[], byte[]> records = globalConsumer.poll(pollTime);
//                        List<ConsumeResult<byte[], byte[]>> restoreRecords = new List<ConsumeResult<byte[], byte[]>>();

//                        //foreach (ConsumeResult<byte[], byte[]> record in records.records(topicPartition))
//                        //{
//                        //    if (record.Key != null)
//                        //    {
//                        //        restoreRecords.Add(recordConverter.convert(record));
//                        //    }
//                        //}

//                        offset = globalConsumer.Position(topicPartition);
//                        stateRestoreAdapter.restoreBatch(restoreRecords);
//                        stateRestoreListener.onBatchRestored(topicPartition, storeName, offset, restoreRecords.Count);
//                        restoreCount += restoreRecords.Count;
//                    }
//                    catch (Exception recoverableException) //InvalidOffsetException
//                    {
//                        log.LogWarning("Restoring GlobalStore {} failed due to: {}. Deleting global store to recreate from scratch.",
//                            storeName,
//                            recoverableException.ToString());
//                        //                        reinitializeStateStoresForPartitions(recoverableException.partitions(), globalProcessorContext);

//                        stateRestoreListener.onRestoreStart(topicPartition, storeName, offset, highWatermark);
//                        restoreCount = 0L;
//                    }
//                }

//                stateRestoreListener.onRestoreEnd(topicPartition, storeName, restoreCount);
//                checkpointFileCache.Add(topicPartition, offset);
//            }
//        }

//        public void flush()
//        {
//            log.LogDebug("Flushing all global globalStores registered in the state manager");
//            foreach (KeyValuePair<string, IStateStore?> entry in globalStores)
//            {
//                if (entry.Value != null)
//                {
//                    IStateStore store = entry.Value;
//                    try
//                    {

//                        log.LogTrace("Flushing global store={}", store.name);
//                        store.flush();
//                    }
//                    catch (Exception e)
//                    {
//                        //throw new ProcessorStateException(
//                        //    string.Format("Failed to flush global state store %s", store.name), e);
//                    }
//                }
//                else
//                {

//                    throw new InvalidOperationException("Expected " + entry.Key + " to have been initialized");
//                }
//            }
//        }

//        public void close(bool clean)
//        {
//            try
//            {

//                if (!globalStores.Any())
//                {
//                    return;
//                }

//                StringBuilder closeFailed = new StringBuilder();
//                foreach (KeyValuePair<string, IStateStore?> entry in globalStores)
//                {
//                    if (true)//entry.Value.isPresent())
//                    {
//                        //    log.LogDebug("Closing global storage engine {}", entry.Key);
//                        //    try
//                        //    {

//                        //        entry.Value().close();
//                        //    }
//                        //    catch (RuntimeException e)
//                        //    {
//                        //        log.LogError("Failed to close global state store {}", entry.Key, e);
//                        //        closeFailed.Append("Failed to close global state store:")
//                        //                   .Append(entry.Key)
//                        //                   .Append(". Reason: ")
//                        //                   .Append(e)
//                        //                   .Append("\n");
//                        //    }

//                        //    globalStores.Add(entry.Key, null);
//                    }
//                    else
//                    {

//                        log.LogInformation("Skipping to close non-initialized store {}", entry.Key);
//                    }
//                }
//                //if (closeFailed.Length > 0)
//                //{
//                //    throw new ProcessorStateException("Exceptions caught during close of 1 or more global state globalStores\n" + closeFailed);
//                //}
//            }
//            finally
//            {

//                //stateDirectory.unlockGlobalState();
//            }
//        }

//        public void checkpoint(Dictionary<TopicPartition, long> offsets)
//        {
//            //checkpointFileCache.putAll(offsets);

//            Dictionary<TopicPartition, long> filteredOffsets = new Dictionary<TopicPartition, long>();

//            // Skip non persistent store
//            foreach (KeyValuePair<TopicPartition, long> topicPartitionOffset in checkpointFileCache)
//            {
//                string topic = topicPartitionOffset.Key.Topic;
//                if (!globalNonPersistentStoresTopics.Contains(topic))
//                {
//                    filteredOffsets.Add(topicPartitionOffset.Key, topicPartitionOffset.Value);
//                }
//            }

//            try
//            {

//                checkpointFile.write(filteredOffsets);
//            }
//            catch (IOException e)
//            {
//                log.LogWarning("Failed to write offset checkpoint file to {} for global stores: {}", checkpointFile, e);
//            }
//        }

//        public Dictionary<TopicPartition, long> checkpointed()
//        {
//            return checkpointFileCache;
//        }

//        public void reinitializeStateStoresForPartitions<K, V>(List<TopicPartition> partitions, IInternalProcessorContext<K, V> processorContext)
//        {
//            throw new NotImplementedException();
//        }
//    }
//}