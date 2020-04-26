using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Clients;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Kafka.Streams.Processors.Internals
{
    /**
     * This is responsible for the initialization, restoration, closing, flushing etc
     * of Global State Stores. There is only ever 1 instance of this per Application Instance.
     */
    public class GlobalStateManager : IGlobalStateManager
    {
        private readonly ILogger<GlobalStateManager> logger;
        private readonly IKafkaClientSupplier clientSupplier;
        private readonly StreamsConfig config;

        private readonly bool eosEnabled;
        private readonly ProcessorTopology? topology;
        private readonly IConsumer<byte[], byte[]> globalConsumer;
        private readonly StateDirectory stateDirectory;
        private readonly HashSet<string> globalStoreNames = new HashSet<string>();
        private readonly Dictionary<string, IStateStore?> globalStores = new Dictionary<string, IStateStore?>();
        private readonly IStateRestoreListener stateRestoreListener;
        private IInternalProcessorContext globalProcessorContext;
        private readonly int retries;
        private readonly long retryBackoffMs;
        private readonly TimeSpan pollTime;
        private readonly HashSet<string> globalNonPersistentStoresTopics = new HashSet<string>();
        private readonly OffsetCheckpoint checkpointFile;
        private readonly Dictionary<TopicPartition, long?> checkpointFileCache;

        public GlobalStateManager(
            ILogger<GlobalStateManager> logger,
            ProcessorTopology? topology,
            IKafkaClientSupplier clientSupplier,
            IConsumer<byte[], byte[]> globalConsumer,
            StateDirectory stateDirectory,
            IStateRestoreListener stateRestoreListener,
            StreamsConfig config)
        {
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            stateDirectory = stateDirectory ?? throw new ArgumentNullException(nameof(stateDirectory));
            this.clientSupplier = clientSupplier ?? throw new ArgumentNullException(nameof(clientSupplier));

            this.eosEnabled = config.EnableIdempotence;
            this.BaseDir = stateDirectory.GlobalStateDir();
            this.checkpointFileCache = new Dictionary<TopicPartition, long?>();
            this.checkpointFile = new OffsetCheckpoint(new FileInfo(Path.Combine(this.BaseDir.FullName, StateManagerUtil.CHECKPOINT_FILE_NAME)));

            // Find non Persistent store's topics
            var storeToChangelogTopic = topology?.StoreToChangelogTopic ?? new Dictionary<string, string>();
            foreach (var store in topology?.globalStateStores ?? Enumerable.Empty<IStateStore>())
            {
                if (!store.Persistent())
                {
                    this.globalNonPersistentStoresTopics.Add(storeToChangelogTopic[store.Name]);
                }
            }

            this.topology = topology;
            this.globalConsumer = globalConsumer;
            this.stateDirectory = stateDirectory;
            this.stateRestoreListener = stateRestoreListener;

            this.retries = config.Retries;
            this.retryBackoffMs = config.RetryBackoffMs;

            this.pollTime = TimeSpan.FromMilliseconds(config.PollMs);
        }

        public DirectoryInfo BaseDir { get; }

        public void SetGlobalProcessorContext(IInternalProcessorContext globalProcessorContext)
        {
            this.globalProcessorContext = globalProcessorContext;
        }

        public HashSet<string> Initialize()
        {
            try
            {
                if (!this.stateDirectory.LockGlobalState())
                {
                    throw new LockException($"Failed to lock the global state directory: {this.BaseDir}");
                }
            }
            catch (IOException e)
            {
                //                throw new LockException(string.Format("Failed to lock the global state directory: %s", baseDir), e);
            }

            try
            {

                //              checkpointFileCache.putAll(checkpointFile.read());
            }
            catch (IOException e)
            {
                try
                {
                    this.stateDirectory.UnlockGlobalState();
                }
                catch (IOException e1)
                {
                    this.logger.LogError("Failed to unlock the global state directory", e);
                }

                throw new StreamsException("Failed to read checkpoints for global state globalStores", e);
            }

            var stateStores = this.topology?.globalStateStores;
            foreach (IStateStore stateStore in stateStores ?? Enumerable.Empty<IStateStore>())
            {
                this.globalStoreNames.Add(stateStore.Name);
                stateStore.Init(this.globalProcessorContext, stateStore);
            }

            return this.globalStoreNames;
        }


        public void ReinitializeStateStoresForPartitions(
            List<TopicPartition> partitions,
            IInternalProcessorContext processorContext)
        {
            StateManagerUtil.ReinitializeStateStoresForPartitions(
                this.logger,
                this.eosEnabled,
                this.BaseDir,
                this.globalStores,
                this.topology?.StoreToChangelogTopic,
                partitions,
                processorContext,
                this.checkpointFile,
                this.checkpointFileCache);

            this.globalConsumer.Assign(partitions);
            this.globalConsumer.SeekToBeginning(partitions);
        }


        public IStateStore? GetGlobalStore(string Name)
            => this.globalStores.GetValueOrDefault(Name);


        public IStateStore? GetStore(string Name)
            => this.GetGlobalStore(Name);

        public void Register(
            IStateStore store,
            IStateRestoreCallback stateRestoreCallback)
        {
            store = store ?? throw new ArgumentNullException(nameof(store));

            if (this.globalStores.ContainsKey(store.Name))
            {
                throw new ArgumentException($"Global Store {store.Name} has already been registered");
            }

            if (!this.globalStoreNames.Contains(store.Name))
            {
                throw new ArgumentException($"Trying to register store {store.Name} that is not a known global store");
            }

            if (stateRestoreCallback == null)
            {
                throw new ArgumentException($"The stateRestoreCallback provided for store {store.Name} was null");
            }

            this.logger.LogInformation($"Restoring state for global store {store.Name}");
            List<TopicPartition> topicPartitions = this.TopicPartitionsForStore(store);
            Dictionary<TopicPartition, long> highWatermarks = null;

            var attempts = 0;
            while (highWatermarks == null)
            {
                try
                {
                    // highWatermarks = globalConsumer.endOffsets(topicPartitions);
                }
                catch (TimeoutException retryableException)
                {
                    if (++attempts > this.retries)
                    {
                        this.logger.LogError("Failed to get end offsets for topic partitions of global store {} after {} retry attempts. " +
                            "You can increase the number of retries via configuration parameter `retries`.",
                            store.Name,
                            this.retries,
                            retryableException);

                        throw new StreamsException(string.Format("Failed to get end offsets for topic partitions of global store %s after %d retry attempts. " +
                                "You can increase the number of retries via configuration parameter `retries`.", store.Name, this.retries),
                            retryableException);
                    }

                    this.logger.LogDebug("Failed to get end offsets for partitions {}, backing off for {} ms to retry (attempt {} of {})",
                        topicPartitions,
                        this.retryBackoffMs,
                        attempts,
                        this.retries,
                        retryableException);
                    //                  Utils.sleep(retryBackoffMs);
                }
            }

            try
            {
                this.RestoreState(
                    stateRestoreCallback,
                    topicPartitions,
                    highWatermarks,
                    store.Name,
                    StateManagerUtil.ConverterForStore(store));

                this.globalStores.Add(store.Name, store);
            }
            finally
            {
                this.globalConsumer.Unsubscribe();
            }

        }

        private List<TopicPartition> TopicPartitionsForStore(IStateStore store)
        {
            var sourceTopic = this.topology.StoreToChangelogTopic[store.Name];
            var partitionInfos = new List<PartitionMetadata>();
            var attempts = 0;
            while (true)
            {
                try
                {
                    var admin = this.clientSupplier.GetAdminClient(this.config);
                    var metadata = admin.GetMetadata(sourceTopic, TimeSpan.FromMilliseconds(5000));
                    partitionInfos = metadata.Topics[0].Partitions;

                    break;
                }
                catch (TimeoutException retryableException)
                {
                    if (++attempts > this.retries)
                    {
                        this.logger.LogError("Failed to get partitions for topic {} after {} retry attempts due to timeout. " +
                                "The broker may be transiently unavailable at the moment. " +
                                "You can increase the number of retries via configuration parameter `retries`.",
                            null, //sourceTopic,
                            this.retries,
                            retryableException);
                        //throw new StreamsException(string.Format("Failed to get partitions for topic %s after %d retry attempts due to timeout. " +
                        //    "The broker may be transiently unavailable at the moment. " +
                        //    "You can increase the number of retries via configuration parameter `retries`.", sourceTopic, retries),
                        //    retryableException);
                    }
                    this.logger.LogDebug("Failed to get partitions for topic {} due to timeout. The broker may be transiently unavailable at the moment. " +
                            "Backing off for {} ms to retry (attempt {} of {})",
                        null, //sourceTopic,
                        this.retryBackoffMs,
                        attempts,
                        this.retries,
                        retryableException);
                    //                    Utils.sleep(retryBackoffMs);
                }
            }

            if (partitionInfos == null || !partitionInfos.Any())
            {
                throw new StreamsException($"There are no partitions available for topic {sourceTopic} when initializing global store {store.Name}");
            }

            var topicPartitions = new List<TopicPartition>();
            foreach (var partition in partitionInfos)
            {
                topicPartitions.Add(new TopicPartition(sourceTopic, new Partition(partition.PartitionId)));
            }

            return topicPartitions;
        }

        private void RestoreState(
            IStateRestoreCallback stateRestoreCallback,
            List<TopicPartition> topicPartitions,
            Dictionary<TopicPartition, long> highWatermarks,
            string storeName,
            IRecordConverter recordConverter)
        {
            foreach (TopicPartition topicPartition in topicPartitions)
            {
                this.globalConsumer.Assign(topicPartition);
                var checkpoint = this.checkpointFileCache[topicPartition];

                this.globalConsumer.Seek(new TopicPartitionOffset(topicPartition, new Offset(checkpoint ?? 0)));

                long offset = this.globalConsumer.Position(topicPartition);
                var highWatermark = highWatermarks[topicPartition];
                IRecordBatchingStateRestoreCallback stateRestoreAdapter =
                    StateRestoreCallbackAdapter.Adapt(stateRestoreCallback);

                this.stateRestoreListener.OnRestoreStart(topicPartition, storeName, offset, highWatermark);
                var restoreCount = 0L;

                while (offset < highWatermark)
                {
                    try
                    {
                        ConsumerRecords<byte[], byte[]> records = this.globalConsumer.Poll(this.pollTime);
                        var restoreRecords = new List<ConsumeResult<byte[], byte[]>>();

                        //foreach (ConsumeResult<byte[], byte[]> record in records.records(topicPartition))
                        //{
                        //    if (record.Key != null)
                        //    {
                        //        restoreRecords.Add(recordConverter.convert(record));
                        //    }
                        //}

                        offset = this.globalConsumer.Position(topicPartition);
                        stateRestoreAdapter.RestoreBatch(restoreRecords);
                        this.stateRestoreListener.OnBatchRestored(topicPartition, storeName, offset, restoreRecords.Count);
                        restoreCount += restoreRecords.Count;
                    }
                    catch (Exception recoverableException) //InvalidOffsetException
                    {
                        this.logger.LogWarning("Restoring GlobalStore {} failed due to: {}. Deleting global store to recreate from scratch.",
                            storeName,
                            recoverableException.ToString());
                        //                        reinitializeStateStoresForPartitions(recoverableException.partitions(), globalProcessorContext);

                        this.stateRestoreListener.OnRestoreStart(topicPartition, storeName, offset, highWatermark);
                        restoreCount = 0L;
                    }
                }

                this.stateRestoreListener.OnRestoreEnd(topicPartition, storeName, restoreCount);
                this.checkpointFileCache.Add(topicPartition, offset);
            }
        }

        public void Flush()
        {
            this.logger.LogDebug("Flushing All global globalStores registered in the state manager");
            foreach (KeyValuePair<string, IStateStore?> entry in this.globalStores)
            {
                if (entry.Value != null)
                {
                    IStateStore store = entry.Value;
                    try
                    {

                        this.logger.LogTrace("Flushing global store={}", store.Name);
                        store.Flush();
                    }
                    catch (Exception e)
                    {
                        //throw new ProcessorStateException(
                        //    string.Format("Failed to Flush global state store %s", store.Name), e);
                    }
                }
                else
                {

                    throw new InvalidOperationException("Expected " + entry.Key + " to have been initialized");
                }
            }
        }

        public void Close(bool clean)
        {
            try
            {

                if (!this.globalStores.Any())
                {
                    return;
                }

                var closeFailed = new StringBuilder();
                foreach (KeyValuePair<string, IStateStore?> entry in this.globalStores)
                {
                    if (true)//entry.Value.isPresent())
                    {
                        //    log.LogDebug("Closing global storage engine {}", entry.Key);
                        //    try
                        //    {

                        //        entry.Value().Close();
                        //    }
                        //    catch (RuntimeException e)
                        //    {
                        //        log.LogError("Failed to Close global state store {}", entry.Key, e);
                        //        closeFailed.Append("Failed to Close global state store:")
                        //                   .Append(entry.Key)
                        //                   .Append(". Reason: ")
                        //                   .Append(e)
                        //                   .Append("\n");
                        //    }

                        //    globalStores.Add(entry.Key, null);
                    }
                    else
                    {

                        this.logger.LogInformation("Skipping to Close non-initialized store {}", entry.Key);
                    }
                }
                //if (closeFailed.Length > 0)
                //{
                //    throw new ProcessorStateException("Exceptions caught during Close of 1 or more global state globalStores\n" + closeFailed);
                //}
            }
            finally
            {

                //stateDirectory.UnlockGlobalState();
            }
        }

        public void Checkpoint(Dictionary<TopicPartition, long> offsets)
        {
            //checkpointFileCache.putAll(offsets);

            var filteredOffsets = new Dictionary<TopicPartition, long?>();

            // Skip non Persistent store
            foreach (var topicPartitionOffset in this.checkpointFileCache)
            {
                var topic = topicPartitionOffset.Key.Topic;
                if (!this.globalNonPersistentStoresTopics.Contains(topic))
                {
                    filteredOffsets.Add(topicPartitionOffset.Key, topicPartitionOffset.Value);
                }
            }

            try
            {

                this.checkpointFile.Write(filteredOffsets);
            }
            catch (IOException e)
            {
                this.logger.LogWarning("Failed to write offset checkpoint file to {} for global stores: {}", this.checkpointFile, e);
            }
        }

        public Dictionary<TopicPartition, long?> Checkpointed()
        {
            return this.checkpointFileCache;
        }
    }
}