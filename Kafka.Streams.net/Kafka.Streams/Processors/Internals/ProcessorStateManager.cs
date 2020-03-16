using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Tasks;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Kafka.Streams.Processors.Internals
{
    public class ProcessorStateManager : IStateManager
    {
        private static readonly string STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog";

        private readonly ILogger logger;
        private readonly TaskId taskId;
        private readonly string logPrefix;
        private readonly bool isStandby;
        private readonly IChangelogReader changelogReader;
        private readonly Dictionary<TopicPartition, long> offsetLimits;
        private readonly Dictionary<TopicPartition, long> standbyRestoredOffsets;
        private readonly Dictionary<string, IStateRestoreCallback>? restoreCallbacks; // used for standby tasks, keyed by state topic name
        private readonly Dictionary<string, IRecordConverter> recordConverters; // used for standby tasks, keyed by state topic name
        private readonly Dictionary<string, string> storeToChangelogTopic;

        // must be maintained in topological order
        private readonly Dictionary<string, IStateStore?> registeredStores = new Dictionary<string, IStateStore?>();
        private readonly Dictionary<string, IStateStore?> globalStores = new Dictionary<string, IStateStore?>();

        private readonly List<TopicPartition> changelogPartitions = new List<TopicPartition>();

        // TODO: this map does not work with customized grouper where multiple partitions
        // of the same topic can be assigned to the same task.
        private readonly Dictionary<string, TopicPartition> partitionForTopic;

        private readonly bool eosEnabled;
        public DirectoryInfo baseDir { get; }
        private OffsetCheckpoint? checkpointFile;
        private readonly Dictionary<TopicPartition, long?> checkpointFileCache = new Dictionary<TopicPartition, long?>();
        private readonly Dictionary<TopicPartition, long> initialLoadedCheckpoints;

        /**
         * @throws ProcessorStateException if the task directory does not exist and could not be created
         * @throws IOException             if any severe error happens while creating or locking the state directory
         */
        public ProcessorStateManager(
            ILogger<ProcessorStateManager> logger,
            TaskId taskId,
            List<TopicPartition> sources,
            bool isStandby,
            StateDirectory stateDirectory,
            Dictionary<string, string> storeToChangelogTopic,
            IChangelogReader changelogReader,
            bool eosEnabled)
        {
            this.eosEnabled = eosEnabled;

            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

            this.taskId = taskId;
            this.changelogReader = changelogReader;
            logPrefix = string.Format("task [%s] ", taskId);

            partitionForTopic = new Dictionary<string, TopicPartition>();

            foreach (TopicPartition source in sources)
            {
                partitionForTopic.Add(source.Topic, source);
            }

            offsetLimits = new Dictionary<TopicPartition, long>();
            standbyRestoredOffsets = new Dictionary<TopicPartition, long>();
            this.isStandby = isStandby;
            restoreCallbacks = isStandby
                ? new Dictionary<string, IStateRestoreCallback>()
                : null;

            recordConverters = isStandby ? new Dictionary<string, IRecordConverter>() : null;
            this.storeToChangelogTopic = new Dictionary<string, string>(storeToChangelogTopic);

            baseDir = stateDirectory.directoryForTask(taskId);
            checkpointFile = new OffsetCheckpoint(new FileInfo(Path.Combine(baseDir.FullName, StateManagerUtil.CHECKPOINT_FILE_NAME)));
            initialLoadedCheckpoints = checkpointFile.Read();

            this.logger.LogTrace($"Checkpointable offsets read from checkpoint: {this.initialLoadedCheckpoints}");

            if (eosEnabled)
            {
                // with EOS enabled, there should never be a checkpoint file _during_ processing.
                // delete the checkpoint file after loading its stored offsets.
                checkpointFile.Delete();
                checkpointFile = null;
            }

            this.logger.LogDebug("Created state store manager for task {}", taskId);
        }


        public static string storeChangelogTopic(
            string applicationId,
            string storeName)
        {
            return applicationId + "-" + storeName + STATE_CHANGELOG_TOPIC_SUFFIX;
        }

        public void Register(
            IStateStore store,
            IStateRestoreCallback stateRestoreCallback)
        {
            var storeName = store.name;
            logger.LogDebug("Registering state store {} to its state manager", storeName);

            if (StateManagerUtil.CHECKPOINT_FILE_NAME.Equals(storeName))
            {
                throw new ArgumentException(string.Format("%sIllegal store name: %s", logPrefix, storeName));
            }

            if (registeredStores.ContainsKey(storeName) && registeredStores[storeName] != null)
            {
                throw new ArgumentException(string.Format("%sStore %s has already been registered.", logPrefix, storeName));
            }

            // check that the underlying change log topic exist or not
            var topic = storeToChangelogTopic[storeName];
            if (topic != null)
            {
                var storePartition = new TopicPartition(topic, getPartition(topic));

                IRecordConverter recordConverter = StateManagerUtil.converterForStore(store);

                if (isStandby)
                {
                    logger.LogTrace("Preparing standby replica of persistent state store {} with changelog topic {}", storeName, topic);

                    restoreCallbacks.Add(topic, stateRestoreCallback);
                    recordConverters.Add(topic, recordConverter);
                }
                else
                {
                    long? restoreCheckpoint = initialLoadedCheckpoints[storePartition];

                    if (restoreCheckpoint != null)
                    {
                        checkpointFileCache.Add(storePartition, restoreCheckpoint.Value);
                    }

                    logger.LogTrace("Restoring state store {} from changelog topic {} at checkpoint {}", storeName, topic, restoreCheckpoint);

                    var restorer = new StateRestorer(
                        storePartition,
                        new CompositeRestoreListener(stateRestoreCallback),
                        restoreCheckpoint.GetValueOrDefault(-1),
                        offsetLimit(storePartition),
                        store.persistent(),
                        storeName,
                        recordConverter
                    );

                    changelogReader.register(restorer);
                }

                changelogPartitions.Add(storePartition);
            }

            registeredStores.Add(storeName, null);
        }

        public void ReinitializeStateStoresForPartitions(
            List<TopicPartition> partitions,
            IInternalProcessorContext processorContext)
        {
            StateManagerUtil.reinitializeStateStoresForPartitions(
                logger,
                eosEnabled,
                baseDir,
                registeredStores,
                storeToChangelogTopic,
                partitions,
                processorContext,
                checkpointFile,
                checkpointFileCache);
        }

        public void clearCheckpoints()
        {
            if (checkpointFile != null)
            {
                checkpointFile.Delete();
                checkpointFile = null;

                checkpointFileCache.Clear();
            }
        }


        public Dictionary<TopicPartition, long?> checkpointed()
        {
            updateCheckpointFileCache(new Dictionary<TopicPartition, long>());
            var partitionsAndOffsets = new Dictionary<TopicPartition, long?>();

            foreach (var entry in restoreCallbacks)
            {
                var topicName = entry.Key;
                var partition = getPartition(topicName);
                var storePartition = new TopicPartition(topicName, partition);

                partitionsAndOffsets.Add(storePartition, checkpointFileCache.GetValueOrDefault(storePartition, -1L));
            }

            return partitionsAndOffsets;
        }

        public void updateStandbyStates(
            TopicPartition storePartition,
            List<ConsumeResult<byte[], byte[]>> restoreRecords,
            long lastOffset)
        {
            // restore states from changelog records
            IRecordBatchingStateRestoreCallback restoreCallback = StateRestoreCallbackAdapter.adapt(restoreCallbacks[storePartition.Topic]);

            if (restoreRecords.Any())
            {
                IRecordConverter converter = recordConverters[storePartition.Topic];
                var convertedRecords = new List<ConsumeResult<byte[], byte[]>>(restoreRecords.Count);
                foreach (ConsumeResult<byte[], byte[]> record in restoreRecords)
                {
                    convertedRecords.Add(converter.convert(record));
                }

                try
                {
                    restoreCallback.restoreBatch(convertedRecords);
                }
                catch (RuntimeException e)
                {
                    throw new ProcessorStateException(string.Format("%sException caught while trying to restore state from %s", logPrefix, storePartition), e);
                }
            }

            // record the restored offset for its change log partition
            standbyRestoredOffsets.Add(storePartition, lastOffset + 1);
        }

        public void putOffsetLimit(
            TopicPartition partition,
            long limit)
        {
            logger.LogTrace("Updating store offset limit for partition {} to {}", partition, limit);
            offsetLimits.Add(partition, limit);
        }

        public long offsetLimit(TopicPartition partition)
        {
            long? limit = offsetLimits[partition];

            return limit != null
                ? limit.Value
                : long.MaxValue;
        }

        public IStateStore? GetStore(string name)
        {
            return registeredStores.GetValueOrDefault(name, null);
        }


        public void Flush()
        {
            ProcessorStateException firstException = null;
            // attempting to flush the stores
            if (registeredStores.Any())
            {
                logger.LogDebug("Flushing all stores registered in the state manager");
                foreach (KeyValuePair<string, IStateStore?> entry in registeredStores)
                {
                    if (entry.Value.isPresent())
                    {
                        IStateStore store = entry.Value;
                        logger.LogTrace("Flushing store {}", store.name);
                        try
                        {

                            store.flush();
                        }
                        catch (RuntimeException e)
                        {
                            if (firstException == null)
                            {
                                firstException = new ProcessorStateException(string.Format("%sFailed to flush state store %s", logPrefix, store.name), e);
                            }
                            logger.LogError("Failed to flush state store {}: ", store.name, e);
                        }
                    }
                    else
                    {

                        throw new InvalidOperationException("Expected " + entry.Key + " to have been initialized");
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

        public void Close(bool clean)
        {
            ProcessorStateException firstException = null;
            // attempting to close the stores, just in case they
            // are not closed by a ProcessorNode yet
            if (registeredStores.Any())
            {
                logger.LogDebug("Closing its state manager and all the registered state stores");
                foreach (var entry in registeredStores)
                {
                    if (entry.Value.isPresent())
                    {
                        IStateStore store = entry.Value;
                        logger.LogDebug("Closing storage engine {}", store.name);
                        try
                        {

                            store.close();
                            registeredStores.Add(store.name, null);
                        }
                        catch (RuntimeException e)
                        {
                            if (firstException == null)
                            {
                                firstException = new ProcessorStateException(string.Format("%sFailed to close state store %s", logPrefix, store.name), e);
                            }

                            logger.LogError("Failed to close state store {}: ", store.name, e);
                        }
                    }
                    else
                    {

                        logger.LogInformation("Skipping to close non-initialized store {}", entry.Key);
                    }
                }
            }

            if (!clean && eosEnabled)
            {
                // delete the checkpoint file if this is an unclean close
                try
                {

                    clearCheckpoints();
                }
                catch (IOException e)
                {
                    throw new ProcessorStateException(string.Format("%sError while deleting the checkpoint file", logPrefix), e);
                }
            }

            if (firstException != null)
            {
                throw firstException;
            }
        }


        public void checkpoint(Dictionary<TopicPartition, long> checkpointableOffsetsFromProcessing)
        {
            ensureStoresRegistered();

            // write the checkpoint file before closing
            if (checkpointFile == null)
            {
                checkpointFile = new OffsetCheckpoint(new FileInfo(Path.Combine(baseDir.FullName, StateManagerUtil.CHECKPOINT_FILE_NAME)));
            }

            updateCheckpointFileCache(checkpointableOffsetsFromProcessing);

            logger.LogTrace("Checkpointable offsets updated with active acked offsets: {}", checkpointFileCache);

            logger.LogTrace("Writing checkpoint: {}", checkpointFileCache);

            try
            {
                checkpointFile.Write(checkpointFileCache);
            }
            catch (IOException e)
            {
                logger.LogWarning("Failed to write offset checkpoint file to [{}]", checkpointFile, e);
            }
        }

        private void updateCheckpointFileCache(Dictionary<TopicPartition, long> checkpointableOffsetsFromProcessing)
        {
            HashSet<TopicPartition> _validCheckpointableTopics = validCheckpointableTopics();
            var restoredOffsets = validCheckpointableOffsets(
                changelogReader.restoredOffsets(),
                validCheckpointableTopics());

            logger.LogTrace("Checkpointable offsets updated with restored offsets: {}", checkpointFileCache);
            foreach (TopicPartition topicPartition in validCheckpointableTopics())
            {
                if (checkpointableOffsetsFromProcessing.ContainsKey(topicPartition))
                {
                    // if we have just recently processed some offsets,
                    // store the last offset + 1 (the log position after restoration)
                    checkpointFileCache.Add(topicPartition, checkpointableOffsetsFromProcessing[topicPartition] + 1);
                }
                else if (standbyRestoredOffsets.ContainsKey(topicPartition))
                {
                    // or if we restored some offset as a standby task, use it
                    checkpointFileCache.Add(topicPartition, standbyRestoredOffsets[topicPartition]);
                }
                else if (restoredOffsets.ContainsKey(topicPartition))
                {
                    // or if we restored some offset as an active task, use it
                    checkpointFileCache.Add(topicPartition, restoredOffsets[topicPartition]);
                }
                else if (checkpointFileCache.ContainsKey(topicPartition))
                {
                    // or if we have a prior value we've cached (and written to the checkpoint file), then keep it
                }
                else
                {
                    // As a last resort, fall back to the offset we loaded from the checkpoint file at startup, but
                    // only if the offset is actually valid for our current state stores.
                    var loadedOffset = validCheckpointableOffsets(initialLoadedCheckpoints, validCheckpointableTopics())[topicPartition];

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
            return partition == null ? taskId.partition : partition.Partition.Value;
        }

        public void registerGlobalStateStores(List<IStateStore> stateStores)
        {
            logger.LogDebug("Register global stores {}", stateStores);
            foreach (IStateStore stateStore in stateStores)
            {
                globalStores.Add(stateStore.name, stateStore);
            }
        }


        public IStateStore? GetGlobalStore(string name)
        {
            return globalStores.GetValueOrDefault(name, null);
        }

        public void ensureStoresRegistered()
        {
            foreach (var entry in registeredStores)
            {
                if (!entry.Value.isPresent())
                {
                    throw new InvalidOperationException(
                        "store [" + entry.Key + "] has not been correctly registered. This is a bug in Kafka Streams."
                    );
                }
            }
        }

        private HashSet<TopicPartition> validCheckpointableTopics()
        {
            // it's only valid to record checkpoints for registered stores that are both persistent and change-logged

            var result = new HashSet<TopicPartition>();
            foreach (KeyValuePair<string, string> storeToChangelog in storeToChangelogTopic)
            {
                var storeName = storeToChangelog.Key;
                if (registeredStores.ContainsKey(storeName)
                    && registeredStores[storeName].isPresent()
                    && registeredStores[storeName].persistent())
                {

                    var changelogTopic = storeToChangelog.Value;
                    result.Add(new TopicPartition(changelogTopic, getPartition(changelogTopic)));
                }
            }
            return result;
        }

        private static Dictionary<TopicPartition, long?> validCheckpointableOffsets(
            Dictionary<TopicPartition, long> checkpointableOffsets,
            HashSet<TopicPartition> validCheckpointableTopics)
        {
            var result = new Dictionary<TopicPartition, long?>(checkpointableOffsets.Count);

            foreach (var topicToCheckpointableOffset in checkpointableOffsets)
            {
                TopicPartition topic = topicToCheckpointableOffset.Key;
                if (validCheckpointableTopics.Contains(topic))
                {
                    var checkpointableOffset = topicToCheckpointableOffset.Value;
                    result.Add(topic, checkpointableOffset);
                }
            }

            return result;
        }
    }
}