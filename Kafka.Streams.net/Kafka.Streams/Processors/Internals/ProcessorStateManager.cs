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
        private const string STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog";

        private readonly ILogger logger;
        private readonly TaskId taskId;
        private readonly string logPrefix;
        private readonly bool isStandby;
        private readonly IChangelogReader changelogReader;
        private readonly Dictionary<TopicPartition, long> offsetLimits;
        private readonly Dictionary<TopicPartition, long> standbyRestoredOffsets;
        private readonly Dictionary<string, IStateRestoreCallback>? restoreCallbacks; // used for standby tasks, keyed by state topic Name
        private readonly Dictionary<string, IRecordConverter>? recordConverters; // used for standby tasks, keyed by state topic Name
        private readonly Dictionary<string, string> storeToChangelogTopic;

        // must be maintained in topological order
        private readonly Dictionary<string, IStateStore?> registeredStores = new Dictionary<string, IStateStore?>();
        private readonly Dictionary<string, IStateStore?> globalStores = new Dictionary<string, IStateStore?>();

        private readonly List<TopicPartition> changelogPartitions = new List<TopicPartition>();

        // TODO: this map does not work with customized grouper where multiple partitions
        // of the same topic can be assigned to the same task.
        private readonly Dictionary<string, TopicPartition> partitionForTopic;

        private readonly bool eosEnabled;
        public DirectoryInfo BaseDir { get; }
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
            if (stateDirectory is null)
            {
                throw new ArgumentNullException(nameof(stateDirectory));
            }

            this.eosEnabled = eosEnabled;

            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

            this.taskId = taskId;
            this.changelogReader = changelogReader;
            this.logPrefix = $"task [{taskId}] ";

            this.partitionForTopic = new Dictionary<string, TopicPartition>();

            foreach (var source in sources ?? Enumerable.Empty<TopicPartition>())
            {
                this.partitionForTopic.Add(source.Topic, source);
            }

            this.offsetLimits = new Dictionary<TopicPartition, long>();
            this.standbyRestoredOffsets = new Dictionary<TopicPartition, long>();
            this.isStandby = isStandby;
            this.restoreCallbacks = isStandby
                ? new Dictionary<string, IStateRestoreCallback>()
                : null;

            this.recordConverters = isStandby ? new Dictionary<string, IRecordConverter>() : null;
            this.storeToChangelogTopic = new Dictionary<string, string>(storeToChangelogTopic);

            this.BaseDir = stateDirectory.DirectoryForTask(taskId);
            this.checkpointFile = new OffsetCheckpoint(new FileInfo(Path.Combine(this.BaseDir.FullName, StateManagerUtil.CHECKPOINT_FILE_NAME)));
            this.initialLoadedCheckpoints = this.checkpointFile.Read();

            this.logger.LogTrace($"Checkpointable offsets read from checkpoint: {this.initialLoadedCheckpoints}");

            if (eosEnabled)
            {
                // with EOS enabled, there should never be a checkpoint file _during_ processing.
                // delete the checkpoint file after loading its stored offsets.
                this.checkpointFile.Delete();
                this.checkpointFile = null;
            }

            this.logger.LogDebug($"Created state store manager for task {taskId}");
        }

        public static string StoreChangelogTopic(
            string applicationId,
            string storeName)
        {
            return $"{applicationId}-{storeName}{STATE_CHANGELOG_TOPIC_SUFFIX}";
        }

        public void Register(
            IStateStore store,
            IStateRestoreCallback stateRestoreCallback)
        {
            if (store is null)
            {
                throw new ArgumentNullException(nameof(store));
            }

            var storeName = store.Name;
            this.logger.LogDebug("Registering state store {} to its state manager", storeName);

            if (StateManagerUtil.CHECKPOINT_FILE_NAME.Equals(storeName))
            {
                throw new ArgumentException($"{this.logPrefix}Illegal store Name: {storeName}");
            }

            if (this.registeredStores.ContainsKey(storeName) && this.registeredStores[storeName] != null)
            {
                throw new ArgumentException($"{this.logPrefix}Store {storeName} has already been registered.");
            }

            // check that the underlying change log topic exist or not
            var topic = this.storeToChangelogTopic[storeName];
            if (topic != null)
            {
                var storePartition = new TopicPartition(topic, this.GetPartition(topic));

                IRecordConverter recordConverter = StateManagerUtil.ConverterForStore(store);

                if (this.isStandby)
                {
                    this.logger.LogTrace("Preparing standby replica of Persistent state store {} with changelog topic {}", storeName, topic);

                    this.restoreCallbacks?.Add(topic, stateRestoreCallback);
                    this.recordConverters?.Add(topic, recordConverter);
                }
                else
                {
                    long? restoreCheckpoint = this.initialLoadedCheckpoints[storePartition];

                    if (restoreCheckpoint != null)
                    {
                        this.checkpointFileCache.Add(storePartition, restoreCheckpoint.Value);
                    }

                    this.logger.LogTrace("Restoring state store {} from changelog topic {} at checkpoint {}", storeName, topic, restoreCheckpoint);

                    var restorer = new StateRestorer(
                        storePartition,
                        new CompositeRestoreListener(stateRestoreCallback),
                        restoreCheckpoint.GetValueOrDefault(-1),
                        this.OffsetLimit(storePartition),
                        store.Persistent(),
                        storeName,
                        recordConverter
                    );

                    this.changelogReader.Register(restorer);
                }

                this.changelogPartitions.Add(storePartition);
            }

            this.registeredStores.Add(storeName, null);
        }

        public void ReinitializeStateStoresForPartitions(
            List<TopicPartition> partitions,
            IInternalProcessorContext processorContext)
        {
            StateManagerUtil.ReinitializeStateStoresForPartitions(
                this.logger,
                this.eosEnabled,
                this.BaseDir,
                this.registeredStores,
                this.storeToChangelogTopic,
                partitions,
                processorContext,
                this.checkpointFile,
                this.checkpointFileCache);
        }

        public void ClearCheckpoints()
        {
            if (this.checkpointFile != null)
            {
                this.checkpointFile.Delete();
                this.checkpointFile = null;

                this.checkpointFileCache.Clear();
            }
        }


        public Dictionary<TopicPartition, long?> Checkpointed()
        {
            this.UpdateCheckpointFileCache(new Dictionary<TopicPartition, long>());
            var partitionsAndOffsets = new Dictionary<TopicPartition, long?>();

            foreach (var entry in this.restoreCallbacks)
            {
                var topicName = entry.Key;
                var partition = this.GetPartition(topicName);
                var storePartition = new TopicPartition(topicName, partition);

                partitionsAndOffsets.Add(storePartition, this.checkpointFileCache.GetValueOrDefault(storePartition, -1L));
            }

            return partitionsAndOffsets;
        }

        public void UpdateStandbyStates(
            TopicPartition storePartition,
            List<ConsumeResult<byte[], byte[]>> restoreRecords,
            long lastOffset)
        {
            // restore states from changelog records
            IRecordBatchingStateRestoreCallback restoreCallback = StateRestoreCallbackAdapter.Adapt(this.restoreCallbacks[storePartition.Topic]);

            if (restoreRecords.Any())
            {
                IRecordConverter converter = this.recordConverters[storePartition.Topic];
                var convertedRecords = new List<ConsumeResult<byte[], byte[]>>(restoreRecords.Count);
                foreach (ConsumeResult<byte[], byte[]> record in restoreRecords)
                {
                    convertedRecords.Add(converter.Convert(record));
                }

                try
                {
                    restoreCallback.RestoreBatch(convertedRecords);
                }
                catch (RuntimeException e)
                {
                    throw new ProcessorStateException(string.Format("%sException caught while trying to restore state from %s", this.logPrefix, storePartition), e);
                }
            }

            // record the restored offset for its change log partition
            this.standbyRestoredOffsets.Add(storePartition, lastOffset + 1);
        }

        public void PutOffsetLimit(
            TopicPartition partition,
            long limit)
        {
            this.logger.LogTrace("Updating store offset limit for partition {} to {}", partition, limit);
            this.offsetLimits.Add(partition, limit);
        }

        public long OffsetLimit(TopicPartition partition)
        {
            long? limit = this.offsetLimits[partition];

            return limit != null
                ? limit.Value
                : long.MaxValue;
        }

        public IStateStore? GetStore(string Name)
        {
            return this.registeredStores.GetValueOrDefault(Name, null);
        }


        public void Flush()
        {
            ProcessorStateException firstException = null;
            // attempting to Flush the stores
            if (this.registeredStores.Any())
            {
                this.logger.LogDebug("Flushing All stores registered in the state manager");
                foreach (KeyValuePair<string, IStateStore?> entry in this.registeredStores)
                {
                    if (entry.Value.IsPresent())
                    {
                        IStateStore store = entry.Value;
                        this.logger.LogTrace("Flushing store {}", store.Name);
                        try
                        {

                            store.Flush();
                        }
                        catch (RuntimeException e)
                        {
                            if (firstException == null)
                            {
                                firstException = new ProcessorStateException(string.Format("%sFailed to Flush state store %s", this.logPrefix, store.Name), e);
                            }
                            this.logger.LogError("Failed to Flush state store {}: ", store.Name, e);
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
         * {@link IStateStore#Close() Close} All stores (even in case of failure).
         * Log All exception and re-throw the first exception that did occur at the end.
         *
         * @throws ProcessorStateException if any error happens when closing the state stores
         */

        public void Close(bool clean)
        {
            ProcessorStateException firstException = null;
            // attempting to Close the stores, just in case they
            // are not closed by a ProcessorNode yet
            if (this.registeredStores.Any())
            {
                this.logger.LogDebug("Closing its state manager and All the registered state stores");
                foreach (var entry in this.registeredStores)
                {
                    if (entry.Value.IsPresent())
                    {
                        IStateStore store = entry.Value;
                        this.logger.LogDebug("Closing storage engine {}", store.Name);
                        try
                        {

                            store.Close();
                            this.registeredStores.Add(store.Name, null);
                        }
                        catch (RuntimeException e)
                        {
                            if (firstException == null)
                            {
                                firstException = new ProcessorStateException(string.Format("%sFailed to Close state store %s", this.logPrefix, store.Name), e);
                            }

                            this.logger.LogError("Failed to Close state store {}: ", store.Name, e);
                        }
                    }
                    else
                    {

                        this.logger.LogInformation("Skipping to Close non-initialized store {}", entry.Key);
                    }
                }
            }

            if (!clean && this.eosEnabled)
            {
                // delete the checkpoint file if this is an unclean Close
                try
                {

                    this.ClearCheckpoints();
                }
                catch (IOException e)
                {
                    throw new ProcessorStateException(string.Format("%sError while deleting the checkpoint file", this.logPrefix), e);
                }
            }

            if (firstException != null)
            {
                throw firstException;
            }
        }


        public void Checkpoint(Dictionary<TopicPartition, long> checkpointableOffsetsFromProcessing)
        {
            this.EnsureStoresRegistered();

            // write the checkpoint file before closing
            if (this.checkpointFile == null)
            {
                this.checkpointFile = new OffsetCheckpoint(new FileInfo(Path.Combine(this.BaseDir.FullName, StateManagerUtil.CHECKPOINT_FILE_NAME)));
            }

            this.UpdateCheckpointFileCache(checkpointableOffsetsFromProcessing);

            this.logger.LogTrace("Checkpointable offsets updated with active acked offsets: {}", this.checkpointFileCache);

            this.logger.LogTrace("Writing checkpoint: {}", this.checkpointFileCache);

            try
            {
                this.checkpointFile.Write(this.checkpointFileCache);
            }
            catch (IOException e)
            {
                this.logger.LogWarning("Failed to write offset checkpoint file to [{}]", this.checkpointFile, e);
            }
        }

        private void UpdateCheckpointFileCache(Dictionary<TopicPartition, long> checkpointableOffsetsFromProcessing)
        {
            HashSet<TopicPartition> _validCheckpointableTopics = this.ValidCheckpointableTopics();
            var restoredOffsets = ValidCheckpointableOffsets(
                this.changelogReader.GetRestoredOffsets(),
                this.ValidCheckpointableTopics());

            this.logger.LogTrace("Checkpointable offsets updated with restored offsets: {}", this.checkpointFileCache);
            foreach (TopicPartition topicPartition in this.ValidCheckpointableTopics())
            {
                if (checkpointableOffsetsFromProcessing.ContainsKey(topicPartition))
                {
                    // if we have just recently processed some offsets,
                    // store the last offset + 1 (the log position after restoration)
                    this.checkpointFileCache.Add(topicPartition, checkpointableOffsetsFromProcessing[topicPartition] + 1);
                }
                else if (this.standbyRestoredOffsets.ContainsKey(topicPartition))
                {
                    // or if we restored some offset as a standby task, use it
                    this.checkpointFileCache.Add(topicPartition, this.standbyRestoredOffsets[topicPartition]);
                }
                else if (restoredOffsets.ContainsKey(topicPartition))
                {
                    // or if we restored some offset as an active task, use it
                    this.checkpointFileCache.Add(topicPartition, restoredOffsets[topicPartition]);
                }
                else if (this.checkpointFileCache.ContainsKey(topicPartition))
                {
                    // or if we have a prior value we've cached (and written to the checkpoint file), then keep it
                }
                else
                {
                    // As a last resort, fall back to the offset we loaded from the checkpoint file at startup, but
                    // only if the offset is actually valid for our current state stores.
                    var loadedOffset = ValidCheckpointableOffsets(this.initialLoadedCheckpoints, this.ValidCheckpointableTopics())[topicPartition];

                    if (loadedOffset != null)
                    {
                        this.checkpointFileCache.Add(topicPartition, loadedOffset);
                    }
                }
            }
        }

        private int GetPartition(string topic)
        {
            TopicPartition partition = this.partitionForTopic[topic];
            return partition == null ? this.taskId.partition : partition.Partition.Value;
        }

        public void RegisterGlobalStateStores(List<IStateStore> stateStores)
        {
            this.logger.LogDebug("Register global stores {}", stateStores);
            foreach (IStateStore stateStore in stateStores)
            {
                this.globalStores.Add(stateStore.Name, stateStore);
            }
        }


        public IStateStore? GetGlobalStore(string Name)
        {
            return this.globalStores.GetValueOrDefault(Name, null);
        }

        public void EnsureStoresRegistered()
        {
            foreach (var entry in this.registeredStores)
            {
                if (!entry.Value.IsPresent())
                {
                    throw new InvalidOperationException(
                        "store [" + entry.Key + "] has not been correctly registered. This is a bug in Kafka Streams."
                    );
                }
            }
        }

        private HashSet<TopicPartition> ValidCheckpointableTopics()
        {
            // it's only valid to record checkpoints for registered stores that are both Persistent and change-logged

            var result = new HashSet<TopicPartition>();
            foreach (KeyValuePair<string, string> storeToChangelog in this.storeToChangelogTopic)
            {
                var storeName = storeToChangelog.Key;
                if (this.registeredStores.ContainsKey(storeName)
                    && this.registeredStores[storeName].IsPresent()
                    && this.registeredStores[storeName].Persistent())
                {

                    var changelogTopic = storeToChangelog.Value;
                    result.Add(new TopicPartition(changelogTopic, this.GetPartition(changelogTopic)));
                }
            }
            return result;
        }

        private static Dictionary<TopicPartition, long?> ValidCheckpointableOffsets(
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
