using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Tasks;
using Kafka.Streams.Temporary;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Kafka.Streams.Processors.Internals
{
    /**
* ProcessorStateManager is the source of truth for the current offset for each state store,
* which is either the read offset during restoring, or the written offset during normal processing.
*
* The offset is initialized as null when the state store is registered, and then it can be updated by
* loading checkpoint file, restore state stores, or passing from the record collector's written offsets.
*
* When checkpointing, if the offset is not null it would be written to the file.
*
* The manager is also responsible for restoring state stores via their registered restore callback,
* which is used for both updating standby tasks as well as restoring active tasks.
*/
    public class ProcessorStateManager : IStateManager
    {
        public class StateStoreMetadata
        {
            public IStateStore StateStore { get; }

            // corresponding changelog partition of the store, this and the following two fields
            // will only be not-null if the state store is logged (i.e. changelog partition and restorer provided)
            public TopicPartition ChangelogPartition { get; }

            // could be used for both active restoration and standby
            public IStateRestoreCallback RestoreCallback { get; }

            // record converters used for restoration and standby
            public IRecordConverter RecordConverter { get; }

            // indicating the current snapshot of the store as the offset of last changelog record that has been
            // applied to the store used for both restoration (active and standby tasks restored offset) and
            // normal processing that update stores (written offset); could be null (when initialized)
            //
            // the offset is updated in three ways:
            //   1. when loading from the checkpoint file, when the corresponding task has acquired the state
            //      directory lock and have registered all the state store; it is only one-time
            //   2. when updating with restore records (by both restoring active and standby),
            //      update to the last restore record's offset
            //   3. when checkpointing with the given written offsets from record collector,
            //      update blindly with the given offset
            public long? Offset { get; private set; }

            // corrupted state store should not be included in checkpointing
            public bool Corrupted { get; set; }

            public StateStoreMetadata(IStateStore stateStore)
            {
                this.StateStore = stateStore;
                this.RestoreCallback = null;
                this.RecordConverter = null;
                this.ChangelogPartition = null;
                this.Corrupted = false;
                this.Offset = null;
            }

            public StateStoreMetadata(
                IStateStore stateStore,
                TopicPartition changelogPartition,
                IStateRestoreCallback restoreCallback,
                IRecordConverter recordConverter)
            {
                this.StateStore = stateStore;
                this.ChangelogPartition = changelogPartition;
                this.RestoreCallback = restoreCallback ?? throw new InvalidOperationException("Log enabled store should always provide a restore callback upon registration");
                this.RecordConverter = recordConverter;
                this.Offset = null;
            }

            public void SetOffset(long offset)
            {
                this.Offset = offset;
            }

            public IStateStore Store()
            {
                return this.StateStore;
            }

            public override string ToString()
            {
                return "StateStoreMetadata (" + this.StateStore.Name + " : " + this.ChangelogPartition + " @ " + this.Offset;
            }
        }

        private static readonly string STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog";
        private readonly KafkaStreamsContext context;
        private readonly ILogger log;
        public TaskId TaskId { get; }
        public DirectoryInfo BaseDir { get; }

        private readonly string logPrefix;
        private readonly TaskType taskType;
        private readonly bool eosEnabled;
        private readonly IChangelogRegister changelogReader;
        private readonly Dictionary<string, string> storeToChangelogTopic;
        private readonly IEnumerable<TopicPartition> sourcePartitions;

        // must be maintained in topological order
        private readonly Dictionary<string, StateStoreMetadata> stores = new Dictionary<string, StateStoreMetadata>();
        private readonly Dictionary<string, IStateStore> globalStores = new Dictionary<string, IStateStore>();

        private readonly DirectoryInfo baseDir;
        private readonly OffsetCheckpoint checkpointFile;

        public static string StoreChangelogTopic(string applicationId, string storeName)
        {
            return applicationId + "-" + storeName + STATE_CHANGELOG_TOPIC_SUFFIX;
        }

        /**
         * @throws ProcessorStateException if the task directory does not exist and could not be created
         */
        public ProcessorStateManager(
            KafkaStreamsContext context,
            TaskId taskId,
            TaskType taskType,
            bool eosEnabled,
            StateDirectory stateDirectory,
            IChangelogRegister changelogReader,
            Dictionary<string, string> storeToChangelogTopic,
            IEnumerable<TopicPartition> sourcePartitions)
        {
            this.context = context;
            this.log = this.context.CreateLogger<ProcessorStateManager>();
            this.TaskId = taskId;
            this.taskType = taskType;
            this.eosEnabled = eosEnabled;
            this.changelogReader = changelogReader;
            this.sourcePartitions = sourcePartitions;
            this.storeToChangelogTopic = storeToChangelogTopic;

            this.baseDir = stateDirectory.DirectoryForTask(taskId);
            this.checkpointFile = new OffsetCheckpoint(stateDirectory.CheckpointFileFor(taskId));

            this.log.LogDebug("Created state store manager for task {}", taskId);
        }

        public void RegisterGlobalStateStores(List<IStateStore> stateStores)
        {
            this.log.LogDebug("Register global stores {}", stateStores);
            foreach (IStateStore stateStore in stateStores)
            {
                this.globalStores.Add(stateStore.Name, stateStore);
            }
        }

        public IStateStore GetGlobalStore(string name)
        {
            return this.globalStores[name];
        }

        // package-private for test only
        public void InitializeStoreOffsetsFromCheckpoint(bool storeDirIsEmpty)
        {
            try
            {
                Dictionary<TopicPartition, long> loadedCheckpoints = this.checkpointFile.Read();

                this.log.LogTrace("Loaded offsets from the checkpoint file: {}", loadedCheckpoints);

                foreach (StateStoreMetadata store in this.stores.Values)
                {
                    if (store.ChangelogPartition == null)
                    {
                        this.log.LogInformation("State store {} is not logged and hence would not be restored", store.StateStore.Name);
                    }
                    else
                    {
                        if (loadedCheckpoints.TryGetValue(store.ChangelogPartition, out var partition))
                        {
                            loadedCheckpoints.Remove(store.ChangelogPartition);
                            store.SetOffset(partition);

                            this.log.LogDebug("State store {} initialized from checkpoint with offset {} at changelog {}",
                                store.StateStore.Name, store.Offset, store.ChangelogPartition);
                        }
                        else
                        {
                            // with EOS, if the previous run did not shutdown gracefully, we may lost the checkpoint file
                            // and hence we are uncertain the the current local state only contains committed data;
                            // in that case we need to treat it as a task-corrupted exception
                            if (this.eosEnabled && !storeDirIsEmpty)
                            {
                                this.log.LogWarning("State store {} did not find checkpoint offsets while stores are not empty, " +
                                    "since under EOS it has the risk of getting uncommitted data in stores we have to " +
                                    "treat it as a task corruption error and wipe out the local state of task {} " +
                                    "before re-bootstrapping", store.StateStore.Name, this.TaskId);

                                throw new TaskCorruptedException();// Collections.singletonMap(this.taskId, this.ChangelogPartitions()));
                            }
                            else
                            {
                                this.log.LogInformation("State store {} did not find checkpoint offset, hence would " +
                                    "default to the starting offset at changelog {}",
                                    store.StateStore.Name, store.ChangelogPartition);
                            }
                        }
                    }
                }

                if (!loadedCheckpoints.IsEmpty())
                {
                    this.log.LogWarning("Some loaded checkpoint offsets cannot find their corresponding state stores: {}", loadedCheckpoints);
                }

                this.checkpointFile.Delete();
            }
            catch (TaskCorruptedException e)
            {
                throw e;
            }
            catch (IOException e)
            {
                // both IOException or runtime exception like number parsing can throw
                throw new ProcessorStateException($"{this.logPrefix}Error loading and deleting checkpoint file when creating the state manager", e);
            }
        }

        public void RegisterStore(IStateStore store, IStateRestoreCallback stateRestoreCallback)
        {
            string storeName = store.Name;

            if (StateManagerUtil.CHECKPOINT_FILE_NAME.Equals(storeName))
            {
                throw new ArgumentException($"{this.logPrefix}Illegal store name: {storeName}, which collides with the pre-defined " +
                    "checkpoint file name");
            }

            if (this.stores.ContainsKey(storeName))
            {
                throw new ArgumentException(string.Format("%sStore %s has already been registered.", this.logPrefix, storeName));
            }

            string topic = this.storeToChangelogTopic[storeName];

            // if the store name does not exist in the changelog map, it means the underlying store
            // is not log enabled (including global stores), and hence it does not need to be restored
            if (topic != null)
            {
                // NOTE we assume the partition of the topic can always be inferred from the task id;
                // if user ever use a custom partition grouper (deprecated in KIP-528) this would break and
                // it is not a regression (it would always break anyways)
                TopicPartition storePartition = new TopicPartition(topic, this.TaskId.Partition);
                StateStoreMetadata storeMetadata = new StateStoreMetadata(
                    store,
                    storePartition,
                    stateRestoreCallback,
                    StateManagerUtil.ConverterForStore(store));

                this.stores.Add(storeName, storeMetadata);

                this.changelogReader.Register(storePartition, this);
            }
            else
            {
                this.stores.Add(storeName, new StateStoreMetadata(store));
            }

            this.log.LogDebug("Registered state store {} to its state manager", storeName);
        }

        public IStateStore? GetStore(string name)
        {
            if (this.stores.ContainsKey(name))
            {
                return this.stores[name].StateStore;
            }
            else
            {
                return null;
            }
        }

        public IEnumerable<TopicPartition> ChangelogPartitions()
        {
            return this.ChangelogOffsets().Keys;
        }

        private void MarkChangelogAsCorrupted(List<TopicPartition> partitions)
        {
            foreach (StateStoreMetadata storeMetadata in this.stores.Values)
            {
                if (partitions.Contains(storeMetadata.ChangelogPartition))
                {
                    storeMetadata.Corrupted = true;
                    partitions.Remove(storeMetadata.ChangelogPartition);
                }
            }

            if (!partitions.IsEmpty())
            {
                throw new InvalidOperationException("Some partitions " + partitions + " are not contained in the store list of task " +
                    this.TaskId + " marking as corrupted, this is not expected");
            }
        }

        public Dictionary<TopicPartition, long> ChangelogOffsets()
        {
            // return the current offsets for those logged stores
            Dictionary<TopicPartition, long> changelogOffsets = new Dictionary<TopicPartition, long>();
            foreach (StateStoreMetadata storeMetadata in this.stores.Values)
            {
                if (storeMetadata.ChangelogPartition != null)
                {
                    // for changelog whose offset is unknown, use 0L indicating earliest offset
                    // otherwise return the current offset + 1 as the next offset to fetch
                    changelogOffsets.Add(
                        storeMetadata.ChangelogPartition,
                        storeMetadata.Offset == null
                        ? 0L
                        : storeMetadata.Offset.Value + 1L);
                }
            }

            return changelogOffsets;
        }

        // used by the changelog reader only
        private bool ChangelogAsSource(TopicPartition partition)
        {
            return this.sourcePartitions.Contains(partition);
        }

        // used by the changelog reader only
        private StateStoreMetadata StoreMetadata(TopicPartition partition)
        {
            foreach (var storeMetadata in this.stores.Values)
            {
                if (partition.Equals(storeMetadata.ChangelogPartition))
                {
                    return storeMetadata;
                }
            }
            return null;
        }

        // used by the changelog reader only
        private void Restore(StateStoreMetadata storeMetadata, List<ConsumeResult<byte[], byte[]>> restoreRecords)
        {
            if (!this.stores.Values.Contains(storeMetadata))
            {
                throw new InvalidOperationException("Restoring " + storeMetadata + " which is not registered in this state manager, " +
                    "this should not happen.");
            }

            if (!restoreRecords.IsEmpty())
            {
                // restore states from changelog records and update the snapshot offset as the batch end record's offset
                long batchEndOffset = restoreRecords[restoreRecords.Count - 1].Offset;
                IRecordBatchingStateRestoreCallback restoreCallback = (IRecordBatchingStateRestoreCallback)storeMetadata.RestoreCallback;
                List<ConsumeResult<byte[], byte[]>> convertedRecords = restoreRecords;

                //.Map(storeMetadata.recordConverter::convert)
                //.Collect(Collectors.toList());

                try
                {
                    restoreCallback.RestoreBatch(convertedRecords);
                }
                catch (RuntimeException e)
                {
                    throw new ProcessorStateException(
                        string.Format("%sException caught while trying to restore state from %s",
                        this.logPrefix,
                        storeMetadata.ChangelogPartition),
                        e);
                }

                storeMetadata.SetOffset(batchEndOffset);
            }
        }

        /**
         * @throws TaskMigratedException recoverable error sending changelog records that would cause the task to be removed
         * @throws StreamsException fatal error when flushing the state store, for example sending changelog records failed
         *                          or flushing state store get IO errors; such error should cause the thread to die
         */
        public void Flush()
        {
            Exception? firstException = null;

            // attempting to flush the stores
            if (!this.stores.IsEmpty())
            {
                this.log.LogDebug("Flushing all stores registered in the state manager: {}", this.stores);
                foreach (var entry in this.stores)
                {
                    IStateStore store = entry.Value.StateStore;
                    this.log.LogTrace("Flushing store {}", store.Name);

                    try
                    {
                        store.Flush();
                    }
                    catch (RuntimeException exception)
                    {
                        if (firstException == null)
                        {
                            // do NOT wrap the error if it is actually caused by Streams itself
                            if (exception is StreamsException)
                            {
                                firstException = exception;
                            }
                        }
                        else
                        {
                            firstException = new ProcessorStateException(
                                string.Format("%sFailed to flush state store %s", this.logPrefix, store.Name), exception);
                        }
                    }

                    this.log.LogError(firstException, $"Failed to flush state store {store.Name}: ");
                }
            }

            if (firstException != null)
            {
                throw firstException;
            }
        }

        /**
         * {@link StateStore#close() Close} all stores (even in case of failure).
         * Log all exception and re-throw the first exception that did occur at the end.
         *
         * @throws ProcessorStateException if any error happens when closing the state stores
         */
        public void Close()
        {
            Exception? firstException = null;

            // attempting to close the stores, just in case they
            // are not closed by a ProcessorNode yet
            if (!this.stores.IsEmpty())
            {
                this.log.LogDebug("Closing its state manager and all the registered state stores: {}", this.stores);
                foreach (var entry in this.stores)
                {
                    IStateStore store = entry.Value.StateStore;
                    this.log.LogTrace("Closing store {}", store.Name);
                    try
                    {
                        store.Close();
                    }
                    catch (RuntimeException exception)
                    {
                        if (firstException == null)
                        {
                            // do NOT wrap the error if it is actually caused by Streams itself
                            if (exception is StreamsException)
                            {
                                firstException = exception;
                            }
                            else
                            {
                                firstException = new ProcessorStateException(
                                    string.Format("%sFailed to close state store %s", this.logPrefix, store.Name), exception);
                            }
                        }
                        this.log.LogError("Failed to close state store {}: ", store.Name, exception);
                    }
                }

                this.stores.Clear();
            }

            if (firstException != null)
            {
                throw firstException;
            }
        }

        public void Checkpoint(Dictionary<TopicPartition, long> writtenOffsets)
        {
            // first update each state store's current offset, then checkpoint
            // those stores that are only logged and persistent to the checkpoint file
            foreach (var entry in writtenOffsets)
            {
                StateStoreMetadata? store = this.FindStore(entry.Key);

                if (store != null)
                {
                    store.SetOffset(entry.Value);

                    this.log.LogDebug("State store {} updated to written offset {} at changelog {}",
                        store.StateStore.Name, store.Offset, store.ChangelogPartition);
                }
            }

            Dictionary<TopicPartition, long?> checkpointingOffsets = new Dictionary<TopicPartition, long?>();
            foreach (StateStoreMetadata storeMetadata in this.stores.Values)
            {
                // store is logged, persistent, not corrupted, and has a valid current offset
                if (storeMetadata.ChangelogPartition != null &&
                    storeMetadata.StateStore.Persistent() &&
                    storeMetadata.Offset != null &&
                    !storeMetadata.Corrupted)
                {

                    checkpointingOffsets.Add(storeMetadata.ChangelogPartition, storeMetadata.Offset);
                }
            }

            this.log.LogDebug("Writing checkpoint: {}", checkpointingOffsets);

            try
            {
                this.checkpointFile.Write(checkpointingOffsets);
            }
            catch (IOException e)
            {
                this.log.LogWarning("Failed to write offset checkpoint file to [{}]", this.checkpointFile, e);
            }
        }

        private StateStoreMetadata? FindStore(TopicPartition changelogPartition)
        {
            var found = this.stores
                .Where(metadata => changelogPartition.Equals(metadata.Value.ChangelogPartition))
                .ToList();

            if (found.Count > 1)
            {
                throw new InvalidOperationException("Multiple state stores are found for changelog partition " + changelogPartition +
                    ", this should never happen: " + found);
            }

            return found.IsEmpty()
                ? null
                : found[0].Value;
        }

        public void Register(IStateStore store, IStateRestoreCallback stateRestoreCallback)
        {
            throw new NotImplementedException();
        }

        public void ReinitializeStateStoresForPartitions(List<TopicPartition> partitions, IInternalProcessorContext processorContext)
        {
            throw new NotImplementedException();
        }

        public void Close(bool clean)
        {
            throw new NotImplementedException();
        }

        public Dictionary<TopicPartition, long?> Checkpointed()
        {
            throw new NotImplementedException();
        }
    }
}


//        private const string STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog";
//
//        private readonly ILogger logger;
//        private readonly TaskId taskId;
//        private readonly string logPrefix;
//        private readonly bool isStandby;
//        private readonly IChangelogReader changelogReader;
//        private readonly Dictionary<TopicPartition, long> offsetLimits;
//        private readonly Dictionary<TopicPartition, long> standbyRestoredOffsets;
//        private readonly Dictionary<string, IStateRestoreCallback>? restoreCallbacks; // used for standby tasks, keyed by state topic Name
//        private readonly Dictionary<string, IRecordConverter>? recordConverters; // used for standby tasks, keyed by state topic Name
//        private readonly Dictionary<string, string> storeToChangelogTopic;
//
//        // must be maintained in topological order
//        private readonly Dictionary<string, IStateStore?> registeredStores = new Dictionary<string, IStateStore?>();
//        private readonly Dictionary<string, IStateStore?> globalStores = new Dictionary<string, IStateStore?>();
//
//        public List<TopicPartition> ChangelogPartitions { get; } = new List<TopicPartition>();
//
//        // TODO: this map does not work with customized grouper where multiple partitions
//        // of the same topic can be assigned to the same task.
//        private readonly Dictionary<string, TopicPartition> partitionForTopic;
//
//        private readonly bool eosEnabled;
//        public DirectoryInfo BaseDir { get; }
//        public Dictionary<TopicPartition, long> ChangelogOffsets { get; internal set; }
//
//        private OffsetCheckpoint? checkpointFile;
//        private readonly Dictionary<TopicPartition, long?> checkpointFileCache = new Dictionary<TopicPartition, long?>();
//        private readonly Dictionary<TopicPartition, long> initialLoadedCheckpoints;
//
//        /**
//         * @throws ProcessorStateException if the task directory does not exist and could not be created
//         * @throws IOException             if any severe error happens while creating or locking the state directory
//         */
//        public ProcessorStateManager(
//            ILogger<ProcessorStateManager> logger,
//            TaskId taskId,
//            List<TopicPartition> sources,
//            bool isStandby,
//            StateDirectory stateDirectory,
//            Dictionary<string, string> storeToChangelogTopic,
//            IChangelogReader changelogReader,
//            bool eosEnabled)
//        {
//            if (stateDirectory is null)
//            {
//                throw new ArgumentNullException(nameof(stateDirectory));
//            }
//
//            this.eosEnabled = eosEnabled;
//
//            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
//
//            this.taskId = taskId;
//            this.changelogReader = changelogReader;
//            this.logPrefix = $"task [{taskId}] ";
//
//            this.partitionForTopic = new Dictionary<string, TopicPartition>();
//
//            foreach (var source in sources ?? Enumerable.Empty<TopicPartition>())
//            {
//                this.partitionForTopic.Add(source.Topic, source);
//            }
//
//            this.offsetLimits = new Dictionary<TopicPartition, long>();
//            this.standbyRestoredOffsets = new Dictionary<TopicPartition, long>();
//            this.isStandby = isStandby;
//            this.restoreCallbacks = isStandby
//                ? new Dictionary<string, IStateRestoreCallback>()
//                : null;
//
//            this.recordConverters = isStandby ? new Dictionary<string, IRecordConverter>() : null;
//            this.storeToChangelogTopic = new Dictionary<string, string>(storeToChangelogTopic);
//
//            this.BaseDir = stateDirectory.DirectoryForTask(taskId);
//            this.checkpointFile = new OffsetCheckpoint(new FileInfo(Path.Combine(this.BaseDir.FullName, StateManagerUtil.CHECKPOINT_FILE_NAME)));
//            this.initialLoadedCheckpoints = this.checkpointFile.Read();
//
//            this.logger.LogTrace($"Checkpointable offsets read from checkpoint: {this.initialLoadedCheckpoints}");
//
//            if (eosEnabled)
//            {
//                // with EOS enabled, there should never be a checkpoint file _during_ processing.
//                // delete the checkpoint file after loading its stored offsets.
//                this.checkpointFile.Delete();
//                this.checkpointFile = null;
//            }
//
//            this.logger.LogDebug($"Created state store manager for task {taskId}");
//        }
//
//        public static string StoreChangelogTopic(
//            string applicationId,
//            string storeName)
//        {
//            return $"{applicationId}-{storeName}{STATE_CHANGELOG_TOPIC_SUFFIX}";
//        }
//
//        public void Register(
//            IStateStore store,
//            IStateRestoreCallback stateRestoreCallback)
//        {
//            if (store is null)
//            {
//                throw new ArgumentNullException(nameof(store));
//            }
//
//            var storeName = store.Name;
//            this.logger.LogDebug("Registering state store {} to its state manager", storeName);
//
//            if (StateManagerUtil.CHECKPOINT_FILE_NAME.Equals(storeName))
//            {
//                throw new ArgumentException($"{this.logPrefix}Illegal store Name: {storeName}");
//            }
//
//            if (this.registeredStores.ContainsKey(storeName) && this.registeredStores[storeName] != null)
//            {
//                throw new ArgumentException($"{this.logPrefix}Store {storeName} has already been registered.");
//            }
//
//            // check that the underlying change log topic exist or not
//            var topic = this.storeToChangelogTopic[storeName];
//            if (topic != null)
//            {
//                var storePartition = new TopicPartition(topic, this.GetPartition(topic));
//
//                IRecordConverter recordConverter = StateManagerUtil.ConverterForStore(store);
//
//                if (this.isStandby)
//                {
//                    this.logger.LogTrace("Preparing standby replica of Persistent state store {} with changelog topic {}", storeName, topic);
//
//                    this.restoreCallbacks?.Add(topic, stateRestoreCallback);
//                    this.recordConverters?.Add(topic, recordConverter);
//                }
//                else
//                {
//                    long? restoreCheckpoint = this.initialLoadedCheckpoints[storePartition];
//
//                    if (restoreCheckpoint != null)
//                    {
//                        this.checkpointFileCache.Add(storePartition, restoreCheckpoint.Value);
//                    }
//
//                    this.logger.LogTrace("Restoring state store {} from changelog topic {} at checkpoint {}", storeName, topic, restoreCheckpoint);
//
//                    var restorer = new StateRestorer(
//                        storePartition,
//                        new CompositeRestoreListener(stateRestoreCallback),
//                        restoreCheckpoint.GetValueOrDefault(-1),
//                        this.OffsetLimit(storePartition),
//                        store.Persistent(),
//                        storeName,
//                        recordConverter
//                    );
//
//                    this.changelogReader.Register(restorer);
//                }
//
//                this.ChangelogPartitions.Add(storePartition);
//            }
//
//            this.registeredStores.Add(storeName, null);
//        }
//
//        public void ReinitializeStateStoresForPartitions(
//            List<TopicPartition> partitions,
//            IInternalProcessorContext processorContext)
//        {
//            StateManagerUtil.ReinitializeStateStoresForPartitions(
//                this.logger,
//                this.eosEnabled,
//                this.BaseDir,
//                this.registeredStores,
//                this.storeToChangelogTopic,
//                partitions,
//                processorContext,
//                this.checkpointFile,
//                this.checkpointFileCache);
//        }
//
//        public void ClearCheckpoints()
//        {
//            if (this.checkpointFile != null)
//            {
//                this.checkpointFile.Delete();
//                this.checkpointFile = null;
//
//                this.checkpointFileCache.Clear();
//            }
//        }
//
//        public Dictionary<TopicPartition, long?> Checkpointed()
//        {
//            this.UpdateCheckpointFileCache(new Dictionary<TopicPartition, long>());
//            var partitionsAndOffsets = new Dictionary<TopicPartition, long?>();
//
//            foreach (var entry in this.restoreCallbacks)
//            {
//                var topicName = entry.Key;
//                var partition = this.GetPartition(topicName);
//                var storePartition = new TopicPartition(topicName, partition);
//
//                partitionsAndOffsets.Add(storePartition, this.checkpointFileCache.GetValueOrDefault(storePartition, -1L));
//            }
//
//            return partitionsAndOffsets;
//        }
//
//        public void UpdateStandbyStates(
//            TopicPartition storePartition,
//            List<ConsumeResult<byte[], byte[]>> restoreRecords,
//            long lastOffset)
//        {
//            // restore states from changelog records
//            IRecordBatchingStateRestoreCallback restoreCallback = StateRestoreCallbackAdapter.Adapt(this.restoreCallbacks[storePartition.Topic]);
//
//            if (restoreRecords.Any())
//            {
//                IRecordConverter converter = this.recordConverters[storePartition.Topic];
//                var convertedRecords = new List<ConsumeResult<byte[], byte[]>>(restoreRecords.Count);
//                foreach (ConsumeResult<byte[], byte[]> record in restoreRecords)
//                {
//                    convertedRecords.Add(converter.Convert(record));
//                }
//
//                try
//                {
//                    restoreCallback.RestoreBatch(convertedRecords);
//                }
//                catch (RuntimeException e)
//                {
//                    throw new ProcessorStateException(string.Format("%sException caught while trying to restore state from %s", this.logPrefix, storePartition), e);
//                }
//            }
//
//            // record the restored offset for its change log partition
//            this.standbyRestoredOffsets.Add(storePartition, lastOffset + 1);
//        }
//
//        public void PutOffsetLimit(
//            TopicPartition partition,
//            long limit)
//        {
//            this.logger.LogTrace("Updating store offset limit for partition {} to {}", partition, limit);
//            this.offsetLimits.Add(partition, limit);
//        }
//
//        public long OffsetLimit(TopicPartition partition)
//        {
//            long? limit = this.offsetLimits[partition];
//
//            return limit != null
//                ? limit.Value
//                : long.MaxValue;
//        }
//
//        public IStateStore? GetStore(string Name)
//        {
//            return this.registeredStores.GetValueOrDefault(Name, null);
//        }
//
//        public void Flush()
//        {
//            ProcessorStateException firstException = null;
//            // attempting to Flush the stores
//            if (this.registeredStores.Any())
//            {
//                this.logger.LogDebug("Flushing All stores registered in the state manager");
//                foreach (KeyValuePair<string, IStateStore?> entry in this.registeredStores)
//                {
//                    if (entry.Value.IsPresent())
//                    {
//                        IStateStore store = entry.Value;
//                        this.logger.LogTrace("Flushing store {}", store.Name);
//                        try
//                        {
//
//                            store.Flush();
//                        }
//                        catch (RuntimeException e)
//                        {
//                            if (firstException == null)
//                            {
//                                firstException = new ProcessorStateException(string.Format("%sFailed to Flush state store %s", this.logPrefix, store.Name), e);
//                            }
//                            this.logger.LogError("Failed to Flush state store {}: ", store.Name, e);
//                        }
//                    }
//                    else
//                    {
//
//                        throw new InvalidOperationException("Expected " + entry.Key + " to have been initialized");
//                    }
//                }
//            }
//
//            if (firstException != null)
//            {
//                throw firstException;
//            }
//        }
//
//        /**
//         * {@link IStateStore#Close() Close} All stores (even in case of failure).
//         * Log All exception and re-throw the first exception that did occur at the end.
//         *
//         * @throws ProcessorStateException if any error happens when closing the state stores
//         */
//
//        public void Close(bool clean)
//        {
//            ProcessorStateException firstException = null;
//            // attempting to Close the stores, just in case they
//            // are not closed by a ProcessorNode yet
//            if (this.registeredStores.Any())
//            {
//                this.logger.LogDebug("Closing its state manager and All the registered state stores");
//                foreach (var entry in this.registeredStores)
//                {
//                    if (entry.Value.IsPresent())
//                    {
//                        IStateStore store = entry.Value;
//                        this.logger.LogDebug("Closing storage engine {}", store.Name);
//                        try
//                        {
//                            store.Close();
//                            this.registeredStores.Add(store.Name, null);
//                        }
//                        catch (RuntimeException e)
//                        {
//                            if (firstException == null)
//                            {
//                                firstException = new ProcessorStateException(string.Format("%sFailed to Close state store %s", this.logPrefix, store.Name), e);
//                            }
//
//                            this.logger.LogError("Failed to Close state store {}: ", store.Name, e);
//                        }
//                    }
//                    else
//                    {
//
//                        this.logger.LogInformation("Skipping to Close non-initialized store {}", entry.Key);
//                    }
//                }
//            }
//
//            if (!clean && this.eosEnabled)
//            {
//                // delete the checkpoint file if this is an unclean Close
//                try
//                {
//                    this.ClearCheckpoints();
//                }
//                catch (IOException e)
//                {
//                    throw new ProcessorStateException($"{this.logPrefix}Error while deleting the checkpoint file", e);
//                }
//            }
//
//            if (firstException != null)
//            {
//                throw firstException;
//            }
//        }
//
//
//        public void Checkpoint(Dictionary<TopicPartition, long> checkpointableOffsetsFromProcessing)
//        {
//            this.EnsureStoresRegistered();
//
//            // write the checkpoint file before closing
//            if (this.checkpointFile == null)
//            {
//                this.checkpointFile = new OffsetCheckpoint(new FileInfo(Path.Combine(this.BaseDir.FullName, StateManagerUtil.CHECKPOINT_FILE_NAME)));
//            }
//
//            this.UpdateCheckpointFileCache(checkpointableOffsetsFromProcessing);
//
//            this.logger.LogTrace("Checkpointable offsets updated with active acked offsets: {}", this.checkpointFileCache);
//
//            this.logger.LogTrace("Writing checkpoint: {}", this.checkpointFileCache);
//
//            try
//            {
//                this.checkpointFile.Write(this.checkpointFileCache);
//            }
//            catch (IOException e)
//            {
//                this.logger.LogWarning("Failed to write offset checkpoint file to [{}]", this.checkpointFile, e);
//            }
//        }
//
//        private void UpdateCheckpointFileCache(Dictionary<TopicPartition, long> checkpointableOffsetsFromProcessing)
//        {
//            HashSet<TopicPartition> _validCheckpointableTopics = this.ValidCheckpointableTopics();
//            var restoredOffsets = ValidCheckpointableOffsets(
//                this.changelogReader.GetRestoredOffsets(),
//                this.ValidCheckpointableTopics());
//
//            this.logger.LogTrace("Checkpointable offsets updated with restored offsets: {}", this.checkpointFileCache);
//            foreach (TopicPartition topicPartition in this.ValidCheckpointableTopics())
//            {
//                if (checkpointableOffsetsFromProcessing.ContainsKey(topicPartition))
//                {
//                    // if we have just recently processed some offsets,
//                    // store the last offset + 1 (the log position after restoration)
//                    this.checkpointFileCache.Add(topicPartition, checkpointableOffsetsFromProcessing[topicPartition] + 1);
//                }
//                else if (this.standbyRestoredOffsets.ContainsKey(topicPartition))
//                {
//                    // or if we restored some offset as a standby task, use it
//                    this.checkpointFileCache.Add(topicPartition, this.standbyRestoredOffsets[topicPartition]);
//                }
//                else if (restoredOffsets.ContainsKey(topicPartition))
//                {
//                    // or if we restored some offset as an active task, use it
//                    this.checkpointFileCache.Add(topicPartition, restoredOffsets[topicPartition]);
//                }
//                else if (this.checkpointFileCache.ContainsKey(topicPartition))
//                {
//                    // or if we have a prior value we've cached (and written to the checkpoint file), then keep it
//                }
//                else
//                {
//                    // As a last resort, fall back to the offset we loaded from the checkpoint file at startup, but
//                    // only if the offset is actually valid for our current state stores.
//                    var loadedOffset = ValidCheckpointableOffsets(this.initialLoadedCheckpoints, this.ValidCheckpointableTopics())[topicPartition];
//
//                    if (loadedOffset != null)
//                    {
//                        this.checkpointFileCache.Add(topicPartition, loadedOffset);
//                    }
//                }
//            }
//        }
//
//        private int GetPartition(string topic)
//        {
//            TopicPartition partition = this.partitionForTopic[topic];
//            return partition == null ? this.taskId.Partition : partition.Partition.Value;
//        }
//
//        public void RegisterGlobalStateStores(List<IStateStore> stateStores)
//        {
//            this.logger.LogDebug("Register global stores {}", stateStores);
//            foreach (IStateStore stateStore in stateStores)
//            {
//                this.globalStores.Add(stateStore.Name, stateStore);
//            }
//        }
//
//
//        public IStateStore? GetGlobalStore(string Name)
//        {
//            return this.globalStores.GetValueOrDefault(Name, null);
//        }
//
//        public void EnsureStoresRegistered()
//        {
//            foreach (var entry in this.registeredStores)
//            {
//                if (!entry.Value.IsPresent())
//                {
//                    throw new InvalidOperationException(
//                        $"store [{entry.Key}] has not been correctly registered. This is a bug in Kafka Streams.");
//                }
//            }
//        }
//
//        private HashSet<TopicPartition> ValidCheckpointableTopics()
//        {
//            // it's only valid to record checkpoints for registered stores that are both Persistent and change-logged
//
//            var result = new HashSet<TopicPartition>();
//            foreach (KeyValuePair<string, string> storeToChangelog in this.storeToChangelogTopic)
//            {
//                var storeName = storeToChangelog.Key;
//                if (this.registeredStores.ContainsKey(storeName)
//                    && this.registeredStores[storeName].IsPresent()
//                    && this.registeredStores[storeName].Persistent())
//                {
//
//                    var changelogTopic = storeToChangelog.Value;
//                    result.Add(new TopicPartition(changelogTopic, this.GetPartition(changelogTopic)));
//                }
//            }
//            return result;
//        }
//
//        private static Dictionary<TopicPartition, long?> ValidCheckpointableOffsets(
//            Dictionary<TopicPartition, long> checkpointableOffsets,
//            HashSet<TopicPartition> validCheckpointableTopics)
//        {
//            var result = new Dictionary<TopicPartition, long?>(checkpointableOffsets.Count);
//
//            foreach (var topicToCheckpointableOffset in checkpointableOffsets)
//            {
//                TopicPartition topic = topicToCheckpointableOffset.Key;
//                if (validCheckpointableTopics.Contains(topic))
//                {
//                    var checkpointableOffset = topicToCheckpointableOffset.Value;
//                    result.Add(topic, checkpointableOffset);
//                }
//            }
//
//            return result;
//        }
//    }
//}
