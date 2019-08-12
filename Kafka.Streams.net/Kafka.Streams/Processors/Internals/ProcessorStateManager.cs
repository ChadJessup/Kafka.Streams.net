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
using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Kafka.Streams.Processor.Internals
{
    public class ProcessorStateManager<K, V> : IStateManager
    {
        private static string STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog";

        private ILogger log;
        private TaskId taskId;
        private string logPrefix;
        private bool isStandby;
        private IChangelogReader changelogReader;
        private Dictionary<TopicPartition, long> offsetLimits;
        private Dictionary<TopicPartition, long> standbyRestoredOffsets;
        private Dictionary<string, IStateRestoreCallback> restoreCallbacks; // used for standby tasks, keyed by state topic name
        private Dictionary<string, IRecordConverter> recordConverters; // used for standby tasks, keyed by state topic name
        private Dictionary<string, string> storeToChangelogTopic;

        // must be maintained in topological order
        private Dictionary<string, IStateStore?> registeredStores = new Dictionary<string, IStateStore?>();
        private Dictionary<string, IStateStore?> globalStores = new Dictionary<string, IStateStore?>();

        private List<TopicPartition> changelogPartitions = new List<TopicPartition>();

        // TODO: this map does not work with customized grouper where multiple partitions
        // of the same topic can be assigned to the same task.
        private Dictionary<string, TopicPartition> partitionForTopic;

        private bool eosEnabled;
        public DirectoryInfo baseDir { get; }
        private OffsetCheckpoint checkpointFile;
        private Dictionary<TopicPartition, long> checkpointFileCache = new Dictionary<>();
        private Dictionary<TopicPartition, long> initialLoadedCheckpoints;

        /**
         * @throws ProcessorStateException if the task directory does not exist and could not be created
         * @throws IOException             if any severe error happens while creating or locking the state directory
         */
        public ProcessorStateManager(
            TaskId taskId,
            List<TopicPartition> sources,
            bool isStandby,
            StateDirectory stateDirectory,
            Dictionary<string, string> storeToChangelogTopic,
            IChangelogReader changelogReader,
            bool eosEnabled,
            LogContext logContext)
        {
            this.eosEnabled = eosEnabled;

            log = logContext.logger(typeof(ProcessorStateManager<K, V>));
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
            restoreCallbacks = isStandby ? new Dictionary<string, IStateRestoreCallback>() : null;
            recordConverters = isStandby ? new Dictionary<string, IRecordConverter>() : null;
            this.storeToChangelogTopic = new Dictionary<string, string>(storeToChangelogTopic);

            baseDir = stateDirectory.directoryForTask(taskId);
            checkpointFile = new OffsetCheckpoint(new FileInfo(Path.Combine(baseDir, CHECKPOINT_FILE_NAME)));
            initialLoadedCheckpoints = checkpointFile.read();

            log.LogTrace("Checkpointable offsets read from checkpoint: {}", initialLoadedCheckpoints);

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

        public void register(
            IStateStore store,
            IStateRestoreCallback stateRestoreCallback)
        {
            string storeName = store.name;
            log.LogDebug("Registering state store {} to its state manager", storeName);

            if (CHECKPOINT_FILE_NAME.Equals(storeName))
            {
                throw new System.ArgumentException(string.Format("%sIllegal store name: %s", logPrefix, storeName));
            }

            if (registeredStores.ContainsKey(storeName) && registeredStores[storeName].isPresent())
            {
                throw new System.ArgumentException(string.Format("%sStore %s has already been registered.", logPrefix, storeName));
            }

            // check that the underlying change log topic exist or not
            string topic = storeToChangelogTopic[storeName];
            if (topic != null)
            {
                TopicPartition storePartition = new TopicPartition(topic, getPartition(topic));

                IRecordConverter recordConverter = converterForStore(store);

                if (isStandby)
                {
                    log.LogTrace("Preparing standby replica of persistent state store {} with changelog topic {}", storeName, topic);

                    restoreCallbacks.Add(topic, stateRestoreCallback);
                    recordConverters.Add(topic, recordConverter);
                }
                else
                {

                    long restoreCheckpoint = store.persistent() ? initialLoadedCheckpoints[storePartition] : null;
                    if (restoreCheckpoint != null)
                    {
                        checkpointFileCache.Add(storePartition, restoreCheckpoint);
                    }
                    log.LogTrace("Restoring state store {} from changelog topic {} at checkpoint {}", storeName, topic, restoreCheckpoint);

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
                changelogPartitions.Add(storePartition);
            }

            registeredStores.Add(storeName, Optional.of(store));
        }


        public void reinitializeStateStoresForPartitions(
            List<TopicPartition> partitions,
            IInternalProcessorContext<K, V> processorContext)
        {
            StateManagerUtil.reinitializeStateStoresForPartitions(
                log,
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
                checkpointFile.delete();
                checkpointFile = null;

                checkpointFileCache.clear();
            }
        }


        public Dictionary<TopicPartition, long> checkpointed()
        {
            updateCheckpointFileCache(emptyMap());
            Dictionary<TopicPartition, long> partitionsAndOffsets = new Dictionary<>();

            foreach (KeyValuePair<string, IStateRestoreCallback> entry in restoreCallbacks)
            {
                string topicName = entry.Key;
                int partition = getPartition(topicName);
                TopicPartition storePartition = new TopicPartition(topicName, partition);

                partitionsAndOffsets.Add(storePartition, checkpointFileCache.getOrDefault(storePartition, -1L));
            }
            return partitionsAndOffsets;
        }

        void updateStandbyStates(TopicPartition storePartition,
                                 List<ConsumeResult<byte[], byte[]>> restoreRecords,
                                 long lastOffset)
        {
            // restore states from changelog records
            IRecordBatchingStateRestoreCallback restoreCallback = adapt(restoreCallbacks[storePartition.Topic]);

            if (restoreRecords.Any())
            {
                IRecordConverter converter = recordConverters[storePartition.Topic];
                List<ConsumeResult<byte[], byte[]>> convertedRecords = new List<ConsumeResult<byte[], byte[]>>(restoreRecords.Count);
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

        public void putOffsetLimit(TopicPartition partition,
                            long limit)
        {
            log.LogTrace("Updating store offset limit for partition {} to {}", partition, limit);
            offsetLimits.Add(partition, limit);
        }

        long offsetLimit(TopicPartition partition)
        {
            long limit = offsetLimits[partition];
            return limit != null ? limit : long.MaxValue;
        }


        public IStateStore getStore(string name)
        {
            return registeredStores.getOrDefault(name, Optional.empty()).orElse(null);
        }


        public void flush()
        {
            ProcessorStateException firstException = null;
            // attempting to flush the stores
            if (registeredStores.Any())
            {
                log.LogDebug("Flushing all stores registered in the state manager");
                foreach (KeyValuePair<string, IStateStore?> entry in registeredStores)
                {
                    if (entry.Value.isPresent())
                    {
                        IStateStore store = entry.Value;
                        log.LogTrace("Flushing store {}", store.name);
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
                            log.LogError("Failed to flush state store {}: ", store.name, e);
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

        public void close(bool clean)
        {
            ProcessorStateException firstException = null;
            // attempting to close the stores, just in case they
            // are not closed by a ProcessorNode yet
            if (registeredStores.Any())
            {
                log.LogDebug("Closing its state manager and all the registered state stores");
                foreach (var entry in registeredStores)
                {
                    if (entry.Value.isPresent())
                    {
                        IStateStore store = entry.Value.get();
                        log.LogDebug("Closing storage engine {}", store.name);
                        try
                        {

                            store.close();
                            registeredStores.Add(store.name, Optional.empty());
                        }
                        catch (RuntimeException e)
                        {
                            if (firstException == null)
                            {
                                firstException = new ProcessorStateException(string.Format("%sFailed to close state store %s", logPrefix, store.name), e);
                            }
                            log.LogError("Failed to close state store {}: ", store.name, e);
                        }
                    }
                    else
                    {

                        log.LogInformation("Skipping to close non-initialized store {}", entry.Key);
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
                checkpointFile = new OffsetCheckpoint(new DirectoryInfo(baseDir, CHECKPOINT_FILE_NAME));
            }

            updateCheckpointFileCache(checkpointableOffsetsFromProcessing);

            log.LogTrace("Checkpointable offsets updated with active acked offsets: {}", checkpointFileCache);

            log.LogTrace("Writing checkpoint: {}", checkpointFileCache);

            try
            {
                checkpointFile.write(checkpointFileCache);
            }
            catch (IOException e)
            {
                log.LogWarning("Failed to write offset checkpoint file to [{}]", checkpointFile, e);
            }
        }

        private void updateCheckpointFileCache(Dictionary<TopicPartition, long> checkpointableOffsetsFromProcessing)
        {
            HashSet<TopicPartition> _validCheckpointableTopics = validCheckpointableTopics();
            Dictionary<TopicPartition, long> restoredOffsets = validCheckpointableOffsets(
                changelogReader.restoredOffsets(),
                validCheckpointableTopics);

            log.LogTrace("Checkpointable offsets updated with restored offsets: {}", checkpointFileCache);
            foreach (TopicPartition topicPartition in validCheckpointableTopics)
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
                    long loadedOffset = validCheckpointableOffsets(initialLoadedCheckpoints, validCheckpointableTopics)[topicPartition];

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
            log.LogDebug("Register global stores {}", stateStores);
            foreach (IStateStore stateStore in stateStores)
            {
                globalStores.Add(stateStore.name, Optional.of(stateStore));
            }
        }


        public IStateStore getGlobalStore(string name)
        {
            return globalStores.getOrDefault(name, Optional.empty()).orElse(null);
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

            HashSet<TopicPartition> result = new HashSet<TopicPartition>();
            foreach (KeyValuePair<string, string> storeToChangelog in storeToChangelogTopic)
            {
                string storeName = storeToChangelog.Key;
                if (registeredStores.ContainsKey(storeName)
                    && registeredStores[storeName].isPresent()
                    && registeredStores[storeName].persistent())
                {

                    string changelogTopic = storeToChangelog.Value;
                    result.Add(new TopicPartition(changelogTopic, getPartition(changelogTopic)));
                }
            }
            return result;
        }

        private static Dictionary<TopicPartition, long> validCheckpointableOffsets(
            Dictionary<TopicPartition, long> checkpointableOffsets,
            HashSet<TopicPartition> validCheckpointableTopics)
        {
            Dictionary<TopicPartition, long> result = new Dictionary<TopicPartition, long>(checkpointableOffsets.size());

            foreach (KeyValuePair<TopicPartition, long> topicToCheckpointableOffset in checkpointableOffsets)
            {
                TopicPartition topic = topicToCheckpointableOffset.Key;
                if (validCheckpointableTopics.Contains(topic))
                {
                    long checkpointableOffset = topicToCheckpointableOffset.Value;
                    result.Add(topic, checkpointableOffset);
                }
            }

            return result;
        }
    }
}