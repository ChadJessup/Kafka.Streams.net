
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

namespace Kafka.Streams.Processors.Internals
{
    public static class StateManagerUtil
    {
        public const string CHECKPOINT_FILE_NAME = ".checkpoint";

        public static IRecordConverter ConverterForStore(IStateStore store)
        {
            return WrappedStateStore.IsTimestamped(store)
                ? RecordConverters.RawValueToTimestampedValue()
                : RecordConverters.Identity();
        }

        public static void ReinitializeStateStoresForPartitions(
            ILogger log,
            bool eosEnabled,
            DirectoryInfo baseDir,
            Dictionary<string, IStateStore?> stateStores,
            Dictionary<string, string> storeToChangelogTopic,
            List<TopicPartition> partitions,
            IInternalProcessorContext processorContext,
            OffsetCheckpoint checkpointFile,
            Dictionary<TopicPartition, long?> checkpointFileCache)
        {
            Dictionary<string, string> changelogTopicToStore = InverseOneToOneMap(storeToChangelogTopic);
            var storesToBeReinitialized = new HashSet<string>();

            foreach (TopicPartition topicPartition in partitions)
            {
                checkpointFileCache.Remove(topicPartition);
                storesToBeReinitialized.Add(changelogTopicToStore[topicPartition.Topic]);
            }

            if (!eosEnabled)
            {
                try
                {

                    checkpointFile.Write(checkpointFileCache);
                }
                catch (IOException fatalException)
                {
                    log.LogError("Failed to write offset checkpoint file to {} while re-initializing {}: {}",
                              checkpointFile,
                              stateStores,
                              fatalException);
                    throw new StreamsException("Failed to reinitialize global store.", fatalException);
                }
            }

            foreach (var storeName in storesToBeReinitialized)
            {
                if (!stateStores.ContainsKey(storeName))
                {
                    // the store has never been registered; carry on...
                    continue;
                }

                if (!stateStores.TryGetValue(storeName, out IStateStore? stateStore))
                {
                    throw new InvalidOperationException("Re-initializing store that has not been initialized. This is a bug in Kafka Streams.");
                }

                try
                {
                    stateStore?.Close();
                }
                catch (RuntimeException ignoreAndSwallow)
                {
                    /* ignore */
                }

                processorContext.Uninitialize();
                stateStores.Add(storeName, null);

                // TODO Remove this eventually
                // => (only after we are sure, we don't need it for backward compatibility reasons anymore; maybe 2.0 release?)
                // this is an ugly "hack" that is required because RocksDbStore does not follow the pattern to Put the
                // store directory as <taskDir>/<storeName> but nests it with an intermediate <taskDir>/rocksdb/<storeName>
                try
                {

                    var di = new DirectoryInfo(Path.Combine(baseDir.FullName, "rocksdb", storeName));
                    di.Delete(recursive: true);
                }
                catch (IOException fatalException)
                {
                    log.LogError("Failed to reinitialize store {}.", storeName, fatalException);
                    throw new StreamsException(string.Format("Failed to reinitialize store %s.", storeName), fatalException);
                }

                try
                {

                    //                    Utils.delete(new DirectoryInfo(Path.Combine(baseDir, storeName));
                }
                catch (IOException fatalException)
                {
                    log.LogError("Failed to reinitialize store {}.", storeName, fatalException);
                    throw new StreamsException(string.Format("Failed to reinitialize store %s.", storeName), fatalException);
                }

                stateStore?.Init(processorContext, stateStore);
            }
        }

        private static Dictionary<string, string> InverseOneToOneMap(Dictionary<string, string> origin)
        {
            var reversedMap = new Dictionary<string, string>();

            foreach (KeyValuePair<string, string> entry in origin)
            {
                reversedMap.Add(entry.Value, entry.Key);
            }

            return reversedMap;
        }

        internal static void CloseStateManager(ILogger log, string logPrefix, bool clean, bool eosEnabled, ProcessorStateManager stateMgr, StateDirectory stateDirectory, TaskType aCTIVE)
        {
            throw new NotImplementedException();
        }

        internal static void RegisterStateStores(
            ILogger log,
            string logPrefix,
            ProcessorTopology topology,
            ProcessorStateManager stateMgr,
            StateDirectory stateDirectory,
            IInternalProcessorContext processorContext)
        {
            if (topology.StateStores.IsEmpty())
            {
                return;
            }

            TaskId id = stateMgr.TaskId;
            try
            {
                if (!stateDirectory.Lock(id))
                {
                    throw new LockException($"{logPrefix}Failed to lock the state directory for task {id}");
                }
            }
            catch (IOException e)
            {
                throw new StreamsException(
                    $"{logPrefix}Fatal error while trying to lock the state directory for task {id}", e);
            }
            log.LogDebug("Acquired state directory lock");

            bool storeDirsEmpty = stateDirectory.DirectoryForTaskIsEmpty(id);

            // We should only load checkpoint AFTER the corresponding state directory lock has been acquired and
            // the state stores have been registered; we should not try to load at the state manager construction time.
            // See https://issues.apache.org/jira/browse/KAFKA-8574
            foreach (var store in topology.StateStores)
            {
                processorContext.Uninitialize();
                store.Init(processorContext, store);
                log.LogTrace("Registered state store {}", store.Name);
            }

            stateMgr.InitializeStoreOffsetsFromCheckpoint(storeDirsEmpty);
            log.LogDebug("Initialized state stores");
        }
    }
}
