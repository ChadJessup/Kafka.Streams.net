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

namespace Kafka.Streams.Processor.Internals
{
    public class StateManagerUtil
    {
        public static string CHECKPOINT_FILE_NAME = ".checkpoint";

        private StateManagerUtil() { }

        public static IRecordConverter converterForStore(IStateStore store)
        {
            return WrappedStateStore.isTimestamped(store)
                ? RecordConverters.rawValueToTimestampedValue()
                : RecordConverters.identity();
        }

        public static void reinitializeStateStoresForPartitions<K, V>(
            ILogger log,
            bool eosEnabled,
            DirectoryInfo baseDir,
            Dictionary<string, IStateStore?> stateStores,
            Dictionary<string, string> storeToChangelogTopic,
            List<TopicPartition> partitions,
            IInternalProcessorContext<K, V> processorContext,
            OffsetCheckpoint checkpointFile,
            Dictionary<TopicPartition, long> checkpointFileCache)
        {
            Dictionary<string, string> changelogTopicToStore = inverseOneToOneMap(storeToChangelogTopic);
            HashSet<string> storesToBeReinitialized = new HashSet<string>();

            foreach (TopicPartition topicPartition in partitions)
            {
                checkpointFileCache.Remove(topicPartition);
                storesToBeReinitialized.Add(changelogTopicToStore[topicPartition.Topic]);
            }

            if (!eosEnabled)
            {
                try
                {

                    checkpointFile.write(checkpointFileCache);
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

            foreach (string storeName in storesToBeReinitialized)
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
                    stateStore?.close();
                }
                catch (RuntimeException ignoreAndSwallow)
                {
                    /* ignore */
                }

                processorContext.uninitialize();
                stateStores.Add(storeName, null);

                // TODO Remove this eventually
                // => (only after we are sure, we don't need it for backward compatibility reasons anymore; maybe 2.0 release?)
                // this is an ugly "hack" that is required because RocksDbStore does not follow the pattern to put the
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

                stateStore?.init(processorContext, stateStore);
            }
        }

        private static Dictionary<string, string> inverseOneToOneMap(Dictionary<string, string> origin)
        {
            Dictionary<string, string> reversedMap = new Dictionary<string, string>();

            foreach (KeyValuePair<string, string> entry in origin)
            {
                reversedMap.Add(entry.Value, entry.Key);
            }

            return reversedMap;
        }
    }
}