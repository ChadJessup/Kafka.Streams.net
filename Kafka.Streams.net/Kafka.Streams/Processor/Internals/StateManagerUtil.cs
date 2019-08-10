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
using Kafka.Streams.IProcessor.Interfaces;
using Kafka.Streams.State.Internals;
using System.Collections.Generic;
using System.IO;

namespace Kafka.Streams.IProcessor.Internals
{

    public class StateManagerUtil
    {

        static string CHECKPOINT_FILE_NAME = ".checkpoint";

        private StateManagerUtil() { }

        static RecordConverter converterForStore(IStateStore store)
        {
            return isTimestamped(store) ? rawValueToTimestampedValue() : identity();
        }

        public static void reinitializeStateStoresForPartitions(ILogger log,
                                                                bool eosEnabled,
                                                                FileInfo baseDir,
                                                                FixedOrderMap<string, Optional<IStateStore>> stateStores,
                                                                Dictionary<string, string> storeToChangelogTopic,
                                                                List<TopicPartition> partitions,
                                                                IInternalProcessorContext<K, V> processorContext,
                                                                OffsetCheckpoint checkpointFile,
                                                                Dictionary<TopicPartition, long> checkpointFileCache)
        {
            Dictionary<string, string> changelogTopicToStore = inverseOneToOneMap(storeToChangelogTopic);
            HashSet<string> storesToBeReinitialized = new HashSet<>();

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
                IStateStore stateStore = stateStores[storeName]
                    .orElseThrow(
                        () => new InvalidOperationException(
                            "Re-initializing store that has not been initialized. This is a bug in Kafka Streams."
                        )
                    );

                try
                {

                    stateStore.close();
                }
                catch (RuntimeException ignoreAndSwallow) { /* ignore */ }
                processorContext.uninitialize();
                stateStores.Add(storeName, Optional.empty());

                // TODO Remove this eventually
                // => (only after we are sure, we don't need it for backward compatibility reasons anymore; maybe 2.0 release?)
                // this is an ugly "hack" that is required because RocksDbStore does not follow the pattern to put the
                // store directory as <taskDir>/<storeName> but nests it with an intermediate <taskDir>/rocksdb/<storeName>
                try
                {

                    Utils.delete(new FileInfo(baseDir + FileInfo.separator + "rocksdb" + FileInfo.separator + storeName));
                }
                catch (IOException fatalException)
                {
                    log.LogError("Failed to reinitialize store {}.", storeName, fatalException);
                    throw new StreamsException(string.Format("Failed to reinitialize store %s.", storeName), fatalException);
                }

                try
                {

                    Utils.delete(new FileInfo(baseDir + FileInfo.separator + storeName));
                }
                catch (IOException fatalException)
                {
                    log.LogError("Failed to reinitialize store {}.", storeName, fatalException);
                    throw new StreamsException(string.Format("Failed to reinitialize store %s.", storeName), fatalException);
                }

                stateStore.init(processorContext, stateStore);
            }
        }

        private static Dictionary<string, string> inverseOneToOneMap(Dictionary<string, string> origin)
        {
            Dictionary<string, string> reversedMap = new Dictionary<>();
            foreach (KeyValuePair<string, string> entry in origin)
            {
                reversedMap.Add(entry.Value, entry.Key);
            }
            return reversedMap;
        }

    }
}