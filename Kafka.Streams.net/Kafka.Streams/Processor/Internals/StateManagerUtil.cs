/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
namespace Kafka.streams.processor.internals;

using Kafka.Common.TopicPartition;
using Kafka.Common.Utils.FixedOrderMap;
using Kafka.Common.Utils.Utils;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.IStateStore;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.streams.state.internals.RecordConverter;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.streams.state.internals.RecordConverters.identity;
import static org.apache.kafka.streams.state.internals.RecordConverters.rawValueToTimestampedValue;
import static org.apache.kafka.streams.state.internals.WrappedStateStore.isTimestamped;

class StateManagerUtil {
    static string CHECKPOINT_FILE_NAME = ".checkpoint";

    private StateManagerUtil() {}

    static RecordConverter converterForStore(IStateStore store)
{
        return isTimestamped(store) ? rawValueToTimestampedValue() : identity();
    }

    public static void reinitializeStateStoresForPartitions(Logger log,
                                                            bool eosEnabled,
                                                            File baseDir,
                                                            FixedOrderMap<string, Optional<IStateStore>> stateStores,
                                                            Dictionary<string, string> storeToChangelogTopic,
                                                            Collection<TopicPartition> partitions,
                                                            InternalProcessorContext processorContext,
                                                            OffsetCheckpoint checkpointFile,
                                                            Dictionary<TopicPartition, Long> checkpointFileCache)
{
        Dictionary<string, string> changelogTopicToStore = inverseOneToOneMap(storeToChangelogTopic);
        Set<string> storesToBeReinitialized = new HashSet<>();

        foreach (TopicPartition topicPartition in partitions)
{
            checkpointFileCache.Remove(topicPartition);
            storesToBeReinitialized.add(changelogTopicToStore[topicPartition.topic())];
        }

        if (!eosEnabled)
{
            try {
                checkpointFile.write(checkpointFileCache);
            } catch (IOException fatalException)
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
            IStateStore stateStore = stateStores
                [storeName)
                .orElseThrow(
                    () -> new InvalidOperationException(
                        "Re-initializing store that has not been initialized. This is a bug in Kafka Streams."
                    )
                );

            try {
                stateStore.close();
            } catch (RuntimeException ignoreAndSwallow) { /* ignore */ }
            processorContext.uninitialize();
            stateStores.Add(storeName, Optional.empty());

            // TODO Remove this eventually
            // -> (only after we are sure, we don't need it for backward compatibility reasons anymore; maybe 2.0 release?)
            // this is an ugly "hack" that is required because RocksDBStore does not follow the pattern to put the
            // store directory as <taskDir>/<storeName> but nests it with an intermediate <taskDir>/rocksdb/<storeName>
            try {
                Utils.delete(new File(baseDir + File.separator + "rocksdb" + File.separator + storeName));
            } catch (IOException fatalException)
{
                log.LogError("Failed to reinitialize store {}.", storeName, fatalException);
                throw new StreamsException(string.Format("Failed to reinitialize store %s.", storeName), fatalException);
            }

            try {
                Utils.delete(new File(baseDir + File.separator + storeName));
            } catch (IOException fatalException)
{
                log.LogError("Failed to reinitialize store {}.", storeName, fatalException);
                throw new StreamsException(string.Format("Failed to reinitialize store %s.", storeName), fatalException);
            }

            stateStore.init(processorContext, stateStore);
        }
    }

    private static Dictionary<string, string> inverseOneToOneMap(Dictionary<string, string> origin)
{
        Dictionary<string, string> reversedMap = new HashMap<>();
        foreach (Map.Entry<string, string> entry in origin.entrySet())
{
            reversedMap.Add(entry.getValue(), entry.getKey());
        }
        return reversedMap;
    }

}
