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
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace Kafka.Streams.Processor.Internals
{
    /**
     * Provides access to the {@link StreamsMetadata} in a KafkaStreams application. This can be used
     * to discover the locations of {@link org.apache.kafka.streams.processor.IStateStore}s
     * in a KafkaStreams application
     */
    public class StreamsMetadataState
    {

        public static HostInfo UNKNOWN_HOST = new HostInfo("unknown", -1);
        private InternalTopologyBuilder builder;
        private List<StreamsMetadata> allMetadata = new List<>();
        private HashSet<string> globalStores;
        private HostInfo thisHost;
        private Cluster clusterMetadata;
        private StreamsMetadata myMetadata;

        public StreamsMetadataState(InternalTopologyBuilder builder, HostInfo thisHost)
        {
            this.builder = builder;
            this.globalStores = builder.globalStateStores().keySet();
            this.thisHost = thisHost;
        }


        public string ToString()
        {
            return ToString("");
        }

        public string ToString(string indent)
        {
            StringBuilder builder = new StringBuilder();
            builder.Append(indent).Append("GlobalMetadata: ").Append(allMetadata).Append("\n");
            builder.Append(indent).Append("GlobalStores: ").Append(globalStores).Append("\n");
            builder.Append(indent).Append("My HostInfo: ").Append(thisHost).Append("\n");
            builder.Append(indent).Append(clusterMetadata).Append("\n");

            return builder.ToString();
        }

        /**
         * Find all of the {@link StreamsMetadata}s in a
         * {@link KafkaStreams application}
         *
         * @return all the {@link StreamsMetadata}s in a {@link KafkaStreams} application
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Collection<StreamsMetadata> getAllMetadata()
        {
            return allMetadata;
        }

        /**
         * Find all of the {@link StreamsMetadata}s for a given storeName
         *
         * @param storeName the storeName to find metadata for
         * @return A collection of {@link StreamsMetadata} that have the provided storeName
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Collection<StreamsMetadata> getAllMetadataForStore(string storeName)
        {
            storeName = storeName ?? throw new System.ArgumentNullException("storeName cannot be null", nameof(storeName));

            if (!isInitialized())
            {
                return Collections.emptyList();
            }

            if (globalStores.contains(storeName))
            {
                return allMetadata;
            }

            List<string> sourceTopics = builder.stateStoreNameToSourceTopics()[storeName);
            if (sourceTopics == null)
            {
                return Collections.emptyList();
            }

            List<StreamsMetadata> results = new List<>();
            foreach (StreamsMetadata metadata in allMetadata)
            {
                if (metadata.stateStoreNames().contains(storeName))
                {
                    results.Add(metadata);
                }
            }
            return results;
        }

        /**
         * Find the {@link StreamsMetadata}s for a given storeName and key. This method will use the
         * {@link DefaultStreamPartitioner} to locate the store. If a custom partitioner has been used
         * please use {@link StreamsMetadataState#getMetadataWithKey(string, object, StreamPartitioner)}
         *
         * Note: the key may not exist in the {@link org.apache.kafka.streams.processor.IStateStore},
         * this method provides a way of finding which {@link StreamsMetadata} it would exist on.
         *
         * @param storeName     Name of the store
         * @param key           Key to use
         * @param keySerializer Serializer for the key
         * @param           key type
         * @return The {@link StreamsMetadata} for the storeName and key or {@link StreamsMetadata#NOT_AVAILABLE}
         * if streams is (re-)initializing
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public StreamsMetadata getMetadataWithKey(string storeName,
                                                                   K key,
                                                                   Serializer<K> keySerializer)
        {
            keySerializer = keySerializer ?? throw new System.ArgumentNullException("keySerializer can't be null", nameof(keySerializer));
            storeName = storeName ?? throw new System.ArgumentNullException("storeName can't be null", nameof(storeName));
            key = key ?? throw new System.ArgumentNullException("key can't be null", nameof(key));

            if (!isInitialized())
            {
                return StreamsMetadata.NOT_AVAILABLE;
            }

            if (globalStores.contains(storeName))
            {
                // global stores are on every node. if we dont' have the host info
                // for this host then just pick the first metadata
                if (thisHost == UNKNOWN_HOST)
                {
                    return allMetadata[0];
                }
                return myMetadata;
            }

            SourceTopicsInfo sourceTopicsInfo = getSourceTopicsInfo(storeName);
            if (sourceTopicsInfo == null)
            {
                return null;
            }

            return getStreamsMetadataForKey(storeName,
                                            key,
                                            new DefaultStreamPartitioner<>(keySerializer, clusterMetadata),
                                            sourceTopicsInfo);
        }





        /**
         * Find the {@link StreamsMetadata}s for a given storeName and key.
         *
         * Note: the key may not exist in the {@link IStateStore},
         * this method provides a way of finding which {@link StreamsMetadata} it would exist on.
         *
         * @param storeName   Name of the store
         * @param key         Key to use
         * @param partitioner partitioner to use to find correct partition for key
         * @param         key type
         * @return The {@link StreamsMetadata} for the storeName and key or {@link StreamsMetadata#NOT_AVAILABLE}
         * if streams is (re-)initializing
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public StreamsMetadata getMetadataWithKey(string storeName,
                                                                   K key,
                                                                   StreamPartitioner<K, ?> partitioner)
        {
            storeName = storeName ?? throw new System.ArgumentNullException("storeName can't be null", nameof(storeName));
            key = key ?? throw new System.ArgumentNullException("key can't be null", nameof(key));
            partitioner = partitioner ?? throw new System.ArgumentNullException("partitioner can't be null", nameof(partitioner));

            if (!isInitialized())
            {
                return StreamsMetadata.NOT_AVAILABLE;
            }

            if (globalStores.contains(storeName))
            {
                // global stores are on every node. if we dont' have the host info
                // for this host then just pick the first metadata
                if (thisHost == UNKNOWN_HOST)
                {
                    return allMetadata[0];
                }
                return myMetadata;
            }

            SourceTopicsInfo sourceTopicsInfo = getSourceTopicsInfo(storeName);
            if (sourceTopicsInfo == null)
            {
                return null;
            }
            return getStreamsMetadataForKey(storeName, key, partitioner, sourceTopicsInfo);
        }

        /**
         * Respond to changes to the HostInfo -> TopicPartition mapping. Will rebuild the
         * metadata
         * @param currentState       the current mapping of {@link HostInfo} -> {@link TopicPartition}s
         * @param clusterMetadata    the current clusterMetadata {@link Cluster}
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        void onChange(Dictionary<HostInfo, HashSet<TopicPartition>> currentState, Cluster clusterMetadata)
        {
            this.clusterMetadata = clusterMetadata;
            rebuildMetadata(currentState);
        }

        private bool hasPartitionsForAnyTopics(List<string> topicNames, HashSet<TopicPartition> partitionForHost)
        {
            foreach (TopicPartition topicPartition in partitionForHost)
            {
                if (topicNames.contains(topicPartition.topic()))
                {
                    return true;
                }
            }
            return false;
        }

        private void rebuildMetadata(Dictionary<HostInfo, HashSet<TopicPartition>> currentState)
        {
            allMetadata.clear();
            if (currentState.isEmpty())
            {
                return;
            }
            Dictionary<string, List<string>> stores = builder.stateStoreNameToSourceTopics();
            foreach (Map.Entry<HostInfo, HashSet<TopicPartition>> entry in currentState.entrySet())
            {
                HostInfo key = entry.Key;
                HashSet<TopicPartition> partitionsForHost = new HashSet<>(entry.Value);
                HashSet<string> storesOnHost = new HashSet<>();
                foreach (Map.Entry<string, List<string>> storeTopicEntry in stores.entrySet())
                {
                    List<string> topicsForStore = storeTopicEntry.Value;
                    if (hasPartitionsForAnyTopics(topicsForStore, partitionsForHost))
                    {
                        storesOnHost.Add(storeTopicEntry.Key);
                    }
                }
                storesOnHost.AddAll(globalStores);
                StreamsMetadata metadata = new StreamsMetadata(key, storesOnHost, partitionsForHost);
                allMetadata.Add(metadata);
                if (key.Equals(thisHost))
                {
                    myMetadata = metadata;
                }
            }
        }

        private StreamsMetadata getStreamsMetadataForKey(string storeName,
                                                             K key,
                                                             StreamPartitioner<K, ?> partitioner,
                                                             SourceTopicsInfo sourceTopicsInfo)
        {

            int partition = partitioner.partition(sourceTopicsInfo.topicWithMostPartitions, key, null, sourceTopicsInfo.maxPartitions);
            HashSet<TopicPartition> matchingPartitions = new HashSet<>();
            foreach (string sourceTopic in sourceTopicsInfo.sourceTopics)
            {
                matchingPartitions.Add(new TopicPartition(sourceTopic, partition));
            }

            foreach (StreamsMetadata streamsMetadata in allMetadata)
            {
                HashSet<string> stateStoreNames = streamsMetadata.stateStoreNames();
                HashSet<TopicPartition> topicPartitions = new HashSet<>(streamsMetadata.topicPartitions());
                topicPartitions.retainAll(matchingPartitions);
                if (stateStoreNames.contains(storeName)
                        && !topicPartitions.isEmpty())
                {
                    return streamsMetadata;
                }
            }
            return null;
        }

        private SourceTopicsInfo getSourceTopicsInfo(string storeName)
        {
            List<string> sourceTopics = builder.stateStoreNameToSourceTopics()[storeName);
            if (sourceTopics == null || sourceTopics.isEmpty())
            {
                return null;
            }
            return new SourceTopicsInfo(sourceTopics);
        }

        private bool isInitialized()
        {
            return clusterMetadata != null && !clusterMetadata.topics().isEmpty();
        }

        private class SourceTopicsInfo
        {

            private List<string> sourceTopics;
            private int maxPartitions;
            private string topicWithMostPartitions;

            private SourceTopicsInfo(List<string> sourceTopics)
            {
                this.sourceTopics = sourceTopics;
                foreach (string topic in sourceTopics)
                {
                    List<PartitionInfo> partitions = clusterMetadata.partitionsForTopic(topic);
                    if (partitions.size() > maxPartitions)
                    {
                        maxPartitions = partitions.size();
                        topicWithMostPartitions = partitions[0).topic();
                    }
                }
            }
        }
    }
}