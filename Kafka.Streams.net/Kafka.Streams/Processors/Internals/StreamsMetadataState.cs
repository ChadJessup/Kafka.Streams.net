using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.Topologies;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace Kafka.Streams.Processors.Internals
{
    /**
     * Provides access to the {@link StreamsMetadata} in a KafkaStreams application. This can be used
     * to discover the locations of {@link org.apache.kafka.streams.processor.IStateStore}s
     * in a KafkaStreams application
     */
    public class StreamsMetadataState
    {
        public static HostInfo UNKNOWN_HOST = new HostInfo("unknown", -1);
        private readonly InternalTopologyBuilder builder;
        private readonly List<StreamsMetadata> allMetadata = new List<StreamsMetadata>();
        private readonly HashSet<string> globalStores;
        private readonly HostInfo thisHost;
        private Cluster clusterMetadata;
        private StreamsMetadata myMetadata;

        public StreamsMetadataState(
            InternalTopologyBuilder builder,
            HostInfo thisHost)
        {
            this.builder = builder;
            this.globalStores = new HashSet<string>(builder.globalStateStores().Keys);
            this.thisHost = thisHost;
        }

        public override string ToString()
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
        public List<StreamsMetadata> GetAllMetadata()
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
        public List<StreamsMetadata> GetAllMetadataForStore(string storeName)
        {
            storeName = storeName ?? throw new ArgumentNullException(nameof(storeName));

            if (!IsInitialized())
            {
                return new List<StreamsMetadata>();
            }

            if (globalStores.Contains(storeName))
            {
                return allMetadata;
            }

            List<string> sourceTopics = builder.stateStoreNameToSourceTopics()[storeName];
            if (sourceTopics == null)
            {
                return new List<StreamsMetadata>();
            }

            List<StreamsMetadata> results = new List<StreamsMetadata>();
            foreach (StreamsMetadata metadata in allMetadata)
            {
                if (metadata.stateStoreNames.Contains(storeName))
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
        public StreamsMetadata GetMetadataWithKey<K>(
            string storeName,
            K key,
            ISerializer<K> keySerializer)
        {
            keySerializer = keySerializer ?? throw new ArgumentNullException(nameof(keySerializer));
            storeName = storeName ?? throw new ArgumentNullException(nameof(storeName));
            key = key ?? throw new ArgumentNullException(nameof(key));

            if (!IsInitialized())
            {
                return StreamsMetadata.NOT_AVAILABLE;
            }

            if (globalStores.Contains(storeName))
            {
                // global stores are on every node. if we dont' have the host LogInformation
                // for this host then just pick the first metadata
                if (thisHost == UNKNOWN_HOST)
                {
                    return allMetadata[0];
                }

                return myMetadata;
            }

            SourceTopicsInfo sourceTopicsInfo = GetSourceTopicsInfo(storeName);
            if (sourceTopicsInfo == null)
            {
                return null;
            }

            return GetStreamsMetadataForKey(
                storeName,
                key,
                new DefaultStreamPartitioner<K, object>(keySerializer, clusterMetadata),
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
        public StreamsMetadata GetMetadataWithKey<K, V>(
            string storeName,
            K key,
            IStreamPartitioner<K, V> partitioner)
        {
            storeName = storeName ?? throw new ArgumentNullException(nameof(storeName));
            key = key ?? throw new ArgumentNullException(nameof(key));
            partitioner = partitioner ?? throw new ArgumentNullException(nameof(partitioner));

            if (!IsInitialized())
            {
                return StreamsMetadata.NOT_AVAILABLE;
            }

            if (globalStores.Contains(storeName))
            {
                // global stores are on every node. if we dont' have the host LogInformation
                // for this host then just pick the first metadata
                if (thisHost == UNKNOWN_HOST)
                {
                    return allMetadata[0];
                }

                return myMetadata;
            }

            SourceTopicsInfo sourceTopicsInfo = GetSourceTopicsInfo(storeName);
            if (sourceTopicsInfo == null)
            {
                return null;
            }

            return GetStreamsMetadataForKey(storeName, key, partitioner, sourceTopicsInfo);
        }

        /**
         * Respond to changes to the HostInfo => TopicPartition mapping. Will rebuild the
         * metadata
         * @param currentState       the current mapping of {@link HostInfo} => {@link TopicPartition}s
         * @param clusterMetadata    the current clusterMetadata {@link Cluster}
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void OnChange(Dictionary<HostInfo, HashSet<TopicPartition>> currentState, Cluster clusterMetadata)
        {
            this.clusterMetadata = clusterMetadata;
            RebuildMetadata(currentState);
        }

        private bool HasPartitionsForAnyTopics(List<string> topicNames, HashSet<TopicPartition> partitionForHost)
        {
            foreach (TopicPartition topicPartition in partitionForHost)
            {
                if (topicNames.Contains(topicPartition.Topic))
                {
                    return true;
                }
            }

            return false;
        }

        private void RebuildMetadata(Dictionary<HostInfo, HashSet<TopicPartition>> currentState)
        {
            //allMetadata.clear();
            if (!currentState.Any())
            {
                return;
            }

            var stores = builder.stateStoreNameToSourceTopics();
            foreach (var entry in currentState)
            {
                HostInfo key = entry.Key;
                HashSet<TopicPartition> partitionsForHost = new HashSet<TopicPartition>(entry.Value);
                HashSet<string> storesOnHost = new HashSet<string>();

                foreach (var storeTopicEntry in stores)
                {
                    List<string> topicsForStore = storeTopicEntry.Value;
                    if (HasPartitionsForAnyTopics(topicsForStore, partitionsForHost))
                    {
                        storesOnHost.Add(storeTopicEntry.Key);
                    }
                }

                storesOnHost.AddRange(globalStores);
                StreamsMetadata metadata = new StreamsMetadata(key, storesOnHost, partitionsForHost);
                allMetadata.Add(metadata);

                if (key.Equals(thisHost))
                {
                    myMetadata = metadata;
                }
            }
        }

        private StreamsMetadata GetStreamsMetadataForKey<K, V>(
            string storeName,
            K key,
            IStreamPartitioner<K, V> partitioner,
            SourceTopicsInfo sourceTopicsInfo)
        {
            int partition = partitioner.partition(sourceTopicsInfo.topicWithMostPartitions, key, default, sourceTopicsInfo.maxPartitions);

            HashSet<TopicPartition> matchingPartitions = new HashSet<TopicPartition>();
            foreach (string sourceTopic in sourceTopicsInfo.sourceTopics)
            {
                matchingPartitions.Add(new TopicPartition(sourceTopic, partition));
            }

            foreach (StreamsMetadata streamsMetadata in allMetadata)
            {
                HashSet<string> stateStoreNames = streamsMetadata.stateStoreNames;
                HashSet<TopicPartition> topicPartitions = new HashSet<TopicPartition>(streamsMetadata.topicPartitions);
                topicPartitions.IntersectWith(matchingPartitions);

                if (stateStoreNames.Contains(storeName) && topicPartitions.Any())
                {
                    return streamsMetadata;
                }
            }

            return null;
        }

        private SourceTopicsInfo GetSourceTopicsInfo(string storeName)
        {
            List<string> sourceTopics = builder.stateStoreNameToSourceTopics()[storeName];
            if (sourceTopics == null || !sourceTopics.Any())
            {
                return null;
            }

            return new SourceTopicsInfo(sourceTopics);
        }

        private bool IsInitialized()
        {
            return clusterMetadata != null && clusterMetadata.topics().Any();
        }
    }
}