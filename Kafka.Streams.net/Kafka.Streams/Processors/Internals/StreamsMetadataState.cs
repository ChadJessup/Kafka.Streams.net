using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Configs;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.Topologies;

namespace Kafka.Streams.Processors.Internals
{
    /**
     * Provides access to the {@link StreamsMetadata} in a KafkaStreams application. This can be used
     * to discover the locations of {@link org.apache.kafka.streams.processor.IStateStore}s
     * in a KafkaStreams application
     */
    public class StreamsMetadataState
    {
        public static HostInfo UNKNOWN_HOST { get; } = new HostInfo("unknown", -1);
        private readonly InternalTopologyBuilder builder;
        private readonly List<StreamsMetadata> allMetadata = new List<StreamsMetadata>();
        private readonly HashSet<string> globalStores;
        private readonly HostInfo thisHost;
        private Cluster clusterMetadata;
        private StreamsMetadata myMetadata;

        public StreamsMetadataState(Topology topology, StreamsConfig config)
        {
            if (topology is null)
            {
                throw new ArgumentNullException(nameof(topology));
            }

            if (config is null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            this.builder = topology.internalTopologyBuilder;
            this.globalStores = new HashSet<string>(this.builder.GlobalStateStores().Keys);

            this.thisHost = this.ParseHostInfo(config.ApplicationServer);
        }

        public override string ToString()
            => this.ToString("");

        public string ToString(string indent)
        {
            var builder = new StringBuilder();

            builder.Append(indent).Append("GlobalMetadata: ").Append(this.allMetadata.ToJoinedString()).Append("\n");
            builder.Append(indent).Append("GlobalStores: ").Append(this.globalStores.ToJoinedString()).Append("\n");
            builder.Append(indent).Append("My HostInfo: ").Append(this.thisHost).Append("\n");
            builder.Append(indent).Append(this.clusterMetadata).Append("\n");

            return builder.ToString();
        }

        /**
         * Find All of the {@link StreamsMetadata}s in a
         * {@link KafkaStreams application}
         *
         * @return All the {@link StreamsMetadata}s in a {@link KafkaStreams} application
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public List<StreamsMetadata> GetAllMetadata()
        {
            return this.allMetadata;
        }

        /**
         * Find All of the {@link StreamsMetadata}s for a given storeName
         *
         * @param storeName the storeName to find metadata for
         * @return A collection of {@link StreamsMetadata} that have the provided storeName
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public List<StreamsMetadata> GetAllMetadataForStore(string storeName)
        {
            storeName = storeName ?? throw new ArgumentNullException(nameof(storeName));

            if (!this.IsInitialized())
            {
                return new List<StreamsMetadata>();
            }

            if (this.globalStores.Contains(storeName))
            {
                return this.allMetadata;
            }

            List<string> sourceTopics = this.builder.StateStoreNameToSourceTopics()[storeName];
            if (sourceTopics == null)
            {
                return new List<StreamsMetadata>();
            }

            var results = new List<StreamsMetadata>();
            foreach (StreamsMetadata metadata in this.allMetadata)
            {
                if (metadata.StateStoreNames.Contains(storeName))
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
        public StreamsMetadata? GetMetadataWithKey<K>(
            string storeName,
            K key,
            ISerializer<K> keySerializer)
        {
            keySerializer = keySerializer ?? throw new ArgumentNullException(nameof(keySerializer));
            storeName = storeName ?? throw new ArgumentNullException(nameof(storeName));
            key = key ?? throw new ArgumentNullException(nameof(key));

            if (!this.IsInitialized())
            {
                return StreamsMetadata.NOT_AVAILABLE;
            }

            if (this.globalStores.Contains(storeName))
            {
                // global stores are on every node. if we dont' have the host LogInformation
                // for this host then just pick the first metadata
                if (this.thisHost == UNKNOWN_HOST)
                {
                    return this.allMetadata[0];
                }

                return this.myMetadata;
            }

            SourceTopicsInfo? sourceTopicsInfo = this.GetSourceTopicsInfo(storeName);
            if (sourceTopicsInfo == null)
            {
                return null;
            }

            return this.GetStreamsMetadataForKey(
                storeName,
                key,
                new DefaultStreamPartitioner<K, object>(keySerializer, this.clusterMetadata),
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
        public StreamsMetadata? GetMetadataWithKey<K, V>(
            string storeName,
            K key,
            IStreamPartitioner<K, V> partitioner)
        {
            storeName = storeName ?? throw new ArgumentNullException(nameof(storeName));
            key = key ?? throw new ArgumentNullException(nameof(key));
            partitioner = partitioner ?? throw new ArgumentNullException(nameof(partitioner));

            if (!this.IsInitialized())
            {
                return StreamsMetadata.NOT_AVAILABLE;
            }

            if (this.globalStores.Contains(storeName))
            {
                // global stores are on every node. if we dont' have the host LogInformation
                // for this host then just pick the first metadata
                if (this.thisHost == UNKNOWN_HOST)
                {
                    return this.allMetadata[0];
                }

                return this.myMetadata;
            }

            SourceTopicsInfo? sourceTopicsInfo = this.GetSourceTopicsInfo(storeName);
            if (sourceTopicsInfo == null)
            {
                return null;
            }

            return this.GetStreamsMetadataForKey(storeName, key, partitioner, sourceTopicsInfo);
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
            if (currentState is null)
            {
                throw new ArgumentNullException(nameof(currentState));
            }

            this.clusterMetadata = clusterMetadata ?? throw new ArgumentNullException(nameof(clusterMetadata));
            this.RebuildMetadata(currentState);
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
            this.allMetadata.Clear();
            if (!currentState.Any())
            {
                return;
            }

            var stores = this.builder.StateStoreNameToSourceTopics();
            foreach (var entry in currentState)
            {
                HostInfo key = entry.Key;
                var partitionsForHost = new HashSet<TopicPartition>(entry.Value);
                var storesOnHost = new HashSet<string>();

                foreach (var storeTopicEntry in stores)
                {
                    List<string> topicsForStore = storeTopicEntry.Value;
                    if (this.HasPartitionsForAnyTopics(topicsForStore, partitionsForHost))
                    {
                        storesOnHost.Add(storeTopicEntry.Key);
                    }
                }

                storesOnHost.AddRange(this.globalStores);
                var metadata = new StreamsMetadata(key, storesOnHost, partitionsForHost);
                this.allMetadata.Add(metadata);

                if (key.Equals(this.thisHost))
                {
                    this.myMetadata = metadata;
                }
            }
        }

        private StreamsMetadata? GetStreamsMetadataForKey<K, V>(
            string storeName,
            K key,
            IStreamPartitioner<K, V> partitioner,
            SourceTopicsInfo sourceTopicsInfo)
        {
            var partition = partitioner.Partition(
                sourceTopicsInfo.topicWithMostPartitions,
                key,
                default,
                sourceTopicsInfo.maxPartitions);

            var matchingPartitions = new HashSet<TopicPartition>();
            foreach (var sourceTopic in sourceTopicsInfo.sourceTopics)
            {
                matchingPartitions.Add(new TopicPartition(sourceTopic, partition));
            }

            foreach (StreamsMetadata streamsMetadata in this.allMetadata)
            {
                HashSet<string> stateStoreNames = streamsMetadata.StateStoreNames;
                var topicPartitions = new HashSet<TopicPartition>(streamsMetadata.TopicPartitions);
                topicPartitions.IntersectWith(matchingPartitions);

                if (stateStoreNames.Contains(storeName) && topicPartitions.Any())
                {
                    return streamsMetadata;
                }
            }

            return null;
        }

        private SourceTopicsInfo? GetSourceTopicsInfo(string storeName)
        {
            List<string> sourceTopics = this.builder.StateStoreNameToSourceTopics()[storeName];
            if (sourceTopics == null || !sourceTopics.Any())
            {
                return null;
            }

            return new SourceTopicsInfo(sourceTopics);
        }

        private bool IsInitialized()
        {
            return this.clusterMetadata != null
                && this.clusterMetadata.topics().Any();
        }

        private HostInfo ParseHostInfo(string endPoint)
        {
            if (endPoint == null || !endPoint.Trim().Any())
            {
                return StreamsMetadataState.UNKNOWN_HOST;
            }

            var host = this.GetHost(endPoint);
            var port = this.GetPort(endPoint);

            if (host == null)
            {
                throw new Exception($"Error parsing host address {endPoint}. Expected string.Format host:port.");
            }

            return new HostInfo(host, port);
        }

        // This matches URIs of formats: host:port and protocol:\\host:port
        // IPv6 is supported with [ip] pattern
        private readonly Regex HOST_PORT_PATTERN = new Regex(".*?\\[?([0-9a-zA-Z\\-%._:]*)\\]?:([0-9]+)", RegexOptions.Compiled);

        /**
         * Extracts the hostname from a "host:port" address string.
         * @param address address string to parse
         * @return hostname or null if the given address is incorrect
         */
        private string GetHost(string address)
        {
            var matcher = this.HOST_PORT_PATTERN.Match(address);

            return matcher.Success
                ? matcher.Groups[1]?.Value ?? ""
                : "";
        }

        /**
         * Extracts the port number from a "host:port" address string.
         * @param address address string to parse
         * @return port number or null if the given address is incorrect
         */
        private int GetPort(string address)
        {
            var matcher = this.HOST_PORT_PATTERN.Match(address);

            return matcher.Success
                ? int.Parse(matcher.Groups[2].Value)
                : 0;
        }

        internal void OnChange(Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHost, Dictionary<HostInfo, HashSet<TopicPartition>> standbyPartitionsByHost, Cluster fakeCluster)
        {
            throw new NotImplementedException();
        }
    }
}
