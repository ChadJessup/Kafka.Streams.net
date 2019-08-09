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
using Confluent.Kafka;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Common
{
    /**
     * An immutable representation of a subset of the nodes, topics, and partitions in the Kafka cluster.
     */
    public class Cluster
    {
        private bool isBootstrapConfigured;
        private List<Node> nodes;
        private HashSet<string> unauthorizedTopics;
        private HashSet<string> invalidTopics;
        private HashSet<string> internalTopics;
        private Node controller;
        private Dictionary<TopicPartition, PartitionMetadata> partitionsByTopicPartition;
        private Dictionary<string, List<PartitionMetadata>> partitionsByTopic;
        private Dictionary<string, List<PartitionMetadata>> availablePartitionsByTopic;
        private Dictionary<int, List<PartitionMetadata>> partitionsByNode;
        private Dictionary<int, Node> nodesById;
        private ClusterResource clusterResource;

        /**
         * Create a new cluster with the given id, nodes and partitions
         * @param nodes The nodes in the cluster
         * @param partitions Information about a subset of the topic-partitions this cluster hosts
         */
        public Cluster(string clusterId,
                       List<Node> nodes,
                       List<PartitionMetadata> partitions,
                       HashSet<string> unauthorizedTopics,
                       HashSet<string> internalTopics)
            : this(clusterId, false, nodes, partitions, unauthorizedTopics, new HashSet<string>(), internalTopics, null)
        {
        }

        /**
         * Create a new cluster with the given id, nodes and partitions
         * @param nodes The nodes in the cluster
         * @param partitions Information about a subset of the topic-partitions this cluster hosts
         */
        public Cluster(
            string clusterId,
            List<Node> nodes,
            List<PartitionMetadata> partitions,
            HashSet<string> unauthorizedTopics,
            HashSet<string> internalTopics,
            Node controller)
            : this(clusterId, false, nodes, partitions, unauthorizedTopics, new HashSet<string>(), internalTopics, controller)
        {
        }

        /**
         * Create a new cluster with the given id, nodes and partitions
         * @param nodes The nodes in the cluster
         * @param partitions Information about a subset of the topic-partitions this cluster hosts
         */
        public Cluster(string clusterId,
                       List<Node> nodes,
                       List<PartitionMetadata> partitions,
                       HashSet<string> unauthorizedTopics,
                       HashSet<string> invalidTopics,
                       HashSet<string> internalTopics,
                       Node controller)
            : this(clusterId, false, nodes, partitions, unauthorizedTopics, invalidTopics, internalTopics, controller)
        {
        }

        private Cluster(string clusterId,
                        bool isBootstrapConfigured,
                        List<Node> nodes,
                        List<PartitionMetadata> partitions,
                        HashSet<string> unauthorizedTopics,
                        HashSet<string> invalidTopics,
                        HashSet<string> internalTopics,
                        Node controller)
        {
            //this.isBootstrapConfigured = isBootstrapConfigured;
            //this.clusterResource = new ClusterResource(clusterId);
            //// make a randomized, unmodifiable copy of the nodes
            //List<Node> copy = new List<Node>(nodes);
            //Collections.shuffle(copy);
            //this.nodes = Collections.unmodifiableList(copy);

            //// Index the nodes for quick lookup
            //Dictionary<int, Node> tmpNodesById = new Dictionary<int, Node>();

            //foreach (var node in nodes)
            //{
            //    tmpNodesById.Add(node.id, node);
            //}

            //this.nodesById = Collections.unmodifiableMap(tmpNodesById);

            //// index the partition infos by topic, topic+partition, and node
            //Dictionary<TopicPartition, PartitionMetadata> tmpPartitionsByTopicPartition = new Dictionary<string, List<PartitionMetadata>>(partitions.Count);
            //Dictionary<string, List<PartitionMetadata>> tmpPartitionsByTopic = new Dictionary<string, List<PartitionMetadata>>();
            //Dictionary<string, List<PartitionMetadata>> tmpAvailablePartitionsByTopic = new Dictionary<string, List<PartitionMetadata>>();
            //Dictionary<int, List<PartitionMetadata>> tmpPartitionsByNode = new Dictionary<int, List<PartitionMetadata>>();

            //foreach (var p in partitions)
            //{
            //    tmpPartitionsByTopicPartition.Add(new TopicPartition(p.topic(), p.partition()), p);
            //    tmpPartitionsByTopic.Zip(p.topic(), Collections.singletonList(p), Utils::concatListsUnmodifiable);
            //    if (p.leader() != null)
            //    {
            //        tmpAvailablePartitionsByTopic.merge(p.topic(), Collections.singletonList(p), Utils::concatListsUnmodifiable);
            //        tmpPartitionsByNode.merge(p.leader().id(), Collections.singletonList(p), Utils::concatListsUnmodifiable);
            //    }
            //}
            //this.partitionsByTopicPartition = Collections.unmodifiableMap(tmpPartitionsByTopicPartition);
            //this.partitionsByTopic = Collections.unmodifiableMap(tmpPartitionsByTopic);
            //this.availablePartitionsByTopic = Collections.unmodifiableMap(tmpAvailablePartitionsByTopic);
            //this.partitionsByNode = Collections.unmodifiableMap(tmpPartitionsByNode);

            //this.unauthorizedTopics = Collections.unmodifiableSet(unauthorizedTopics);
            //this.invalidTopics = Collections.unmodifiableSet(invalidTopics);
            //this.internalTopics = Collections.unmodifiableSet(internalTopics);
            //this.controller = controller;
        }

        /**
         * Create an empty cluster instance with no nodes and no topic-partitions.
         */
        public static Cluster empty()
        {
            return new Cluster(
                null,
                new List<Node>(0),
                new List<PartitionMetadata>(0),
                new HashSet<string>(),
                new HashSet<string>(),
                null);
        }

        /**
         * Create a "bootstrap" cluster using the given list of host/ports
         * @param addresses The addresses
         * @return A cluster for these hosts/ports
         */
        //public static Cluster bootstrap(List<INetSocketAddress> addresses)
        //{
        //    List<Node> nodes = new List<Node>();
        //    int nodeId = -1;
        //    foreach (var address in addresses)
        //    {
        //        nodes.Add(new Node(nodeId--, address.getHostString(), address.getPort()));
        //    }

        //    return new Cluster(null, true, nodes, new List<>(0),
        //        Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null);
        //}

        /**
         * Return a copy of this cluster combined with `partitions`.
         */
        //public Cluster withPartitions(Dictionary<TopicPartition, PartitionMetadata> partitions)
        //{
        //    var combinedPartitions = new Dictionary<TopicPartition, PartitionMetadata>(this.partitionsByTopicPartition);
        //    combinedPartitions = combinedPartitions.Union(partitions).ToDictionary();

        //    return new Cluster(
        //        clusterResource.clusterId(),
        //        this.nodes,
        //        combinedPartitions.Values,
        //        new Dictionary<TopicPartition, PartitionMetadata>(this.unauthorizedTopics),
        //        new Dictionary<TopicPartition, PartitionMetadata>(this.invalidTopics),
        //        new Dictionary<TopicPartition, PartitionMetadata>(this.internalTopics), this.controller);
        //}

        /**
         * Get the node by the node id (or null if no such node exists)
         * @param id The id of the node
         * @return The node, or null if no such node exists
         */
        public Node nodeById(int id)
        {
            return this.nodesById[id];
        }

        /**
         * Get the node by node id if the replica for the given partition is online
         * @param partition
         * @param id
         * @return the node
         */
        //public Optional<Node> nodeIfOnline(TopicPartition partition, int id)
        //{
        //    Node node = nodeById(id);
        //    if (node != null && !Arrays.asList(partition(partition).offlineReplicas()).Contains(node))
        //    {
        //        return Optional.of(node);
        //    }
        //    else
        //    {
        //        return Optional.empty();
        //    }
        //}

        /**
         * Get the current leader for the given topic-partition
         * @param topicPartition The topic and partition we want to know the leader for
         * @return The node that is the leader for this topic-partition, or null if there is currently no leader
         */
        //public Node leaderFor(TopicPartition topicPartition)
        //{
        //    PartitionMetadata LogInformation = partitionsByTopicPartition.get(topicPartition);
        //    if (LogInformation == null)
        //        return null;
        //    else
        //        return LogInformation.leader();
        //}

        /**
         * Get the metadata for the specified partition
         * @param topicPartition The topic and partition to fetch LogInformation for
         * @return The metadata about the given topic and partition, or null if none is found
         */
        public PartitionMetadata partition(TopicPartition topicPartition)
        {
            return partitionsByTopicPartition[topicPartition];
        }

        /**
         * Get the list of partitions for this topic
         * @param topic The topic name
         * @return A list of partitions
         */
        //public List<PartitionMetadata> partitionsForTopic(string topic)
        //{
        //    return partitionsByTopic.getOrDefault(topic, Collections.emptyList());
        //}

        /**
         * Get the number of partitions for the given topic.
         * @param topic The topic to get the number of partitions for
         * @return The number of partitions or null if there is no corresponding metadata
         */
        public int partitionCountForTopic(string topic)
        {
            List<PartitionMetadata> partitions = this.partitionsByTopic[topic];
            return partitions == null
                ? 0
                : partitions.Count;
        }

        /**
         * Get the list of available partitions for this topic
         * @param topic The topic name
         * @return A list of partitions
         */
        //public List<PartitionMetadata> availablePartitionsForTopic(string topic)
        //{
        //    return availablePartitionsByTopic.getOrDefault(topic, Collections.emptyList());
        //}

        /**
         * Get the list of partitions whose leader is this node
         * @param nodeId The node id
         * @return A list of partitions
         */
        //public List<PartitionMetadata> partitionsForNode(int nodeId)
        //{
        //    return partitionsByNode.getOrDefault(nodeId, Collections.emptyList());
        //}

        /**
         * Get all topics.
         * @return a set of all topics
         */
        public HashSet<string> topics()
        {
            return new HashSet<string>(partitionsByTopic.Keys);
        }


        public override string ToString()
        {
            return "Cluster(id = " + clusterResource.clusterId() + ", nodes = " + this.nodes +
                ", partitions = " + this.partitionsByTopicPartition.Values + ", controller = " + controller + ")";
        }

        public override bool Equals(object o)
        {
            if (this == o) return true;
            if (o == null || this.GetType() != o.GetType()) return false;

            Cluster cluster = (Cluster)o;

            return isBootstrapConfigured == cluster.isBootstrapConfigured
                && Equals(nodes, cluster.nodes)
                && Equals(unauthorizedTopics, cluster.unauthorizedTopics)
                && Equals(invalidTopics, cluster.invalidTopics)
                && Equals(internalTopics, cluster.internalTopics)
                && Equals(controller, cluster.controller)
                && Equals(partitionsByTopicPartition, cluster.partitionsByTopicPartition)
                && Equals(clusterResource, cluster.clusterResource);
        }

        public override int GetHashCode()
        {
            return (isBootstrapConfigured, nodes, unauthorizedTopics, invalidTopics, internalTopics, controller,
                    partitionsByTopicPartition, clusterResource).GetHashCode();
        }
    }
}