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
using Kafka.Common;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.IProcessor.Interfaces;
using Kafka.Streams.IProcessor.Internals;
using Kafka.Streams.State;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace Kafka.Streams.IProcessor.Internals
{
    public partial class InternalTopologyBuilder
    {
        private static ILogger log = new LoggerFactory().CreateLogger<InternalTopologyBuilder>();
        private static readonly Pattern EMPTY_ZERO_LENGTH_PATTERN = Pattern.compile("");
        private static string[] NO_PREDECESSORS = { };

        // node factories in a topological order
        private Dictionary<string, NodeFactory> nodeFactories = new Dictionary<string, NodeFactory>();

        // state factories
        private Dictionary<string, StateStoreFactory> stateFactories = new Dictionary<string, StateStoreFactory>();

        // built global state stores
        private Dictionary<string, IStoreBuilder> globalStateBuilders = new Dictionary<string, IStoreBuilder>();

        // built global state stores
        private Dictionary<string, IStateStore> globalStateStores = new Dictionary<string, IStateStore>();

        // all topics subscribed from source processors (without application-id prefix for internal topics)
        private HashSet<string> sourceTopicNames = new HashSet<string>();

        // all internal topics auto-created by the topology builder and used in source / sink processors
        private HashSet<string> internalTopicNames = new HashSet<string>();

        // groups of source processors that need to be copartitioned
        private List<HashSet<string>> copartitionSourceGroups = new List<HashSet<string>>();

        // map from source processor names to subscribed topics (without application-id prefix for internal topics)
        private Dictionary<string, List<string>> nodeToSourceTopics = new Dictionary<string, List<string>>();

        // map from source processor names to regex subscription patterns
        private Dictionary<string, Pattern> nodeToSourcePatterns = new Dictionary<string, Pattern>();

        // map from sink processor names to subscribed topic (without application-id prefix for internal topics)
        private Dictionary<string, string> nodeToSinkTopic = new Dictionary<string, string>();

        // map from topics to their matched regex patterns, this is to ensure one topic is passed through on source node
        // even if it can be matched by multiple regex patterns
        private Dictionary<string, Pattern> topicToPatterns = new Dictionary<string, Pattern>();

        // map from state store names to all the topics subscribed from source processors that
        // are connected to these state stores
        private Dictionary<string, HashSet<string>> stateStoreNameToSourceTopics = new Dictionary<string, HashSet<string>>();

        // map from state store names to all the regex subscribed topics from source processors that
        // are connected to these state stores
        private Dictionary<string, HashSet<Pattern>> stateStoreNameToSourceRegex = new Dictionary<string, HashSet<Pattern>>();

        // map from state store names to this state store's corresponding changelog topic if possible
        private Dictionary<string, string> storeToChangelogTopic = new Dictionary<string, string>();

        // all global topics
        private HashSet<string> globalTopics = new HashSet<string>();

        private HashSet<string> earliestResetTopics = new HashSet<string>();

        private HashSet<string> latestResetTopics = new HashSet<string>();

        private HashSet<Pattern> earliestResetPatterns = new HashSet<Pattern>();

        private HashSet<Pattern> latestResetPatterns = new HashSet<Pattern>();

        private QuickUnion<string> nodeGrouper = new QuickUnion<string>();

        private SubscriptionUpdates subscriptionUpdates = new SubscriptionUpdates();

        private string applicationId = null;

        private Pattern topicPattern = null;

        private Dictionary<int, HashSet<string>> nodeGroups = null;

        // public for testing only
        [MethodImpl(MethodImplOptions.Synchronized)]
        public InternalTopologyBuilder setApplicationId(string applicationId)
        {
            applicationId = applicationId ?? throw new System.ArgumentNullException("applicationId can't be null", nameof(applicationId));
            this.applicationId = applicationId;

            return this;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public InternalTopologyBuilder rewriteTopology(StreamsConfig config)
        {
            config = config ?? throw new System.ArgumentNullException("config can't be null", nameof(config));

            // set application id
            setApplicationId(config.getString(StreamsConfig.APPLICATION_ID_CONFIG));

            // maybe strip out caching layers
            if (config.getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG) == 0L)
            {
                foreach (var storeFactory in stateFactories.Values)
                {
                    storeFactory.builder.withCachingDisabled();
                }

                foreach (var storeBuilder in globalStateBuilders.Values)
                {
                    storeBuilder.withCachingDisabled();
                }
            }

            // build global state stores
            foreach (var storeBuilder in globalStateBuilders.Values)
            {
                globalStateStores.Add(storeBuilder.name, storeBuilder.build());
            }

            return this;
        }

        public void addSource<K, V>(
            AutoOffsetReset offsetReset,
            string name,
            ITimestampExtractor timestampExtractor,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valDeserializer,
            string[] topics)
        {
            if (topics.Length == 0)
            {
                throw new TopologyException("You must provide at least one topic");
            }
            name = name ?? throw new System.ArgumentNullException("name must not be null", nameof(name));
            if (nodeFactories.ContainsKey(name))
            {
                throw new TopologyException("IProcessor " + name + " is already.Added.");
            }

            foreach (string topic in topics)
            {
                if (topic == null)
                {
                    throw new System.ArgumentNullException("topic names cannot be null", nameof(topic));
                }

                validateTopicNotAlreadyRegistered(topic);
                maybeAddToResetList(earliestResetTopics, latestResetTopics, offsetReset, topic);
                sourceTopicNames.Add(topic);
            }

            nodeFactories.Add(name, new SourceNodeFactory<K, V>(name, topics, null, timestampExtractor, keyDeserializer, valDeserializer));
            nodeToSourceTopics.Add(name, topics.ToList());
            nodeGrouper.add(name);
            nodeGroups = null;
        }

        public void addSource<K, V>(
            AutoOffsetReset offsetReset,
            string name,
            ITimestampExtractor timestampExtractor,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valDeserializer,
            Pattern topicPattern)
        {
            topicPattern = topicPattern ?? throw new System.ArgumentNullException("topicPattern can't be null", nameof(topicPattern));
            name = name ?? throw new System.ArgumentNullException("name can't be null", nameof(name));

            if (nodeFactories.ContainsKey(name))
            {
                throw new TopologyException("IProcessor " + name + " is already.Added.");
            }

            foreach (string sourceTopicName in sourceTopicNames)
            {
                if (topicPattern.matcher(sourceTopicName).matches())
                {
                    throw new TopologyException("Pattern " + topicPattern + " will match a topic that has already been registered by another source.");
                }
            }

            foreach (Pattern otherPattern in earliestResetPatterns)
            {
                if (topicPattern.pattern().Contains(otherPattern.pattern()) || otherPattern.pattern().Contains(topicPattern.pattern()))
                {
                    throw new TopologyException("Pattern " + topicPattern + " will overlap with another pattern " + otherPattern + " already been registered by another source");
                }
            }

            foreach (Pattern otherPattern in latestResetPatterns)
            {
                if (topicPattern.pattern.Contains(otherPattern.pattern) || otherPattern.pattern.Contains(topicPattern.pattern))
                {
                    throw new TopologyException("Pattern " + topicPattern + " will overlap with another pattern " + otherPattern + " already been registered by another source");
                }
            }

            maybeAddToResetList(earliestResetPatterns, latestResetPatterns, offsetReset, topicPattern);

            nodeFactories.Add(name, new SourceNodeFactory(name, null, topicPattern, timestampExtractor, keyDeserializer, valDeserializer));
            nodeToSourcePatterns.Add(name, topicPattern);
            nodeGrouper.Add(name);
            nodeGroups = null;
        }

        public void addSink<K, V>(
            string name,
            string topic,
            ISerializer<K> keySerializer,
            ISerializer<V> valSerializer,
            IStreamPartitioner<K, V> partitioner,
            string[] predecessorNames)
        {
            name = name ?? throw new System.ArgumentNullException("name must not be null", nameof(name));
            topic = topic ?? throw new System.ArgumentNullException("topic must not be null", nameof(topic));
            predecessorNames = predecessorNames ?? throw new System.ArgumentNullException("predecessor names must not be null", nameof(predecessorNames));

            if (predecessorNames.Length == 0)
            {
                throw new TopologyException("Sink " + name + " must have at least one parent");
            }

            addSink(
                name,
                new StaticTopicNameExtractor<K, V>(topic),
                keySerializer,
                valSerializer,
                partitioner,
                predecessorNames);

            nodeToSinkTopic.Add(name, topic);
            nodeGroups = null;
        }

        public void addSink<K, V>(
            string name,
            ITopicNameExtractor<K, V> topicExtractor,
            ISerializer<K> keySerializer,
            ISerializer<V> valSerializer,
            IStreamPartitioner<K, V> partitioner,
            string[] predecessorNames)
        {
            name = name ?? throw new System.ArgumentNullException("name must not be null", nameof(name));
            topicExtractor = topicExtractor ?? throw new System.ArgumentNullException("topic extractor must not be null", nameof(topicExtractor));
            predecessorNames = predecessorNames ?? throw new System.ArgumentNullException("predecessor names must not be null", nameof(predecessorNames));

            if (nodeFactories.ContainsKey(name))
            {
                throw new TopologyException("IProcessor " + name + " is already.Added.");
            }
            if (predecessorNames.Length == 0)
            {
                throw new TopologyException("Sink " + name + " must have at least one parent");
            }

            foreach (string predecessor in predecessorNames)
            {
                Objects.requireNonNull(predecessor, "predecessor name can't be null");

                if (predecessor.Equals(name))
                {
                    throw new TopologyException("IProcessor " + name + " cannot be a predecessor of itself.");
                }

                if (!nodeFactories.ContainsKey(predecessor))
                {
                    throw new TopologyException("Predecessor processor " + predecessor + " is not.Added yet.");
                }

                if (nodeToSinkTopic.ContainsKey(predecessor))
                {
                    throw new TopologyException("Sink " + predecessor + " cannot be used a parent.");
                }
            }

            nodeFactories.Add(name, new SinkNodeFactory<K, V>(name, predecessorNames, topicExtractor, keySerializer, valSerializer, partitioner));
            nodeGrouper.add(name);
            nodeGrouper.unite(name, predecessorNames);
            nodeGroups = null;
        }

        public void addProcessor<K, V>(
            string name,
            IProcessorSupplier<K, V> supplier,
            string predecessorNames)
        {
            addProcessor(name, supplier, new[] { predecessorNames });
        }

        public void addProcessor<K, V>(
            string name,
            IProcessorSupplier<K, V> supplier,
            string[] predecessorNames)
        {
            name = name ?? throw new ArgumentNullException("name must not be null", nameof(name));
            supplier = supplier ?? throw new ArgumentNullException("supplier must not be null", nameof(supplier));
            predecessorNames = predecessorNames ?? throw new ArgumentNullException("predecessor names must not be null", nameof(predecessorNames));

            if (nodeFactories.ContainsKey(name))
            {
                throw new TopologyException("IProcessor " + name + " is already.Added.");
            }

            if (predecessorNames.Length == 0)
            {
                throw new TopologyException("IProcessor " + name + " must have at least one parent");
            }

            foreach (string predecessor in predecessorNames)
            {
                if (predecessor == null)
                {
                    throw new ArgumentNullException("predecessor name must not be null", nameof(predecessor));
                }

                if (predecessor.Equals(name))
                {
                    throw new TopologyException("IProcessor " + name + " cannot be a predecessor of itself.");
                }
                if (!nodeFactories.ContainsKey(predecessor))
                {
                    throw new TopologyException("Predecessor processor " + predecessor + " is not.Added yet for " + name);
                }
            }

            nodeFactories.Add(name, new ProcessorNodeFactory<K, V>(name, predecessorNames, supplier));
            nodeGrouper.Add(name);
            nodeGrouper.unite(name, predecessorNames);
            nodeGroups = null;
        }

        public void addStateStore<K, V, T>(
            IStoreBuilder<T> storeBuilder,
            string[] processorNames)
            where T : IStateStore
        {
            addStateStore(
                storeBuilder,
                false,
                processorNames);
        }

        public void addStateStore<T>(
            IStoreBuilder<T> storeBuilder,
            bool allowOverride,
            string[] processorNames)
            where T : IStateStore
        {
            storeBuilder = storeBuilder ?? throw new System.ArgumentNullException("storeBuilder can't be null", nameof(storeBuilder));
            if (!allowOverride && stateFactories.ContainsKey(storeBuilder.name))
            {
                throw new TopologyException("IStateStore " + storeBuilder.name + " is already.Added.");
            }

            stateFactories.Add(storeBuilder.name, new StateStoreFactory<T>(storeBuilder));

            if (processorNames != null)
            {
                foreach (string processorName in processorNames)
                {
                    if (processorName == null)
                    {
                        throw new System.ArgumentNullException("processor name must not be null", nameof(processorName));
                    }
                    connectProcessorAndStateStore(processorName, storeBuilder.name);
                }
            }
            nodeGroups = null;
        }

        public void addGlobalStore<K, V, T>(
            IStoreBuilder<T> storeBuilder,
            string sourceName,
            ITimestampExtractor timestampExtractor,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valueDeserializer,
            string topic,
            string processorName,
            IProcessorSupplier<K, V> stateUpdateSupplier)
        {
            storeBuilder = storeBuilder ?? throw new System.ArgumentNullException("store builder must not be null", nameof(storeBuilder));
            validateGlobalStoreArguments(sourceName,
                                         topic,
                                         processorName,
                                         stateUpdateSupplier,
                                         storeBuilder.name,
                                         storeBuilder.loggingEnabled);
            validateTopicNotAlreadyRegistered(topic);

            string[] topics = { topic };
            string[] predecessors = { sourceName };

            ProcessorNodeFactory nodeFactory = new ProcessorNodeFactory(processorName,
                predecessors,
                stateUpdateSupplier);

            globalTopics.Add(topic);
            nodeFactories.Add(sourceName, new SourceNodeFactory(sourceName,
                topics,
                null,
                timestampExtractor,
                keyDeserializer,
                valueDeserializer));
            nodeToSourceTopics.Add(sourceName, Arrays.asList(topics));
            nodeGrouper.add(sourceName);
            nodeFactory.addStateStore(storeBuilder.name);
            nodeFactories.Add(processorName, nodeFactory);
            nodeGrouper.add(processorName);
            nodeGrouper.unite(processorName, predecessors);
            globalStateBuilders.Add(storeBuilder.name, storeBuilder);
            connectSourceStoreAndTopic(storeBuilder.name, topic);
            nodeGroups = null;
        }

        private void validateTopicNotAlreadyRegistered(string topic)
        {
            if (sourceTopicNames.Contains(topic) || globalTopics.Contains(topic))
            {
                throw new TopologyException("Topic " + topic + " has already been registered by another source.");
            }

            foreach (Pattern pattern in nodeToSourcePatterns.Values)
            {
                if (pattern.matcher(topic).matches())
                {
                    throw new TopologyException("Topic " + topic + " matches a Pattern already registered by another source.");
                }
            }
        }

        public void connectProcessorAndStateStores(string processorName,
                                                         string[] stateStoreNames)
        {
            processorName = processorName ?? throw new System.ArgumentNullException("processorName can't be null", nameof(processorName));
            stateStoreNames = stateStoreNames ?? throw new System.ArgumentNullException("state store list must not be null", nameof(stateStoreNames));
            if (stateStoreNames.Length == 0)
            {
                throw new TopologyException("Must provide at least one state store name.");
            }
            foreach (string stateStoreName in stateStoreNames)
            {
                stateStoreName = stateStoreName ?? throw new System.ArgumentNullException("state store name must not be null", nameof(stateStoreName));
                connectProcessorAndStateStore(processorName, stateStoreName);
            }
            nodeGroups = null;
        }

        public void connectSourceStoreAndTopic(
            string sourceStoreName,
            string topic)
        {
            if (storeToChangelogTopic.ContainsKey(sourceStoreName))
            {
                throw new TopologyException("Source store " + sourceStoreName + " is already.Added.");
            }
            storeToChangelogTopic.Add(sourceStoreName, topic);
        }

        public void addInternalTopic(string topicName)
        {
            topicName = topicName ?? throw new System.ArgumentNullException("topicName can't be null", nameof(topicName));
            internalTopicNames.Add(topicName);
        }

        public void copartitionSources(HashSet<string> sourceNodes)
        {
            copartitionSourceGroups.Add(new HashSet<string>(sourceNodes));
        }

        private void validateGlobalStoreArguments(
            string sourceName,
            string topic,
            string processorName,
            IProcessorSupplier stateUpdateSupplier,
            string storeName,
            bool loggingEnabled)
        {
            sourceName = sourceName ?? throw new System.ArgumentNullException("sourceName must not be null", nameof(sourceName));
            topic = topic ?? throw new System.ArgumentNullException("topic must not be null", nameof(topic));
            stateUpdateSupplier = stateUpdateSupplier ?? throw new System.ArgumentNullException("supplier must not be null", nameof(stateUpdateSupplier));
            processorName = processorName ?? throw new System.ArgumentNullException("processorName must not be null", nameof(processorName));
            if (nodeFactories.ContainsKey(sourceName))
            {
                throw new TopologyException("IProcessor " + sourceName + " is already.Added.");
            }
            if (nodeFactories.ContainsKey(processorName))
            {
                throw new TopologyException("IProcessor " + processorName + " is already.Added.");
            }
            if (stateFactories.ContainsKey(storeName) || globalStateBuilders.ContainsKey(storeName))
            {
                throw new TopologyException("IStateStore " + storeName + " is already.Added.");
            }
            if (loggingEnabled)
            {
                throw new TopologyException("IStateStore " + storeName + " for global table must not have logging enabled.");
            }
            if (sourceName.Equals(processorName))
            {
                throw new TopologyException("sourceName and processorName must be different.");
            }
        }

        private void connectProcessorAndStateStore(
            string processorName,
            string stateStoreName)
        {
            if (globalStateBuilders.ContainsKey(stateStoreName))
            {
                throw new TopologyException("Global IStateStore " + stateStoreName +
                        " can be used by a IProcessor without being specified; it should not be explicitly passed.");
            }
            if (!stateFactories.ContainsKey(stateStoreName))
            {
                throw new TopologyException("IStateStore " + stateStoreName + " is not.Added yet.");
            }
            if (!nodeFactories.ContainsKey(processorName))
            {
                throw new TopologyException("IProcessor " + processorName + " is not.Added yet.");
            }

            StateStoreFactory stateStoreFactory = stateFactories[stateStoreName];
            IEnumerator<string> iter = stateStoreFactory.users().iterator();
            if (iter.hasNext())
            {
                string user = iter.next();
                nodeGrouper.unite(user, processorName);
            }
            stateStoreFactory.users().Add(processorName);

            NodeFactory nodeFactory = nodeFactories[processorName];
            if (nodeFactory is ProcessorNodeFactory)
            {
                ProcessorNodeFactory processorNodeFactory = (ProcessorNodeFactory)nodeFactory;
                processorNodeFactory.addStateStore(stateStoreName);
                connectStateStoreNameToSourceTopicsOrPattern(stateStoreName, processorNodeFactory);
            }
            else
            {

                throw new TopologyException("cannot connect a state store " + stateStoreName + " to a source node or a sink node.");
            }
        }

        private HashSet<SourceNodeFactory> findSourcesForProcessorPredecessors(string[] predecessors)
        {
            HashSet<SourceNodeFactory> sourceNodes = new HashSet<SourceNodeFactory>();
            foreach (string predecessor in predecessors)
            {
                NodeFactory nodeFactory = nodeFactories[predecessor];
                if (nodeFactory is SourceNodeFactory)
                {
                    sourceNodes.Add((SourceNodeFactory)nodeFactory);
                }
                else if (nodeFactory is ProcessorNodeFactory)
                {
                    sourceNodes.AddAll(findSourcesForProcessorPredecessors(((ProcessorNodeFactory)nodeFactory).predecessors));
                }
            }
            return sourceNodes;
        }

        private void connectStateStoreNameToSourceTopicsOrPattern(string stateStoreName,
                                                                  ProcessorNodeFactory processorNodeFactory)
        {
            // we should never update the mapping from state store names to source topics if the store name already exists
            // in the map; this scenario is possible, for example, that a state store underlying a source KTable is
            // connecting to a join operator whose source topic is not the original KTable's source topic but an internal repartition topic.

            if (stateStoreNameToSourceTopics.ContainsKey(stateStoreName)
                || stateStoreNameToSourceRegex.ContainsKey(stateStoreName))
            {
                return;
            }

            HashSet<string> sourceTopics = new HashSet<string>();
            HashSet<Pattern> sourcePatterns = new HashSet<Pattern>();
            HashSet<SourceNodeFactory> sourceNodesForPredecessor =
                findSourcesForProcessorPredecessors(processorNodeFactory.predecessors);

            foreach (var sourceNodeFactory in sourceNodesForPredecessor)
            {
                if (sourceNodeFactory.pattern != null)
                {
                    sourcePatterns.Add(sourceNodeFactory.pattern);
                }
                else
                {

                    sourceTopics.AddAll(sourceNodeFactory.topics);
                }
            }

            if (sourceTopics.Any())
            {
                stateStoreNameToSourceTopics.Add(stateStoreName,
                        sourceTopics);
            }

            if (sourcePatterns.Any())
            {
                stateStoreNameToSourceRegex.Add(
                    stateStoreName,
                    sourcePatterns);
            }

        }

        private void maybeAddToResetList<T>(
            ICollection<T> earliestResets,
            ICollection<T> latestResets,
            AutoOffsetReset offsetReset,
            T item)
        {
            switch (offsetReset)
            {
                case AutoOffsetReset.EARLIEST:
                    earliestResets.Add(item);
                    break;
                case AutoOffsetReset.LATEST:
                    latestResets.Add(item);
                    break;
                case AutoOffsetReset.UNKNOWN:
                default:
                    throw new TopologyException(string.Format("Unrecognized reset string.Format %s", offsetReset));
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Dictionary<int, HashSet<string>> nodeGroups()
        {
            if (_nodeGroups == null)
            {
                _nodeGroups = makeNodeGroups();
            }

            return _nodeGroups;
        }

        private Dictionary<int, HashSet<string>> makeNodeGroups()
        {
            Dictionary<int, HashSet<string>> nodeGroups = new Dictionary<int, HashSet<string>>();
            Dictionary<string, HashSet<string>> rootToNodeGroup = new Dictionary<string, HashSet<string>>();

            int nodeGroupId = 0;

            // Go through source nodes first. This makes the group id assignment easy to predict in tests
            HashSet<string> allSourceNodes = new HashSet<string>(nodeToSourceTopics.Keys);
            allSourceNodes.UnionWith(nodeToSourcePatterns.Keys);

            foreach (string nodeName in allSourceNodes)
            {
                nodeGroupId = putNodeGroupName(nodeName, nodeGroupId, nodeGroups, rootToNodeGroup);
            }

            // Go through non-source nodes
            foreach (string nodeName in nodeFactories.Keys)
            {
                if (!nodeToSourceTopics.ContainsKey(nodeName))
                {
                    nodeGroupId = putNodeGroupName(nodeName, nodeGroupId, nodeGroups, rootToNodeGroup);
                }
            }

            return nodeGroups;
        }

        private int putNodeGroupName(
            string nodeName,
            int nodeGroupId,
            Dictionary<int, HashSet<string>> nodeGroups,
            Dictionary<string, HashSet<string>> rootToNodeGroup)
        {
            int newNodeGroupId = nodeGroupId;
            string root = nodeGrouper.root(nodeName);
            HashSet<string> nodeGroup = rootToNodeGroup[root];
            if (nodeGroup == null)
            {
                nodeGroup = new HashSet<string>();
                rootToNodeGroup.Add(root, nodeGroup);
                nodeGroups.Add(newNodeGroupId++, nodeGroup);
            }
            nodeGroup.Add(nodeName);
            return newNodeGroupId;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public ProcessorTopology build()
        {
            return build((int?)null);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public ProcessorTopology build(int? topicGroupId)
        {
            HashSet<string> nodeGroup;
            if (topicGroupId != null)
            {
                nodeGroup = nodeGroups()[topicGroupId.Value];
            }
            else
            {

                // when topicGroupId is null, we build the full topology minus the global groups
                HashSet<string> globalNodeGroups = globalNodeGroups();
                var values = nodeGroups().Values;
                nodeGroup = new HashSet<string>();
                foreach (HashSet<string> value in values)
                {
                    nodeGroup.AddAll(value);
                }
                nodeGroup.removeAll(globalNodeGroups);
            }
            return build(nodeGroup);
        }

        /**
         * Builds the topology for any global state stores
         * @return ProcessorTopology
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public ProcessorTopology buildGlobalStateTopology()
        {
            applicationId = applicationId ?? throw new System.ArgumentNullException("topology has not completed optimization", nameof(applicationId));

            HashSet<string> globalGroups = globalNodeGroups();
            if (!globalGroups.Any())
            {
                return null;
            }

            return build(globalGroups);
        }

        private HashSet<string> globalNodeGroups()
        {
            HashSet<string> globalGroups = new HashSet<string>();
            foreach (KeyValuePair<int, HashSet<string>> nodeGroup in nodeGroups())
            {
                HashSet<string> nodes = nodeGroup.Value;
                foreach (string node in nodes)
                {
                    if (isGlobalSource(node))
                    {
                        globalGroups.AddAll(nodes);
                    }
                }
            }
            return globalGroups;
        }

        private ProcessorTopology build(HashSet<string> nodeGroup)
        {
            applicationId = applicationId ?? throw new System.ArgumentNullException("topology has not completed optimization", nameof(applicationId));

            Dictionary<string, ProcessorNode> processorMap = new LinkedHashMap<>();
            Dictionary<string, SourceNode> topicSourceMap = new Dictionary<>();
            Dictionary<string, SinkNode> topicSinkMap = new Dictionary<>();
            Dictionary<string, IStateStore> stateStoreMap = new LinkedHashMap<>();
            HashSet<string> repartitionTopics = new HashSet<>();

            // create processor nodes in a topological order ("nodeFactories" is already topologically sorted)
            // also make sure the state store map values following the insertion ordering
            foreach (NodeFactory factory in nodeFactories.Values)
            {
                if (nodeGroup == null || nodeGroup.Contains(factory.name))
                {
                    ProcessorNode node = factory.build();
                    processorMap.Add(node.name, node);

                    if (factory is ProcessorNodeFactory)
                    {
                        buildProcessorNode(
                            processorMap,
                            stateStoreMap,
                            (ProcessorNodeFactory)factory,
                            node);

                    }
                    else if (factory is SourceNodeFactory)
                    {
                        buildSourceNode(topicSourceMap,
                                        repartitionTopics,
                                        (SourceNodeFactory)factory,
                                        (SourceNode)node);

                    }
                    else if (factory is SinkNodeFactory)
                    {
                        buildSinkNode(processorMap,
                                      topicSinkMap,
                                      repartitionTopics,
                                      (SinkNodeFactory)factory,
                                      (SinkNode)node);
                    }
                    else
                    {

                        throw new TopologyException("Unknown definition: " + factory.GetType().getName());
                    }
                }
            }

            return new ProcessorTopology(new List<>(processorMap.Values),
                                         topicSourceMap,
                                         topicSinkMap,
                                         new List<>(stateStoreMap.Values),
                                         new List<>(globalStateStores.Values),
                                         storeToChangelogTopic,
                                         repartitionTopics);
        }


        private void buildSinkNode(
            Dictionary<string, ProcessorNode> processorMap,
            Dictionary<string, SinkNode> topicSinkMap,
            HashSet<string> repartitionTopics,
            SinkNodeFactory sinkNodeFactory,
            SinkNode node)
        {

            foreach (string predecessor in sinkNodeFactory.predecessors)
            {
                processorMap[predecessor].AddChild(node);
                if (sinkNodeFactory.topicExtractor is StaticTopicNameExtractor)
                {
                    string topic = ((StaticTopicNameExtractor)sinkNodeFactory.topicExtractor).topicName;

                    if (internalTopicNames.Contains(topic))
                    {
                        // prefix the internal topic name with the application id
                        string decoratedTopic = decorateTopic(topic);
                        topicSinkMap.Add(decoratedTopic, node);
                        repartitionTopics.Add(decoratedTopic);
                    }
                    else
                    {

                        topicSinkMap.Add(topic, node);
                    }

                }
            }
        }

        private void buildSourceNode(
            Dictionary<string, SourceNode> topicSourceMap,
            HashSet<string> repartitionTopics,
            SourceNodeFactory sourceNodeFactory,
            SourceNode node)
        {

            List<string> topics = (sourceNodeFactory.pattern != null) ?
                                        sourceNodeFactory.getTopics(subscriptionUpdates.getUpdates()) :
                                        sourceNodeFactory.topics;

            foreach (string topic in topics)
            {
                if (internalTopicNames.Contains(topic))
                {
                    // prefix the internal topic name with the application id
                    string decoratedTopic = decorateTopic(topic);
                    topicSourceMap.Add(decoratedTopic, node);
                    repartitionTopics.Add(decoratedTopic);
                }
                else
                {

                    topicSourceMap.Add(topic, node);
                }
            }
        }

        private void buildProcessorNode<K, V>(
            Dictionary<string, ProcessorNode<K, V>> processorMap,
            Dictionary<string, IStateStore> stateStoreMap,
            ProcessorNodeFactory<K, V> factory,
            ProcessorNode<K, V> node)
        {

            foreach (string predecessor in factory.predecessors)
            {
                ProcessorNode<K, V> predecessorNode = processorMap[predecessor];
                predecessorNode.AddChild(node);
            }
            foreach (string stateStoreName in factory.stateStoreNames)
            {
                if (!stateStoreMap.ContainsKey(stateStoreName))
                {
                    if (stateFactories.ContainsKey(stateStoreName))
                    {
                        StateStoreFactory stateStoreFactory = stateFactories[stateStoreName];

                        // remember the changelog topic if this state store is change-logging enabled
                        if (stateStoreFactory.loggingEnabled && !storeToChangelogTopic.ContainsKey(stateStoreName))
                        {
                            string changelogTopic = ProcessorStateManager.storeChangelogTopic(applicationId, stateStoreName);
                            storeToChangelogTopic.Add(stateStoreName, changelogTopic);
                        }
                        stateStoreMap.Add(stateStoreName, stateStoreFactory.build());
                    }
                    else
                    {

                        stateStoreMap.Add(stateStoreName, globalStateStores[stateStoreName]);
                    }
                }
            }
        }

        /**
         * Get any global {@link IStateStore}s that are part of the
         * topology
         * @return map containing all global {@link IStateStore}s
         */
        public Dictionary<string, IStateStore> globalStateStores()
        {
            applicationId = applicationId ?? throw new System.ArgumentNullException("topology has not completed optimization", nameof(applicationId));

            return globalStateStores
        }

        public HashSet<string> allStateStoreName()
        {
            applicationId = applicationId ?? throw new System.ArgumentNullException("topology has not completed optimization", nameof(applicationId));

            HashSet<string> allNames = new HashSet<>(stateFactories.Keys);
            allNames.AddAll(globalStateStores.Keys);
            return Collections.unmodifiableSet(allNames);
        }

        /**
         * Returns the map of topic groups keyed by the group id.
         * A topic group is a group of topics in the same task.
         *
         * @return groups of topic names
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Dictionary<int, TopicsInfo> topicGroups()
        {
            Dictionary<int, TopicsInfo> topicGroups = new LinkedHashMap<>();

            if (nodeGroups == null)
            {
                nodeGroups = makeNodeGroups();
            }

            foreach (KeyValuePair<int, HashSet<string>> entry in nodeGroups)
            {
                HashSet<string> sinkTopics = new HashSet<>();
                HashSet<string> sourceTopics = new HashSet<>();
                Dictionary<string, InternalTopicConfig> repartitionTopics = new Dictionary<>();
                Dictionary<string, InternalTopicConfig> stateChangelogTopics = new Dictionary<>();
                foreach (string node in entry.Value)
                {
                    // if the node is a source node,.Add to the source topics
                    List<string> topics = nodeToSourceTopics[node];
                    if (topics != null)
                    {
                        // if some of the topics are internal,.Add them to the internal topics
                        foreach (string topic in topics)
                        {
                            // skip global topic as they don't need partition assignment
                            if (globalTopics.Contains(topic))
                            {
                                continue;
                            }
                            if (internalTopicNames.Contains(topic))
                            {
                                // prefix the internal topic name with the application id
                                string internalTopic = decorateTopic(topic);
                                repartitionTopics.Add(
                                    internalTopic,
                                    new RepartitionTopicConfig(internalTopic, Collections.emptyMap()));
                                sourceTopics.Add(internalTopic);
                            }
                            else
                            {

                                sourceTopics.Add(topic);
                            }
                        }
                    }

                    // if the node is a sink node,.Add to the sink topics
                    string topic = nodeToSinkTopic[node];
                    if (topic != null)
                    {
                        if (internalTopicNames.Contains(topic))
                        {
                            // prefix the change log topic name with the application id
                            sinkTopics.Add(decorateTopic(topic));
                        }
                        else
                        {

                            sinkTopics.Add(topic);
                        }
                    }

                    // if the node is connected to a state store whose changelog topics are not predefined,
                    //.Add to the changelog topics
                    foreach (StateStoreFactory stateFactory in stateFactories.Values)
                    {
                        if (stateFactory.loggingEnabled && stateFactory.users().Contains(node))
                        {
                            string topicName = storeToChangelogTopic.ContainsKey(stateFactory.name) ?
                                    storeToChangelogTopic[stateFactory.name] :
                                    ProcessorStateManager.storeChangelogTopic(applicationId, stateFactory.name);
                            if (!stateChangelogTopics.ContainsKey(topicName))
                            {
                                InternalTopicConfig internalTopicConfig =
                                    createChangelogTopicConfig(stateFactory, topicName);
                                stateChangelogTopics.Add(topicName, internalTopicConfig);
                            }
                        }
                    }
                }
                if (!sourceTopics.isEmpty())
                {
                    topicGroups.Add(entry.Key, new TopicsInfo(
                            Collections.unmodifiableSet(sinkTopics),
                            Collections.unmodifiableSet(sourceTopics),
                            Collections.unmodifiableMap(repartitionTopics),
                            Collections.unmodifiableMap(stateChangelogTopics)));
                }
            }

            return Collections.unmodifiableMap(topicGroups);
        }

        private void setRegexMatchedTopicsToSourceNodes()
        {
            if (subscriptionUpdates.hasUpdates())
            {
                foreach (KeyValuePair<string, Pattern> stringPatternEntry in nodeToSourcePatterns)
                {
                    SourceNodeFactory sourceNode =
                        (SourceNodeFactory)nodeFactories[stringPatternEntry.Key];
                    //need to update nodeToSourceTopics with topics matched from given regex
                    nodeToSourceTopics.Add(
                        stringPatternEntry.Key,
                        sourceNode.getTopics(subscriptionUpdates.getUpdates()));
                    log.LogDebug("nodeToSourceTopics {}", nodeToSourceTopics);
                }
            }
        }

        private void setRegexMatchedTopicToStateStore()
        {
            if (subscriptionUpdates.hasUpdates())
            {
                foreach (KeyValuePair<string, HashSet<Pattern>> storePattern in stateStoreNameToSourceRegex)
                {
                    HashSet<string> updatedTopicsForStateStore = new HashSet<>();
                    foreach (string subscriptionUpdateTopic in subscriptionUpdates.getUpdates())
                    {
                        foreach (Pattern pattern in storePattern.Value)
                        {
                            if (pattern.matcher(subscriptionUpdateTopic).matches())
                            {
                                updatedTopicsForStateStore.Add(subscriptionUpdateTopic);
                            }
                        }
                    }
                    if (!updatedTopicsForStateStore.isEmpty())
                    {
                        List<string> storeTopics = stateStoreNameToSourceTopics[storePattern.Key];
                        if (storeTopics != null)
                        {
                            updatedTopicsForStateStore.AddAll(storeTopics);
                        }
                        stateStoreNameToSourceTopics.Add(
                            storePattern.Key,
                            Collections.unmodifiableSet(updatedTopicsForStateStore));
                    }
                }
            }
        }

        private InternalTopicConfig createChangelogTopicConfig(StateStoreFactory factory,
                                                               string name)
        {
            if (factory.isWindowStore())
            {
                WindowedChangelogTopicConfig config = new WindowedChangelogTopicConfig(name, factory.logConfig());
                config.setRetentionMs(factory.retentionPeriod());
                return config;
            }
            else
            {

                return new UnwindowedChangelogTopicConfig(name, factory.logConfig());
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Pattern earliestResetTopicsPattern()
        {
            return resetTopicsPattern(earliestResetTopics, earliestResetPatterns);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Pattern latestResetTopicsPattern()
        {
            return resetTopicsPattern(latestResetTopics, latestResetPatterns);
        }

        private Pattern resetTopicsPattern(HashSet<string> resetTopics,
                                           HashSet<Pattern> resetPatterns)
        {
            List<string> topics = maybeDecorateInternalSourceTopics(resetTopics);

            return buildPatternForOffsetResetTopics(topics, resetPatterns);
        }

        private static Pattern buildPatternForOffsetResetTopics(List<string> sourceTopics,
                                                                List<Pattern> sourcePatterns)
        {
            StringBuilder builder = new StringBuilder();

            foreach (string topic in sourceTopics)
            {
                builder.Append(topic).Append("|");
            }

            foreach (Pattern sourcePattern in sourcePatterns)
            {
                builder.Append(sourcePattern.pattern()).Append("|");
            }

            if (builder.Length > 0)
            {
                builder.setLength(builder.Length - 1);
                return Pattern.compile(builder.ToString());
            }

            return EMPTY_ZERO_LENGTH_PATTERN;
        }

        public Dictionary<string, List<string>> stateStoreNameToSourceTopics()
        {
            Dictionary<string, List<string>> results = new Dictionary<string, List<string>>();
            foreach (KeyValuePair<string, HashSet<string>> entry in stateStoreNameToSourceTopics)
            {
                results.Add(entry.Key, maybeDecorateInternalSourceTopics(entry.Value));
            }
            return results;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public List<HashSet<string>> copartitionGroups()
        {
            List<HashSet<string>> list = new List<>(copartitionSourceGroups.size());
            foreach (HashSet<string> nodeNames in copartitionSourceGroups)
            {
                HashSet<string> copartitionGroup = new HashSet<>();
                foreach (string node in nodeNames)
                {
                    List<string> topics = nodeToSourceTopics[node];
                    if (topics != null)
                    {
                        copartitionGroup.AddAll(maybeDecorateInternalSourceTopics(topics));
                    }
                }
                list.Add(Collections.unmodifiableSet(copartitionGroup));
            }
            return Collections.unmodifiableList(list);
        }

        private List<string> maybeDecorateInternalSourceTopics(List<string> sourceTopics)
        {
            List<string> decoratedTopics = new List<>();
            foreach (string topic in sourceTopics)
            {
                if (internalTopicNames.Contains(topic))
                {
                    decoratedTopics.Add(decorateTopic(topic));
                }
                else
                {

                    decoratedTopics.Add(topic);
                }
            }
            return decoratedTopics;
        }

        private string decorateTopic(string topic)
        {
            if (applicationId == null)
            {
                throw new TopologyException("there are internal topics and "
                        + "applicationId hasn't been set. Call "
                        + "setApplicationId first");
            }

            return applicationId + "-" + topic;
        }


        [MethodImpl(MethodImplOptions.Synchronized)]
        Pattern sourceTopicPattern()
        {
            if (topicPattern == null)
            {
                List<string> allSourceTopics = new List<>();
                if (!nodeToSourceTopics.isEmpty())
                {
                    foreach (List<string> topics in nodeToSourceTopics.Values)
                    {
                        allSourceTopics.AddAll(maybeDecorateInternalSourceTopics(topics));
                    }
                }
                Collections.sort(allSourceTopics);

                topicPattern = buildPatternForOffsetResetTopics(allSourceTopics, nodeToSourcePatterns.Values);
            }

            return topicPattern;
        }

        // package-private for testing only
        [MethodImpl(MethodImplOptions.Synchronized)]
        void updateSubscriptions(SubscriptionUpdates subscriptionUpdates,
                                              string logPrefix)
        {
            log.LogDebug("{}updating builder with {} topic(s) with possible matching regex subscription(s)",
                    logPrefix, subscriptionUpdates);
            this.subscriptionUpdates = subscriptionUpdates;
            setRegexMatchedTopicsToSourceNodes();
            setRegexMatchedTopicToStateStore();
        }

        private bool isGlobalSource(string nodeName)
        {
            NodeFactory nodeFactory = nodeFactories[nodeName];

            if (nodeFactory is SourceNodeFactory)
            {
                List<string> topics = ((SourceNodeFactory)nodeFactory).topics;
                return topics != null && topics.size() == 1 && globalTopics.Contains(topics[0]);
            }
            return false;
        }

        public TopologyDescription describe()
        {
            TopologyDescription description = new TopologyDescription();

            foreach (KeyValuePair<int, HashSet<string>> nodeGroup in makeNodeGroups())
            {

                HashSet<string> allNodesOfGroups = nodeGroup.Value;
                bool isNodeGroupOfGlobalStores = nodeGroupContainsGlobalSourceNode(allNodesOfGroups);

                if (!isNodeGroupOfGlobalStores)
                {
                    describeSubtopology(description, nodeGroup.Key, allNodesOfGroups);
                }
                else
                {

                    describeGlobalStore(description, allNodesOfGroups, nodeGroup.Key);
                }
            }

            return description;
        }

        private void describeGlobalStore(TopologyDescription description,
                                         HashSet<string> nodes,
                                         int id)
        {
            IEnumerator<string> it = nodes.iterator();
            while (it.hasNext())
            {
                string node = it.next();

                if (isGlobalSource(node))
                {
                    // we found a GlobalStore node group; those contain exactly two node: {sourceNode,processorNode}
                    it.Remove(); // Remove sourceNode from group
                    string processorNode = nodes.iterator().next(); // get remaining processorNode

                    description.AddGlobalStore(new GlobalStore(
                        node,
                        processorNode,
                        ((ProcessorNodeFactory)nodeFactories[processorNode]).stateStoreNames.iterator().next(),
                        nodeToSourceTopics[node][0],
                        id
                    ));
                    break;
                }
            }
        }

        private bool nodeGroupContainsGlobalSourceNode(HashSet<string> allNodesOfGroups)
        {
            foreach (string node in allNodesOfGroups)
            {
                if (isGlobalSource(node))
                {
                    return true;
                }
            }
            return false;
        }

        private static class NodeComparator : Comparator<INode>, Serializable
        {



            public int compare(INode node1,
                               INode node2)
            {
                if (node1.Equals(node2))
                {
                    return 0;
                }
                int size1 = ((AbstractNode)node1).size;
                int size2 = ((AbstractNode)node2).size;

                // it is possible that two nodes have the same sub-tree size (think two nodes connected via state stores)
                // in this case default to processor name string
                if (size1 != size2)
                {
                    return size2 - size1;
                }
                else
                {

                    return node1.name.CompareTo(node2.name);
                }
            }
        }

        private static NodeComparator NODE_COMPARATOR = new NodeComparator();

        private static void updateSize(AbstractNode node,
                                       int delta)
        {
            node.size += delta;

            foreach (INode predecessor in node.predecessors())
            {
                updateSize((AbstractNode)predecessor, delta);
            }
        }

        private void describeSubtopology(TopologyDescription description,
                                         int subtopologyId,
                                         HashSet<string> nodeNames)
        {

            Dictionary<string, AbstractNode> nodesByName = new Dictionary<>();

            //.Add all nodes
            foreach (string nodeName in nodeNames)
            {
                nodesByName.Add(nodeName, nodeFactories[nodeName].describe());
            }

            // connect each node to its predecessors and successors
            foreach (AbstractNode node in nodesByName.Values)
            {
                foreach (string predecessorName in nodeFactories[node.name].predecessors)
                {
                    AbstractNode predecessor = nodesByName[predecessorName];
                    node.addPredecessor(predecessor);
                    predecessor.addSuccessor(node);
                    updateSize(predecessor, node.size);
                }
            }

            description.AddSubtopology(new Subtopology(
                    subtopologyId,
                    new HashSet<>(nodesByName.Values)));
        }
    }
}

public ITopicNameExtractor topicNameExtractor()
{
    if (topicNameExtractor is StaticTopicNameExtractor)
    {
        return null;
    }
    else
    {

        return topicNameExtractor;
    }
}


public void addSuccessor(INode successor)
{
    throw new InvalidOperationException("Sinks don't have successors.");
}


public string ToString()
{
    if (topicNameExtractor is StaticTopicNameExtractor)
    {
        return "Sink: " + name + " (topic: " + Topic + ")\n      <-- " + nodeNames(predecessors);
    }
    return "Sink: " + name + " (extractor: " + topicNameExtractor + ")\n      <-- "
        + nodeNames(predecessors);
}


public bool Equals(object o)
{
    if (this == o)
    {
        return true;
    }
    if (o == null || GetType() != o.GetType())
    {
        return false;
    }

    Sink sink = (Sink)o;
    return name.Equals(sink.name)
        && topicNameExtractor.Equals(sink.topicNameExtractor)
        && predecessors.Equals(sink.predecessors);
}


public override int GetHashCode()
{
    // omit predecessors as it might change and alter the hash code
    return (name, topicNameExtractor).GetHashCode();
}
}

private static GlobalStoreComparator GLOBALSTORE_COMPARATOR = new GlobalStoreComparator();

private static SubtopologyComparator SUBTOPOLOGY_COMPARATOR = new SubtopologyComparator();

private static string nodeNames(HashSet<INode> nodes)
{
    StringBuilder sb = new StringBuilder();
    if (nodes.Any())
    {
        foreach (INode n in nodes)
        {
            sb.Append(n.name);
            sb.Append(", ");
        }
        sb.Remove(sb.Length - 1, 1);
        sb.Remove(sb.Length - 1, 1);
    }
    else
    {

        return "none";
    }
    return sb.ToString();
}

void updateSubscribedTopics(HashSet<string> topics,
                            string logPrefix)
{
    SubscriptionUpdates subscriptionUpdates = new SubscriptionUpdates();
    log.LogDebug("{}found {} topics possibly matching regex", logPrefix, topics);
    // update the topic groups with the returned subscription set for regex pattern subscriptions
    subscriptionUpdates.updateTopics(topics);
    updateSubscriptions(subscriptionUpdates, logPrefix);
}


// following functions are for test only
[MethodImpl(MethodImplOptions.Synchronized)]
public HashSet<string> getSourceTopicNames()
{
    return sourceTopicNames;
}

[MethodImpl(MethodImplOptions.Synchronized)]
public Dictionary<string, StateStoreFactory> getStateStores()
{
    return stateFactories;
}
    }
}