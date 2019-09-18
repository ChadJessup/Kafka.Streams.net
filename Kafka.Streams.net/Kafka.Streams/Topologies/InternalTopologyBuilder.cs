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
using Kafka.Streams.Processor;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals;
using Kafka.Streams.State;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;

namespace Kafka.Streams.Topologies
{
    public class InternalTopologyBuilder
    {
        private readonly ILogger logger;
        private readonly IServiceProvider services;

        private static readonly Regex EMPTY_ZERO_LENGTH_PATTERN = new Regex("", RegexOptions.Compiled);
        private static readonly string[] NO_PREDECESSORS = { };

        public InternalTopologyBuilder(
            ILogger<InternalTopologyBuilder> logger,
            IServiceProvider services)
        {
            this.logger = logger;
            this.services = services;
        }

        // node factories in a topological order
        private readonly Dictionary<string, NodeFactory> nodeFactories = new Dictionary<string, NodeFactory>();

        // state factories
        private readonly Dictionary<string, StateStoreFactory> stateFactories = new Dictionary<string, StateStoreFactory>();

        // built global state stores
        private readonly Dictionary<string, IStoreBuilder> globalStateBuilders = new Dictionary<string, IStoreBuilder>();

        // built global state stores
        private readonly Dictionary<string, IStateStore> _globalStateStores = new Dictionary<string, IStateStore>();

        // all topics subscribed from source processors (without application-id prefix for internal topics)
        private readonly HashSet<string> sourceTopicNames = new HashSet<string>();

        // all internal topics auto-created by the topology builder and used in source / sink processors
        private readonly HashSet<string> internalTopicNames = new HashSet<string>();

        // groups of source processors that need to be copartitioned
        private readonly List<HashSet<string>> copartitionSourceGroups = new List<HashSet<string>>();

        // map from source processor names to subscribed topics (without application-id prefix for internal topics)
        private readonly Dictionary<string, List<string>> nodeToSourceTopics = new Dictionary<string, List<string>>();

        // map from source processor names to regex subscription patterns
        private readonly Dictionary<string, Regex> nodeToSourcePatterns = new Dictionary<string, Regex>();

        // map from sink processor names to subscribed topic (without application-id prefix for internal topics)
        private readonly Dictionary<string, string> nodeToSinkTopic = new Dictionary<string, string>();

        // map from topics to their matched regex patterns, this is to ensure one topic is passed through on source node
        // even if it can be matched by multiple regex patterns
        private readonly Dictionary<string, Regex> topicToPatterns = new Dictionary<string, Regex>();

        // map from state store names to all the topics subscribed from source processors that
        // are connected to these state stores
        private readonly Dictionary<string, HashSet<string>> _stateStoreNameToSourceTopics = new Dictionary<string, HashSet<string>>();

        // map from state store names to all the regex subscribed topics from source processors that
        // are connected to these state stores
        private readonly Dictionary<string, HashSet<Regex>> stateStoreNameToSourceRegex = new Dictionary<string, HashSet<Regex>>();

        // map from state store names to this state store's corresponding changelog topic if possible
        private readonly Dictionary<string, string> storeToChangelogTopic = new Dictionary<string, string>();

        // all global topics
        private readonly HashSet<string> globalTopics = new HashSet<string>();

        private readonly HashSet<string> earliestResetTopics = new HashSet<string>();

        private readonly HashSet<string> latestResetTopics = new HashSet<string>();

        private readonly HashSet<Regex> earliestResetPatterns = new HashSet<Regex>();

        private readonly HashSet<Regex> latestResetPatterns = new HashSet<Regex>();

        private readonly QuickUnion<string> nodeGrouper = new QuickUnion<string>();

        public SubscriptionUpdates subscriptionUpdates { get; } = new SubscriptionUpdates();

        private string applicationId = null;

        private readonly Regex topicPattern = null;

        private Dictionary<int, HashSet<string>> _nodeGroups = null;

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
            setApplicationId(config.Get(StreamsConfigPropertyNames.ApplicationId));

            // maybe strip out caching layers
            if (config.getLong(StreamsConfigPropertyNames.CacheMaxBytesBuffering) == 0L)
            {
                foreach (var storeFactory in stateFactories.Values)
                {
                    //                  storeFactory.builder.withCachingDisabled();
                }

                foreach (var storeBuilder in globalStateBuilders.Values)
                {
                    //                    storeBuilder.withCachingDisabled();
                }
            }

            // build global state stores
            foreach (var storeBuilder in globalStateBuilders.Values)
            {
                //_globalStateStores.Add(storeBuilder.name, storeBuilder.build());
            }

            return this;
        }

        public void addSource<K, V>(
            AutoOffsetReset? offsetReset,
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

            // nodeFactories.Add(name, new SourceNodeFactory<K, V>(name, topics, null, timestampExtractor, keyDeserializer, valDeserializer));
            nodeToSourceTopics.Add(name, topics.ToList());
            nodeGrouper.add(name);
            _nodeGroups = null;
        }

        public void addSource<K, V>(
            AutoOffsetReset offsetReset,
            string name,
            ITimestampExtractor timestampExtractor,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valDeserializer,
            Regex topicPattern)
        {
            topicPattern = topicPattern ?? throw new System.ArgumentNullException("topicPattern can't be null", nameof(topicPattern));
            name = name ?? throw new System.ArgumentNullException("name can't be null", nameof(name));

            if (nodeFactories.ContainsKey(name))
            {
                throw new TopologyException("IProcessor " + name + " is already.Added.");
            }

            foreach (string sourceTopicName in sourceTopicNames)
            {
                if (topicPattern.IsMatch(sourceTopicName))
                {
                    throw new TopologyException("Regex " + topicPattern + " will match a topic that has already been registered by another source.");
                }
            }

            foreach (Regex otherPattern in earliestResetPatterns)
            {
                //if (topicPattern.pattern.Contains(otherPattern.pattern()) || otherPattern.pattern().Contains(topicPattern.pattern))
                //{
                //    throw new TopologyException("Regex " + topicPattern + " will overlap with another pattern " + otherPattern + " already been registered by another source");
                //}
            }

            foreach (Regex otherPattern in latestResetPatterns)
            {
                //if (topicPattern.pattern.Contains(otherPattern.pattern) || otherPattern.pattern.Contains(topicPattern.pattern))
                //{
                //    throw new TopologyException("Regex " + topicPattern + " will overlap with another pattern " + otherPattern + " already been registered by another source");
                //}
            }

            maybeAddToResetList(earliestResetPatterns, latestResetPatterns, offsetReset, topicPattern);

            //nodeFactories.Add(name, new SourceNodeFactory<K, V>(name, null, topicPattern, timestampExtractor, keyDeserializer, valDeserializer));
            nodeToSourcePatterns.Add(name, topicPattern);
            //nodeGrouper.Add(name);
            _nodeGroups = null;
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
                new StaticTopicNameExtractor(topic),
                keySerializer,
                valSerializer,
                partitioner,
                predecessorNames);

            nodeToSinkTopic.Add(name, topic);
            _nodeGroups = null;
        }

        public void addSink<K, V>(
            string name,
            ITopicNameExtractor topicExtractor,
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

            //nodeFactories.Add(name, new SinkNodeFactory<K, V>(name, predecessorNames, topicExtractor, keySerializer, valSerializer, partitioner));
            nodeGrouper.add(name);
            nodeGrouper.unite(name, predecessorNames);
            _nodeGroups = null;
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
            params string[] predecessorNames)
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

            //nodeFactories.Add(name, new ProcessorNodeFactory<K, V>(name, predecessorNames, supplier));
            //            nodeGrouper.Add(name);
            nodeGrouper.unite(name, predecessorNames);
            _nodeGroups = null;
        }

        public void addStateStore<K, V, T>(
            IStoreBuilder<T> storeBuilder,
            string[] processorNames)
            where T : IStateStore
        {
            addStateStore<K, V, T>(
                storeBuilder,
                false,
                processorNames);
        }

        public void addStateStore<K, V, T>(
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

                    connectProcessorAndStateStore<K, V>(processorName, storeBuilder.name);
                }
            }
            _nodeGroups = null;
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
            where T : IStateStore
        {
            storeBuilder = storeBuilder ?? throw new ArgumentNullException("store builder must not be null", nameof(storeBuilder));
            validateGlobalStoreArguments(
                sourceName,
                topic,
                processorName,
                stateUpdateSupplier,
                storeBuilder.name,
                storeBuilder.loggingEnabled);
            validateTopicNotAlreadyRegistered(topic);

            string[] topics = { topic };
            string[] predecessors = { sourceName };

            //ProcessorNodeFactory<K, V> nodeFactory = new ProcessorNodeFactory<K, V>(processorName,
            //    predecessors,
            //    stateUpdateSupplier);

            globalTopics.Add(topic);
            //nodeFactories.Add(sourceName, new SourceNodeFactory<K, V>(sourceName,
            //    topics,
            //    null,
            //    timestampExtractor,
            //    keyDeserializer,
            //    valueDeserializer));

            nodeToSourceTopics.Add(sourceName, topics.ToList());
            nodeGrouper.add(sourceName);
            //nodeFactory.addStateStore(storeBuilder.name);
            //nodeFactories.Add(processorName, nodeFactory);
            nodeGrouper.add(processorName);
            nodeGrouper.unite(processorName, predecessors);
            globalStateBuilders.Add(storeBuilder.name, storeBuilder);
            connectSourceStoreAndTopic(storeBuilder.name, topic);
            _nodeGroups = null;
        }

        private void validateTopicNotAlreadyRegistered(string topic)
        {
            if (sourceTopicNames.Contains(topic) || globalTopics.Contains(topic))
            {
                throw new TopologyException("Topic " + topic + " has already been registered by another source.");
            }

            foreach (Regex pattern in nodeToSourcePatterns.Values)
            {
                //if (pattern.matcher(topic).matches())
                //{
                //    throw new TopologyException("Topic " + topic + " matches a Regex already registered by another source.");
                //}
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
                //stateStoreName = stateStoreName ?? throw new System.ArgumentNullException("state store name must not be null", nameof(stateStoreName));
                //connectProcessorAndStateStore(processorName, stateStoreName);
            }

            _nodeGroups = null;
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

        private void validateGlobalStoreArguments<K, V>(
            string sourceName,
            string topic,
            string processorName,
            IProcessorSupplier<K, V> stateUpdateSupplier,
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

        private void connectProcessorAndStateStore<K, V>(
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
            //IEnumerator<string> iter = stateStoreFactory.users().iterator();
            //if (iter.hasNext())
            //{
            //  //  string user = iter.next();
            //  //  nodeGrouper.unite(user, processorName);
            //}

            //stateStoreFactory.users().Add(processorName);

            NodeFactory nodeFactory = nodeFactories[processorName];
            //if (nodeFactory is ProcessorNodeFactory<K, V>)
            //{
            //    ProcessorNodeFactory<K, V> processorNodeFactory = (ProcessorNodeFactory<K, V>)nodeFactory;
            //    processorNodeFactory.addStateStore(stateStoreName);
            //    connectStateStoreNameToSourceTopicsOrPattern(stateStoreName, processorNodeFactory);
            //}
            //else
            //{
            //    throw new TopologyException("cannot connect a state store " + stateStoreName + " to a source node or a sink node.");
            //}
        }

        //private HashSet<SourceNodeFactory<K, V>> findSourcesForProcessorPredecessors<K, V>(string[] predecessors)
        //{
        //    HashSet<SourceNodeFactory<K, V>> sourceNodes = new HashSet<SourceNodeFactory<K, V>>();
        //    foreach (string predecessor in predecessors)
        //    {
        //        NodeFactory nodeFactory = nodeFactories[predecessor];
        //        if (nodeFactory is SourceNodeFactory<K, V>)
        //        {
        //            sourceNodes.Add((SourceNodeFactory<K, V>)nodeFactory);
        //        }
        //        else if (nodeFactory is ProcessorNodeFactory<K, V>)
        //        {
        //            //        sourceNodes.AddAll(findSourcesForProcessorPredecessors(((ProcessorNodeFactory)nodeFactory).predecessors));
        //        }
        //    }
        //    return sourceNodes;
        //}

        //private void connectStateStoreNameToSourceTopicsOrPattern<K, V>(
        //    string stateStoreName,
        //    ProcessorNodeFactory<K, V> processorNodeFactory)
        //{
        //    // we should never update the mapping from state store names to source topics if the store name already exists
        //    // in the map; this scenario is possible, for example, that a state store underlying a source KTable is
        //    // connecting to a join operator whose source topic is not the original KTable's source topic but an internal repartition topic.
        //    if (_stateStoreNameToSourceTopics.ContainsKey(stateStoreName)
        //        || stateStoreNameToSourceRegex.ContainsKey(stateStoreName))
        //    {
        //        return;
        //    }

        //    HashSet<string> sourceTopics = new HashSet<string>();
        //    HashSet<Regex> sourcePatterns = new HashSet<Regex>();
        //    //HashSet<SourceNodeFactory<K, V>> sourceNodesForPredecessor =
        //    //    findSourcesForProcessorPredecessors<K, V>(processorNodeFactory.predecessors);

        //    foreach (var sourceNodeFactory in sourceNodesForPredecessor)
        //    {
        //        if (sourceNodeFactory.pattern != null)
        //        {
        //            sourcePatterns.Add(sourceNodeFactory.pattern);
        //        }
        //        else
        //        {
        //            //sourceTopics.AddAll(sourceNodeFactory.topics);
        //        }
        //    }

        //    if (sourceTopics.Any())
        //    {
        //        _stateStoreNameToSourceTopics.Add(stateStoreName,
        //                sourceTopics);
        //    }

        //    if (sourcePatterns.Any())
        //    {
        //        stateStoreNameToSourceRegex.Add(
        //            stateStoreName,
        //            sourcePatterns);
        //    }

        //}

        private void maybeAddToResetList<T>(
            ICollection<T> earliestResets,
            ICollection<T> latestResets,
            AutoOffsetReset? offsetReset,
            T item)
        {
            switch (offsetReset)
            {
                case AutoOffsetReset.Earliest:
                    earliestResets.Add(item);
                    break;
                case AutoOffsetReset.Latest:
                    latestResets.Add(item);
                    break;
                case AutoOffsetReset.Error:
                default:
                    throw new TopologyException(string.Format("Unrecognized reset string.Format %s", offsetReset));
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Dictionary<int, HashSet<string>> GetNodeGroups()
        {
            if (_nodeGroups == null)
            {
                _nodeGroups = MakeNodeGroups();
            }

            return _nodeGroups;
        }

        private Dictionary<int, HashSet<string>> MakeNodeGroups()
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
                nodeGroup = GetNodeGroups()[topicGroupId.Value];
            }
            else
            {
                // when topicGroupId is null, we build the full topology minus the global groups
                HashSet<string> globalNodeGroups = GetGlobalNodeGroups();
                var values = GetNodeGroups().Values;
                nodeGroup = new HashSet<string>();

                foreach (HashSet<string> value in values)
                {
                    nodeGroup.UnionWith(value);
                }

                nodeGroup = new HashSet<string>(nodeGroup.Except(globalNodeGroups));
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
            applicationId = applicationId ?? throw new ArgumentNullException("topology has not completed optimization", nameof(applicationId));

            HashSet<string> globalGroups = GetGlobalNodeGroups();
            if (!globalGroups.Any())
            {
                return null;
            }

            return build(globalGroups);
        }

        private HashSet<string> GetGlobalNodeGroups()
        {
            HashSet<string> globalGroups = new HashSet<string>();
            foreach (KeyValuePair<int, HashSet<string>> nodeGroup in GetNodeGroups())
            {
                HashSet<string> nodes = nodeGroup.Value;
                foreach (string node in nodes)
                {
                    //if (isGlobalSource(node))
                    //{
                    //    globalGroups.AddAll(nodes);
                    //}
                }
            }
            return globalGroups;
        }

        private ProcessorTopology build(HashSet<string> nodeGroup)
        {
            applicationId = applicationId ?? throw new ArgumentNullException("topology has not completed optimization", nameof(applicationId));

            Dictionary<string, ProcessorNode> processorMap = new Dictionary<string, ProcessorNode>();
            Dictionary<string, SourceNode> topicSourceMap = new Dictionary<string, SourceNode>();
            Dictionary<string, ISinkNode> topicSinkMap = new Dictionary<string, ISinkNode>();
            Dictionary<string, IStateStore> stateStoreMap = new Dictionary<string, IStateStore>();
            HashSet<string> repartitionTopics = new HashSet<string>();

            // create processor nodes in a topological order ("nodeFactories" is already topologically sorted)
            // also make sure the state store map values following the insertion ordering
            foreach (NodeFactory factory in nodeFactories.Values)
            {
                if (nodeGroup == null || nodeGroup.Contains(factory.name))
                {
                    //                    ProcessorNode node = factory.build();
                    //                  processorMap.Add(node.name, node);

                    //if (factory is ProcessorNodeFactory<K, V>)
                    //{
                    //    //buildProcessorNode(
                    //    //    processorMap,
                    //    //    stateStoreMap,
                    //    //    (ProcessorNodeFactory<K, V>)factory,
                    //    //    node);
                    //}
                    //else if (factory is SourceNodeFactory<K, V>)
                    //{
                    //    //buildSourceNode(topicSourceMap,
                    //    //                repartitionTopics,
                    //    //                (SourceNodeFactory<K, V>)factory,
                    //    //                (SourceNode<K, V>)node);
                    //}
                    //else if (factory is SinkNodeFactory<K, V>)
                    //{
                    //    //buildSinkNode(processorMap,
                    //    //              topicSinkMap,
                    //    //              repartitionTopics,
                    //    //              (SinkNodeFactory<K, V>)factory,
                    //    //              (SinkNode<K, V>)node);
                    //}
                    //else
                    //{
                    //    throw new TopologyException("Unknown definition: " + factory.GetType().FullName);
                    //}
                }
            }

            return new ProcessorTopology(
                processorMap.Values,
                topicSourceMap,
                topicSinkMap,
                stateStoreMap.Values,
                _globalStateStores.Values,
                storeToChangelogTopic,
                repartitionTopics);
        }

        //private void buildSinkNode<K, V>(
        //    Dictionary<string, ProcessorNode<K, V>> processorMap,
        //    Dictionary<string, SinkNode<K, V>> topicSinkMap,
        //    HashSet<string> repartitionTopics,
        //    SinkNodeFactory<K, V> sinkNodeFactory,
        //    SinkNode<K, V> node)
        //{

        //    foreach (string predecessor in sinkNodeFactory.predecessors)
        //    {
        //        //processorMap[predecessor].AddChild(node);
        //        if (sinkNodeFactory.topicExtractor is StaticTopicNameExtractor<K, V>)
        //        {
        //            string topic = ((StaticTopicNameExtractor<K, V>)sinkNodeFactory.topicExtractor).topicName;

        //            if (internalTopicNames.Contains(topic))
        //            {
        //                // prefix the internal topic name with the application id
        //                string decoratedTopic = decorateTopic(topic);
        //                topicSinkMap.Add(decoratedTopic, node);
        //                repartitionTopics.Add(decoratedTopic);
        //            }
        //            else
        //            {

        //                topicSinkMap.Add(topic, node);
        //            }

        //        }
        //    }
        //}

        //private void buildSourceNode<K, V>(
        //    Dictionary<string, SourceNode<K, V>> topicSourceMap,
        //    HashSet<string> repartitionTopics,
        //    SourceNodeFactory<K, V> sourceNodeFactory,
        //    SourceNode<K, V> node)
        //{

        //    List<string> topics = (sourceNodeFactory.pattern != null)
        //        ? null //sourceNodeFactory.getTopics(subscriptionUpdates.getUpdates())
        //        : sourceNodeFactory.topics;

        //    foreach (string topic in topics)
        //    {
        //        if (internalTopicNames.Contains(topic))
        //        {
        //            // prefix the internal topic name with the application id
        //            string decoratedTopic = decorateTopic(topic);
        //            topicSourceMap.Add(decoratedTopic, node);
        //            repartitionTopics.Add(decoratedTopic);
        //        }
        //        else
        //        {

        //            topicSourceMap.Add(topic, node);
        //        }
        //    }
        //}

        //private void buildProcessorNode<K, V>(
        //    Dictionary<string, ProcessorNode<K, V>> processorMap,
        //    Dictionary<string, IStateStore> stateStoreMap,
        //    ProcessorNodeFactory<K, V> factory,
        //    ProcessorNode<K, V> node)
        //{

        //    foreach (string predecessor in factory.predecessors)
        //    {
        //        ProcessorNode<K, V> predecessorNode = processorMap[predecessor];
        //        //                predecessorNode.AddChild(node);
        //    }

        //    foreach (string stateStoreName in factory.stateStoreNames)
        //    {
        //        if (!stateStoreMap.ContainsKey(stateStoreName))
        //        {
        //            if (stateFactories.ContainsKey(stateStoreName))
        //            {
        //                StateStoreFactory stateStoreFactory = stateFactories[stateStoreName];

        //                // remember the changelog topic if this state store is change-logging enabled
        //                //if (/*stateStoreFactory.loggingEnabled && */!storeToChangelogTopic.ContainsKey(stateStoreName))
        //                //{
        //                //    string changelogTopic = ProcessorStateManager<K, V>.storeChangelogTopic(applicationId, stateStoreName);
        //                //    storeToChangelogTopic.Add(stateStoreName, changelogTopic);
        //                //}
        //                //stateStoreMap.Add(stateStoreName, stateStoreFactory.build());
        //            }
        //            else
        //            {
        //                //                        stateStoreMap.Add(stateStoreName, globalStateStores[stateStoreName]);
        //            }
        //        }
        //    }
        //}

        /**
         * Get any global {@link IStateStore}s that are part of the
         * topology
         * @return map containing all global {@link IStateStore}s
         */
        public Dictionary<string, IStateStore> globalStateStores()
        {
            applicationId = applicationId ?? throw new System.ArgumentNullException("topology has not completed optimization", nameof(applicationId));

            return _globalStateStores;
        }

        public HashSet<string> allStateStoreName()
        {
            applicationId = applicationId ?? throw new System.ArgumentNullException("topology has not completed optimization", nameof(applicationId));

            HashSet<string> allNames = new HashSet<string>(stateFactories.Keys);

            allNames.UnionWith(_globalStateStores.Keys);

            return allNames;
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
            Dictionary<int, TopicsInfo> topicGroups = new Dictionary<int, TopicsInfo>();

            if (_nodeGroups == null)
            {
                _nodeGroups = MakeNodeGroups();
            }

            foreach (KeyValuePair<int, HashSet<string>> entry in _nodeGroups)
            {
                HashSet<string> sinkTopics = new HashSet<string>();
                HashSet<string> sourceTopics = new HashSet<string>();
                Dictionary<string, InternalTopicConfig> repartitionTopics = new Dictionary<string, InternalTopicConfig>();
                Dictionary<string, InternalTopicConfig> stateChangelogTopics = new Dictionary<string, InternalTopicConfig>();

                foreach (string node in entry.Value)
                {
                    // if the node is a source node,.Add to the source topics
                    List<string> topics = nodeToSourceTopics[node];
                    if (topics != null)
                    {
                        // if some of the topics are internal,.Add them to the internal topics
                        foreach (string _topic in topics)
                        {
                            // skip global topic as they don't need partition assignment
                            if (globalTopics.Contains(_topic))
                            {
                                continue;
                            }
                            if (internalTopicNames.Contains(_topic))
                            {
                                // prefix the internal topic name with the application id
                                string internalTopic = decorateTopic(_topic);
                                //repartitionTopics.Add(
                                //    internalTopic,
                                //    new RepartitionTopicConfig(internalTopic, Collections.emptyMap()));
                                sourceTopics.Add(internalTopic);
                            }
                            else
                            {

                                sourceTopics.Add(_topic);
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
                        //if (stateFactory.loggingEnabled && stateFactory.users().Contains(node))
                        //{
                        //    string topicName = storeToChangelogTopic.ContainsKey(stateFactory.name)
                        //        ? storeToChangelogTopic[stateFactory.name]
                        //        : ProcessorStateManager.storeChangelogTopic(applicationId, stateFactory.name);

                        //    if (!stateChangelogTopics.ContainsKey(topicName))
                        //    {
                        //        InternalTopicConfig internalTopicConfig =
                        //            createChangelogTopicConfig(stateFactory, topicName);
                        //        stateChangelogTopics.Add(topicName, internalTopicConfig);
                        //    }
                        //}
                    }
                }

                if (sourceTopics.Any())
                {
                    topicGroups.Add(entry.Key, new TopicsInfo(
                            sinkTopics,
                            sourceTopics,
                            repartitionTopics,
                            stateChangelogTopics));
                }
            }

            return topicGroups;
        }

        private void setRegexMatchedTopicsToSourceNodes()
        {
            if (subscriptionUpdates.hasUpdates())
            {
                foreach (KeyValuePair<string, Regex> stringPatternEntry in nodeToSourcePatterns)
                {
                    //SourceNodeFactory sourceNode =
                    //    (SourceNodeFactory)nodeFactories[stringPatternEntry.Key];
                    ////need to update nodeToSourceTopics with topics matched from given regex
                    //nodeToSourceTopics.Add(
                    //    stringPatternEntry.Key,
                    //    sourceNode.getTopics(subscriptionUpdates.getUpdates()));
                    logger.LogDebug($"nodeToSourceTopics {nodeToSourceTopics}");
                }
            }
        }

        private void setRegexMatchedTopicToStateStore()
        {
            if (subscriptionUpdates.hasUpdates())
            {
                foreach (KeyValuePair<string, HashSet<Regex>> storePattern in stateStoreNameToSourceRegex)
                {
                    HashSet<string> updatedTopicsForStateStore = new HashSet<string>();
                    foreach (string subscriptionUpdateTopic in subscriptionUpdates.getUpdates())
                    {
                        foreach (Regex pattern in storePattern.Value)
                        {
                            if (pattern.IsMatch(subscriptionUpdateTopic))
                            {
                                updatedTopicsForStateStore.Add(subscriptionUpdateTopic);
                            }
                        }
                    }

                    if (updatedTopicsForStateStore.Any())
                    {
                        List<string> storeTopics = _stateStoreNameToSourceTopics[storePattern.Key].ToList();
                        if (storeTopics != null)
                        {
                            //                            updatedTopicsForStateStore.AddAll(storeTopics);
                        }

                        _stateStoreNameToSourceTopics.Add(
                            storePattern.Key,
                            updatedTopicsForStateStore);
                    }
                }
            }
        }

        private InternalTopicConfig createChangelogTopicConfig(
            StateStoreFactory factory,
            string name)
        {
            return null;
            //if (factory.isWindowStore())
            //{
            //    WindowedChangelogTopicConfig config = new WindowedChangelogTopicConfig(
            //        name,
            //        factory.logConfig());

            //    config.setRetentionMs(factory.retentionPeriod());
            //    return config;
            //}
            //else
            //{

            //    return new UnwindowedChangelogTopicConfig(name, factory.logConfig());
            //}
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Regex earliestResetTopicsPattern()
        {
            return resetTopicsPattern(earliestResetTopics, earliestResetPatterns);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Regex latestResetTopicsPattern()
        {
            return resetTopicsPattern(latestResetTopics, latestResetPatterns);
        }

        private Regex resetTopicsPattern(
            HashSet<string> resetTopics,
            HashSet<Regex> resetPatterns)
        {
            var resetTopicsList = new List<string>(resetTopics);
            List<string> topics = maybeDecorateInternalSourceTopics(resetTopicsList);

            return buildPatternForOffsetResetTopics(topics, new List<Regex>(resetPatterns));
        }

        private static Regex buildPatternForOffsetResetTopics(
            List<string> sourceTopics,
            List<Regex> sourcePatterns)
        {
            StringBuilder builder = new StringBuilder();

            foreach (string topic in sourceTopics)
            {
                builder.Append(topic).Append("|");
            }

            foreach (Regex sourcePattern in sourcePatterns)
            {
                //builder.Append(sourcePattern.pattern).Append("|");
            }

            if (builder.Length > 0)
            {
                builder.Length = builder.Length - 1;

                return new Regex(builder.ToString(), RegexOptions.Compiled);
            }

            return EMPTY_ZERO_LENGTH_PATTERN;
        }

        public Dictionary<string, List<string>> stateStoreNameToSourceTopics()
        {
            Dictionary<string, List<string>> results = new Dictionary<string, List<string>>();
            foreach (var entry in _stateStoreNameToSourceTopics)
            {
                results.Add(entry.Key, maybeDecorateInternalSourceTopics(new List<string>(entry.Value)));
            }

            return results;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public List<HashSet<string>> copartitionGroups()
        {
            List<HashSet<string>> list = new List<HashSet<string>>(copartitionSourceGroups.Count);
            foreach (HashSet<string> nodeNames in copartitionSourceGroups)
            {
                HashSet<string> copartitionGroup = new HashSet<string>();
                foreach (string node in nodeNames)
                {
                    List<string> topics = nodeToSourceTopics[node];
                    if (topics != null)
                    {
                        //copartitionGroup.AddAll(maybeDecorateInternalSourceTopics(topics));
                    }
                }

                list.Add(copartitionGroup);
            }

            return list;
        }

        private List<string> maybeDecorateInternalSourceTopics(List<string> sourceTopics)
        {
            List<string> decoratedTopics = new List<string>();
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
        public Regex sourceTopicPattern()
        {
            if (topicPattern == null)
            {
                List<string> allSourceTopics = new List<string>();
                if (nodeToSourceTopics.Any())
                {
                    foreach (List<string> topics in nodeToSourceTopics.Values)
                    {
                        allSourceTopics.AddRange(maybeDecorateInternalSourceTopics(topics));
                    }
                }

                allSourceTopics.Sort();

                //                topicPattern = buildPatternForOffsetResetTopics(allSourceTopics, nodeToSourcePatterns.Values);
            }

            return topicPattern;
        }

        // package-private for testing only
        [MethodImpl(MethodImplOptions.Synchronized)]
        void updateSubscriptions(
            SubscriptionUpdates subscriptionUpdates,
            string logPrefix)
        {
            logger.LogDebug("{}updating builder with {} topic(s) with possible matching regex subscription(s)",
                    logPrefix, subscriptionUpdates);
//            this.subscriptionUpdates = subscriptionUpdates;
            setRegexMatchedTopicsToSourceNodes();
            setRegexMatchedTopicToStateStore();
        }

        private bool isGlobalSource<K, V>(string nodeName)
        {
            NodeFactory nodeFactory = nodeFactories[nodeName];

            //if (nodeFactory is SourceNodeFactory<K, V>)
            //{
            //    List<string> topics = ((SourceNodeFactory<K, V>)nodeFactory).topics;

            //    return topics != null && topics.Count == 1 && globalTopics.Contains(topics[0]);
            //}
            return false;
        }

        public TopologyDescription describe()
        {
            TopologyDescription description = new TopologyDescription();

            foreach (KeyValuePair<int, HashSet<string>> nodeGroup in MakeNodeGroups())
            {

                HashSet<string> allNodesOfGroups = nodeGroup.Value;
                bool isNodeGroupOfGlobalStores = false;// nodeGroupContainsGlobalSourceNode(allNodesOfGroups);

                if (!isNodeGroupOfGlobalStores)
                {
                    describeSubtopology(description, nodeGroup.Key, allNodesOfGroups);
                }
                else
                {
                    //describeGlobalStore(description, allNodesOfGroups, nodeGroup.Key);
                }
            }

            return description;
        }

        private void describeGlobalStore<K, V>(
            TopologyDescription description,
            HashSet<string> nodes,
            int id)
        {
            IEnumerator<string> it = nodes.GetEnumerator();
            while (it.MoveNext())
            {
                string node = it.Current;

                if (isGlobalSource<K, V>(node))
                {
                    // we found a GlobalStore node group; those contain exactly two node: {sourceNode,processorNode}
                    //                    it.Remove(); // Remove sourceNode from group
                    string processorNode = nodes.GetEnumerator().Current; // get remaining processorNode

                    //description.addGlobalStore(new GlobalStore(
                    //    node,
                    //    processorNode,
                    //    ((ProcessorNodeFactory<K, V>)nodeFactories[processorNode]).stateStoreNames.GetEnumerator().Current,
                    //    nodeToSourceTopics[node][0],
                    //    id));

                    break;
                }
            }
        }

        private bool nodeGroupContainsGlobalSourceNode<K, V>(HashSet<string> allNodesOfGroups)
        {
            foreach (string node in allNodesOfGroups)
            {
                if (isGlobalSource<K, V>(node))
                {
                    return true;
                }
            }
            return false;
        }

        private static readonly NodeComparator NODE_COMPARATOR = new NodeComparator();

        private static void updateSize(AbstractNode node,
                                       int delta)
        {
            node.Size += delta;

            foreach (INode predecessor in node.Predecessors)
            {
                updateSize((AbstractNode)predecessor, delta);
            }
        }

        private void describeSubtopology(
            TopologyDescription description,
            int subtopologyId,
            HashSet<string> nodeNames)
        {

            Dictionary<string, AbstractNode> nodesByName = new Dictionary<string, AbstractNode>();

            // add all nodes
            foreach (string nodeName in nodeNames)
            {
                //nodesByName.Add(nodeName, nodeFactories[nodeName].describe());
            }

            // connect each node to its predecessors and successors
            foreach (AbstractNode node in nodesByName.Values)
            {
                foreach (string predecessorName in nodeFactories[node.Name].predecessors)
                {
                    AbstractNode predecessor = nodesByName[predecessorName];
                    node.AddPredecessor(predecessor);
                    predecessor.AddSuccessor(node);
                    updateSize(predecessor, node.Size);
                }
            }

            description.addSubtopology(new Subtopology(
                    subtopologyId,
                    new HashSet<INode>(nodesByName.Values)));
        }

        public void addSuccessor(INode successor)
        {
            throw new InvalidOperationException("Sinks don't have successors.");
        }

        private static readonly GlobalStoreComparator GLOBALSTORE_COMPARATOR = new GlobalStoreComparator();

        private static readonly SubtopologyComparator SUBTOPOLOGY_COMPARATOR = new SubtopologyComparator();

        internal static string GetNodeNames(HashSet<INode> nodes)
        {
            StringBuilder sb = new StringBuilder();
            if (nodes.Any())
            {
                foreach (INode n in nodes)
                {
                    sb.Append(n.Name);
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

        public void updateSubscribedTopics(
            HashSet<string> topics,
            string logPrefix)
        {
            SubscriptionUpdates subscriptionUpdates = new SubscriptionUpdates();
            logger.LogDebug($"{logPrefix}found {topics.Count} topics possibly matching regex");
            // update the topic groups with the returned subscription set for regex pattern subscriptions
            subscriptionUpdates.updateTopics(topics.ToList());

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