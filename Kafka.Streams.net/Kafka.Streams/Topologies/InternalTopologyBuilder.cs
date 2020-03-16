using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Factories;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Microsoft.Extensions.Logging;
using NodaTime;
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
        private readonly ILogger<InternalTopologyBuilder> logger;
        private readonly IServiceProvider services;
        private StreamsConfig config;
        private static readonly Regex EMPTY_ZERO_LENGTH_PATTERN = new Regex("^$", RegexOptions.Compiled);
        private static readonly string[] NO_PREDECESSORS = Array.Empty<string>();
        private readonly IClock clock;

        public InternalTopologyBuilder(
            IClock clock,
            ILogger<InternalTopologyBuilder> logger,
            IServiceProvider services,
            StreamsConfig config)
        {
            this.clock = clock ?? throw new ArgumentNullException(nameof(clock));
            this.services = services ?? throw new ArgumentNullException(nameof(services));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.config = config ?? throw new ArgumentNullException(nameof(config));

            this.applicationId = this.config.ApplicationId;
        }

        // node factories in a topological order
        private readonly Dictionary<string, INodeFactory> nodeFactories = new Dictionary<string, INodeFactory>();

        // state factories
        private readonly Dictionary<string, IStateStoreFactory<IStateStore>> stateFactories = new Dictionary<string, IStateStoreFactory<IStateStore>>();

        // built global state stores
        private readonly Dictionary<string, IStoreBuilder<IStateStore>> globalStateBuilders = new Dictionary<string, IStoreBuilder<IStateStore>>();

        // built global state stores
        private readonly Dictionary<string, IStateStore> _globalStateStores = new Dictionary<string, IStateStore>();

        // all topics subscribed from source processors (without application-id prefix for internal topics)
        private readonly HashSet<string> sourceTopicNames = new HashSet<string>();

        // all internal topics auto-created by the topology builder and used in source / sink processors
        internal readonly HashSet<string> internalTopicNames = new HashSet<string>();

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

        public SubscriptionUpdates SubscriptionUpdates { get; internal set; } = new SubscriptionUpdates();

        public IClock Clock => this.clock;

        private string? applicationId = null;

        private Regex? topicPattern = null;

        private Dictionary<int, HashSet<string>>? _nodeGroups = null;

        // public for testing only
        [MethodImpl(MethodImplOptions.Synchronized)]
        public InternalTopologyBuilder SetApplicationId(string applicationId)
        {
            applicationId = applicationId ?? throw new ArgumentNullException(nameof(applicationId));
            this.applicationId = applicationId;

            return this;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public InternalTopologyBuilder RewriteTopology(StreamsConfig config)
        {
            this.config = config ?? throw new ArgumentNullException(nameof(config));

            // set application id
            SetApplicationId(config.ApplicationId);

            // maybe strip out caching layers
            if (config.CacheMaxBytesBuffering == 0L)
            {
                foreach (var storeFactory in stateFactories.Values)
                {
                    storeFactory.Builder.WithCachingDisabled();
                }

                foreach (var storeBuilder in globalStateBuilders.Values)
                {
                    storeBuilder.WithCachingDisabled();
                }
            }

            // build global state stores
            foreach (var storeBuilder in globalStateBuilders.Values)
            {
                _globalStateStores.Add(storeBuilder.name, storeBuilder.Build());
            }

            return this;
        }

        public void AddSource<K, V>(
            AutoOffsetReset? offsetReset,
            string name,
            ITimestampExtractor? timestampExtractor,
            IDeserializer<K>? keyDeserializer,
            IDeserializer<V>? valDeserializer,
            string topic)
        {
            this.AddSource(
                offsetReset,
                name,
                timestampExtractor,
                keyDeserializer,
                valDeserializer,
                new[] { topic });
        }

        public void AddSource<K, V>(
            AutoOffsetReset? offsetReset,
            string name,
            ITimestampExtractor? timestampExtractor,
            IDeserializer<K>? keyDeserializer,
            IDeserializer<V>? valDeserializer,
            string[] topics)
        {
            if (topics is null)
            {
                throw new ArgumentNullException(nameof(topics));
            }

            if (topics.Length == 0)
            {
                throw new TopologyException("You must provide at least one topic");
            }

            name = name ?? throw new ArgumentNullException(nameof(name));
            if (nodeFactories.ContainsKey(name))
            {
                throw new TopologyException("IProcessor " + name + " is already.Added.");
            }

            foreach (var topic in topics.Where(t => t != null))
            {
                ValidateTopicNotAlreadyRegistered(topic);
                maybeAddToResetList(earliestResetTopics, latestResetTopics, offsetReset, topic);
                sourceTopicNames.Add(topic);
            }

            nodeToSourceTopics.Add(name, topics.ToList());

            nodeFactories.Add(
                name,
                new SourceNodeFactory<K, V>(
                    this.Clock,
                    name,
                    topics,
                    null,
                    timestampExtractor,
                    topicToPatterns,
                    nodeToSourceTopics,
                    this,
                    keyDeserializer,
                    valDeserializer));

            nodeGrouper.Add(name);
            _nodeGroups = null;
        }

        public void AddSource<K, V>(
            AutoOffsetReset offsetReset,
            string name,
            ITimestampExtractor timestampExtractor,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valDeserializer,
            Regex topicPattern)
        {
            topicPattern = topicPattern ?? throw new ArgumentNullException(nameof(topicPattern));
            name = name ?? throw new ArgumentNullException(nameof(name));

            if (nodeFactories.ContainsKey(name))
            {
                throw new TopologyException("IProcessor " + name + " is already.Added.");
            }

            foreach (var sourceTopicName in sourceTopicNames)
            {
                if (topicPattern.IsMatch(sourceTopicName))
                {
                    throw new TopologyException("Regex " + topicPattern + " will match a topic that has already been registered by another source.");
                }
            }

            foreach (Regex otherPattern in earliestResetPatterns)
            {
                if (topicPattern.ToString().Contains(otherPattern.ToString()) || otherPattern.ToString().Contains(topicPattern.ToString()))
                {
                    throw new TopologyException($"Regex {topicPattern} will overlap with another pattern {otherPattern} already been registered by another source");
                }
            }

            foreach (Regex otherPattern in latestResetPatterns)
            {
                if (topicPattern.ToString().Contains(otherPattern.ToString()) || otherPattern.ToString().Contains(topicPattern.ToString()))
                {
                    throw new TopologyException($"Regex {topicPattern} will overlap with another pattern {otherPattern} already been registered by another source");
                }
            }

            maybeAddToResetList(earliestResetPatterns, latestResetPatterns, offsetReset, topicPattern);

            nodeFactories.Add(name, new SourceNodeFactory<K, V>(
                this.Clock,
                name,
                null,
                topicPattern,
                timestampExtractor,
                null,
                null,
                this,
                keyDeserializer,
                valDeserializer));

            nodeToSourcePatterns.Add(name, topicPattern);
            nodeGrouper.Add(name);
            _nodeGroups = null;
        }

        public void AddSink<K, V>(
            string name,
            string topic,
            ISerializer<K> keySerializer,
            ISerializer<V> valSerializer,
            IStreamPartitioner<K, V> partitioner,
            string predecessorName)
        {
            this.AddSink(
                name,
                topic,
                keySerializer,
                valSerializer,
                partitioner,
                new[] { predecessorName });
        }

        public void AddSink<K, V>(
            string name,
            string topic,
            ISerializer<K> keySerializer,
            ISerializer<V> valSerializer,
            IStreamPartitioner<K, V> partitioner,
            string[] predecessorNames)
        {
            name = name ?? throw new ArgumentNullException(nameof(name));
            topic = topic ?? throw new ArgumentNullException(nameof(topic));
            predecessorNames = predecessorNames ?? throw new ArgumentNullException(nameof(predecessorNames));

            if (predecessorNames.Length == 0)
            {
                throw new TopologyException($"Sink {name} must have at least one parent");
            }

            AddSink(
                name,
                new StaticTopicNameExtractor(topic),
                keySerializer,
                valSerializer,
                partitioner,
                predecessorNames);

            nodeToSinkTopic.Add(name, topic);
            _nodeGroups = null;
        }

        public void AddSink<K, V>(
            string name,
            ITopicNameExtractor topicExtractor,
            ISerializer<K> keySerializer,
            ISerializer<V> valSerializer,
            IStreamPartitioner<K, V> partitioner,
            string[] predecessorNames)
        {
            name = name ?? throw new ArgumentNullException(nameof(name));
            topicExtractor = topicExtractor ?? throw new ArgumentNullException(nameof(topicExtractor));
            predecessorNames = predecessorNames ?? throw new ArgumentNullException(nameof(predecessorNames));

            if (nodeFactories.ContainsKey(name))
            {
                throw new TopologyException($"IProcessor {name} is already.Added.");
            }

            if (predecessorNames.Length == 0)
            {
                throw new TopologyException($"Sink {name} must have at least one parent");
            }

            foreach (var predecessor in predecessorNames)
            {
                Objects.requireNonNull(predecessor, "predecessor name can't be null");

                if (predecessor.Equals(name))
                {
                    throw new TopologyException($"IProcessor {name} cannot be a predecessor of itself.");
                }

                if (!nodeFactories.ContainsKey(predecessor))
                {
                    throw new TopologyException($"Predecessor processor {predecessor} is not added yet.");
                }

                if (nodeToSinkTopic.ContainsKey(predecessor))
                {
                    throw new TopologyException($"Sink {predecessor} cannot be used a parent.");
                }
            }

            nodeFactories.Add(
                name,
                new SinkNodeFactory<K, V>(
                    this.Clock,
                    name,
                    predecessorNames,
                    topicExtractor,
                    keySerializer,
                    valSerializer,
                    partitioner));

            nodeGrouper.Add(name);
            nodeGrouper.Unite(name, predecessorNames);
            _nodeGroups = null;
        }

        public void AddProcessor<K, V>(
            string name,
            IProcessorSupplier<K, V> supplier,
            string predecessorNames)
        {
            AddProcessor<K, V>(
                name,
                supplier,
                new[] { predecessorNames });
        }

        public void AddProcessor<K, V>(
            string name,
            IProcessorSupplier<K, V> supplier,
            params string[] predecessorNames)
        {
            name = name ?? throw new ArgumentNullException(nameof(name));
            supplier = supplier ?? throw new ArgumentNullException(nameof(supplier));
            predecessorNames = predecessorNames ?? throw new ArgumentNullException(nameof(predecessorNames));

            if (nodeFactories.ContainsKey(name))
            {
                throw new TopologyException($"IProcessor {name} is already.Added.");
            }

            if (predecessorNames.Length == 0)
            {
                throw new TopologyException($"IProcessor {name} must have at least one parent");
            }

            foreach (var predecessor in predecessorNames)
            {
                if (predecessor == null)
                {
                    throw new ArgumentNullException(nameof(predecessor));
                }

                if (predecessor.Equals(name))
                {
                    throw new TopologyException($"IProcessor {name} cannot be a predecessor of itself.");
                }

                if (!nodeFactories.ContainsKey(predecessor))
                {
                    throw new TopologyException($"Predecessor processor {predecessor} is not added yet for {name}");
                }
            }

            nodeFactories.Add(name, new ProcessorNodeFactory<K, V>(this.Clock, name, predecessorNames, supplier));
            nodeGrouper.Add(name);

            nodeGrouper.Unite(name, predecessorNames);
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
            storeBuilder = storeBuilder ?? throw new ArgumentNullException(nameof(storeBuilder));
            if (!allowOverride && stateFactories.ContainsKey(storeBuilder.name))
            {
                throw new TopologyException($"IStateStore {storeBuilder.name} is already.Added.");
            }

            stateFactories.Add(storeBuilder.name, (IStateStoreFactory<IStateStore>)new StateStoreFactory<T>(storeBuilder));

            if (processorNames != null)
            {
                foreach (var processorName in processorNames)
                {
                    if (processorName == null)
                    {
                        throw new ArgumentNullException(nameof(processorName));
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
            storeBuilder = storeBuilder ?? throw new ArgumentNullException(nameof(storeBuilder));
            ValidateGlobalStoreArguments<K, V>(
                sourceName,
                topic,
                processorName,
                stateUpdateSupplier,
                storeBuilder.name,
                storeBuilder.loggingEnabled);

            ValidateTopicNotAlreadyRegistered(topic);

            string[] topics = { topic };
            string[] predecessors = { sourceName };

            var nodeFactory = new ProcessorNodeFactory<K, V>(
                this.Clock,
                processorName,
                predecessors,
                stateUpdateSupplier);

            globalTopics.Add(topic);

            nodeFactories.Add(
                sourceName,
                new SourceNodeFactory<K, V>(
                    this.Clock,
                    sourceName,
                    topics,
                    null,
                    timestampExtractor,
                    topicToPatterns: null,
                    nodeToSourceTopics,
                    this,
                    keyDeserializer,
                    valueDeserializer));

            globalStateBuilders.Add(storeBuilder.name, (IStoreBuilder<IStateStore>)storeBuilder);
            connectSourceStoreAndTopic(storeBuilder.name, topic);
            nodeToSourceTopics.Add(sourceName, topics.ToList());
            nodeGrouper.Unite(processorName, predecessors);
            nodeFactories.Add(processorName, nodeFactory);
            nodeFactory.addStateStore(storeBuilder.name);
            nodeGrouper.Add(processorName);
            nodeGrouper.Add(sourceName);
            _nodeGroups = null;
        }

        private void ValidateTopicNotAlreadyRegistered(string topic)
        {
            if (sourceTopicNames.Contains(topic) || globalTopics.Contains(topic))
            {
                throw new TopologyException($"Topic {topic} has already been registered by another source.");
            }

            foreach (Regex pattern in nodeToSourcePatterns.Values)
            {
                if (pattern.IsMatch(topic))
                {
                    throw new TopologyException($"Topic {topic} matches a Regex already registered by another source.");
                }
            }
        }

        public void ConnectProcessorAndStateStores(
            string processorName,
            string[] stateStoreNames)
        {
            processorName = processorName ?? throw new ArgumentNullException(nameof(processorName));
            stateStoreNames = stateStoreNames ?? throw new ArgumentNullException(nameof(stateStoreNames));
            if (stateStoreNames.Length == 0)
            {
                throw new TopologyException("Must provide at least one state store name.");
            }

            foreach (var stateStoreName in stateStoreNames)
            {
                //stateStoreName = stateStoreName ?? throw new ArgumentNullException(nameof(stateStoreName));
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
            topicName = topicName ?? throw new ArgumentNullException(nameof(topicName));
            internalTopicNames.Add(topicName);
        }

        public void copartitionSources(HashSet<string> sourceNodes)
        {
            copartitionSourceGroups.Add(new HashSet<string>(sourceNodes));
        }

        private void ValidateGlobalStoreArguments<K, V>(
            string sourceName,
            string topic,
            string processorName,
            IProcessorSupplier<K, V> stateUpdateSupplier,
            string storeName,
            bool loggingEnabled)
        {
            sourceName = sourceName ?? throw new ArgumentNullException(nameof(sourceName));
            topic = topic ?? throw new ArgumentNullException(nameof(topic));
            stateUpdateSupplier = stateUpdateSupplier ?? throw new ArgumentNullException(nameof(stateUpdateSupplier));
            processorName = processorName ?? throw new ArgumentNullException(nameof(processorName));
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

            var stateStoreFactory = stateFactories[stateStoreName];
            IEnumerator<string> iter = stateStoreFactory.Users.GetEnumerator();

            if (iter.MoveNext())
            {
                var user = iter.Current;
                nodeGrouper.Unite(user, processorName);
            }

            stateStoreFactory.Users.Add(processorName);

            var nodeFactory = nodeFactories[processorName];
            if (nodeFactory is ProcessorNodeFactory<K, V> processorNodeFactory)
            {
                processorNodeFactory.addStateStore(stateStoreName);
                ConnectStateStoreNameToSourceTopicsOrPattern(stateStoreName, processorNodeFactory);
            }
            else
            {
                throw new TopologyException("cannot connect a state store " + stateStoreName + " to a source node or a sink node.");
            }
        }

        private HashSet<SourceNodeFactory<K, V>> findSourcesForProcessorPredecessors<K, V>(IEnumerable<string> predecessors)
        {
            var sourceNodes = new HashSet<SourceNodeFactory<K, V>>();
            foreach (var predecessor in predecessors)
            {
                var nodeFactory = nodeFactories[predecessor];
                if (nodeFactory is SourceNodeFactory<K, V>)
                {
                    sourceNodes.Add((SourceNodeFactory<K, V>)nodeFactory);
                }
                else if (nodeFactory is ProcessorNodeFactory<K, V>)
                {
                    sourceNodes.AddRange(findSourcesForProcessorPredecessors<K, V>(((ProcessorNodeFactory<K, V>)nodeFactory).Predecessors));
                }
            }

            return sourceNodes;
        }

        private void ConnectStateStoreNameToSourceTopicsOrPattern<K, V>(
            string stateStoreName,
            ProcessorNodeFactory<K, V> processorNodeFactory)
        {
            // we should never update the mapping from state store names to source topics if the store name already exists
            // in the map; this scenario is possible, for example, that a state store underlying a source KTable is
            // connecting to a join operator whose source topic is not the original KTable's source topic but an internal repartition topic.
            if (_stateStoreNameToSourceTopics.ContainsKey(stateStoreName)
                || stateStoreNameToSourceRegex.ContainsKey(stateStoreName))
            {
                return;
            }

            var sourceTopics = new HashSet<string>();
            var sourcePatterns = new HashSet<Regex>();
            var sourceNodesForPredecessor =
                findSourcesForProcessorPredecessors<K, V>(processorNodeFactory.Predecessors);

            foreach (var sourceNodeFactory in sourceNodesForPredecessor)
            {
                if (sourceNodeFactory.Pattern != null)
                {
                    sourcePatterns.Add(sourceNodeFactory.Pattern);
                }
                else
                {
                    sourceTopics.AddRange(sourceNodeFactory.Topics);
                }
            }

            if (sourceTopics.Any())
            {
                _stateStoreNameToSourceTopics.Add(
                    stateStoreName,
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
                AutoOffsetReset? offsetReset,
                T item)
        {
            if (offsetReset.HasValue)
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
                        throw new TopologyException($"Unrecognized {nameof(AutoOffsetReset)} value: {offsetReset}");
                }
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
            var nodeGroups = new Dictionary<int, HashSet<string>>();
            var rootToNodeGroup = new Dictionary<string, HashSet<string>>();

            var nodeGroupId = 0;

            // Go through source nodes first. This makes the group id assignment easy to predict in tests
            var allSourceNodes = new HashSet<string>(nodeToSourceTopics.Keys);
            allSourceNodes.UnionWith(nodeToSourcePatterns.Keys);

            foreach (var nodeName in allSourceNodes)
            {
                nodeGroupId = PutNodeGroupName(nodeName, nodeGroupId, nodeGroups, rootToNodeGroup);
            }

            // Go through non-source nodes
            foreach (var nodeName in nodeFactories.Keys)
            {
                if (!nodeToSourceTopics.ContainsKey(nodeName))
                {
                    nodeGroupId = PutNodeGroupName(nodeName, nodeGroupId, nodeGroups, rootToNodeGroup);
                }
            }

            return nodeGroups;
        }

        private int PutNodeGroupName(
            string nodeName,
            int nodeGroupId,
            Dictionary<int, HashSet<string>> nodeGroups,
            Dictionary<string, HashSet<string>> rootToNodeGroup)
        {
            var newNodeGroupId = nodeGroupId;
            var root = nodeGrouper.Root(nodeName);
            rootToNodeGroup.TryGetValue(root, out var nodeGroup);

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
        public ProcessorTopology Build()
        {
            return Build((int?)null);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public ProcessorTopology Build(int? topicGroupId)
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

            return Build(nodeGroup);
        }

        /**
         * Builds the topology for any global state stores
         * @return ProcessorTopology
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public ProcessorTopology BuildGlobalStateTopology()
        {
            applicationId = applicationId ?? throw new ArgumentNullException(nameof(applicationId));

            HashSet<string> globalGroups = GetGlobalNodeGroups();
            if (!globalGroups.Any())
            {
                return null;
            }

            return Build(globalGroups);
        }

        private HashSet<string> GetGlobalNodeGroups()
        {
            var globalGroups = new HashSet<string>();
            foreach (KeyValuePair<int, HashSet<string>> nodeGroup in GetNodeGroups())
            {
                HashSet<string> nodes = nodeGroup.Value;
                foreach (var node in nodes)
                {
                    if (isGlobalSource(node))
                    {
                        globalGroups.AddRange(nodes);
                    }
                }
            }
            return globalGroups;
        }

        private ProcessorTopology Build(HashSet<string> nodeGroup)
        {
            var processorMap = new Dictionary<string, IProcessorNode>();
            var topicSourceMap = new Dictionary<string, ISourceNode>();
            var topicSinkMap = new Dictionary<string, ISinkNode>();
            var stateStoreMap = new Dictionary<string, IStateStore>();
            var repartitionTopics = new HashSet<string>();

            // create processor nodes in a topological order ("nodeFactories" is already topologically sorted)
            // also make sure the state store map values following the insertion ordering
            foreach (var factory in nodeFactories.Values)
            {
                if (nodeGroup == null || nodeGroup.Contains(factory.Name))
                {
                    var node = factory.Build();
                    processorMap.Add(node.Name, node);

                    if (factory is IProcessorNodeFactory)
                    {
                        BuildProcessorNode(
                            processorMap,
                            stateStoreMap,
                            (IProcessorNodeFactory)factory,
                            node);
                    }
                    else if (factory is ISourceNodeFactory)
                    {
                        BuildSourceNode(topicSourceMap,
                                        repartitionTopics,
                                        (ISourceNodeFactory)factory,
                                        (ISourceNode)node);
                    }
                    else if (factory is ISinkNodeFactory)
                    {
                        BuildSinkNode(processorMap,
                                      topicSinkMap,
                                      repartitionTopics,
                                      (ISinkNodeFactory)factory,
                                      (ISinkNode)node);
                    }
                    else
                    {
                        throw new TopologyException("Unknown definition: " + factory.GetType().FullName);
                    }
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

        private void BuildSinkNode(
            Dictionary<string, IProcessorNode> processorMap,
            Dictionary<string, ISinkNode> topicSinkMap,
            HashSet<string> repartitionTopics,
            ISinkNodeFactory sinkNodeFactory,
            ISinkNode node)
        {

            foreach (string predecessor in sinkNodeFactory.Predecessors)
            {
                processorMap[predecessor].AddChild(node);
                if (sinkNodeFactory.TopicExtractor is StaticTopicNameExtractor)
                {
                    string topic = ((StaticTopicNameExtractor)sinkNodeFactory.TopicExtractor).topicName;

                    if (internalTopicNames.Contains(topic))
                    {
                        // prefix the internal topic name with the application id
                        string decoratedTopic = DecorateTopic(topic);
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

        private void BuildSourceNode(
            Dictionary<string, ISourceNode> topicSourceMap,
            HashSet<string> repartitionTopics,
            ISourceNodeFactory sourceNodeFactory,
            ISourceNode node)
        {
            List<string> topics = (sourceNodeFactory.Pattern != null)
                ? sourceNodeFactory.GetTopics(SubscriptionUpdates.getUpdates())
                : sourceNodeFactory.Topics;

            foreach (string topic in topics)
            {
                if (internalTopicNames.Contains(topic))
                {
                    // prefix the internal topic name with the application id
                    string decoratedTopic = DecorateTopic(topic);
                    topicSourceMap.Add(decoratedTopic, node);
                    repartitionTopics.Add(decoratedTopic);
                }
                else
                {
                    topicSourceMap.Add(topic, node);
                }
            }
        }

        private void BuildProcessorNode(
            Dictionary<string, IProcessorNode> processorMap,
            Dictionary<string, IStateStore> stateStoreMap,
            IProcessorNodeFactory factory,
            IProcessorNode node)
        {
            foreach (string predecessor in factory.Predecessors)
            {
                var predecessorNode = processorMap[predecessor];
                predecessorNode.AddChild(node);
            }

            foreach (string stateStoreName in factory.stateStoreNames)
            {
                if (!stateStoreMap.ContainsKey(stateStoreName))
                {
                    if (stateFactories.ContainsKey(stateStoreName))
                    {
                        IStateStoreFactory stateStoreFactory = stateFactories[stateStoreName];

                        // remember the changelog topic if this state store is change-logging enabled
                        //if (/*stateStoreFactory.loggingEnabled && */!storeToChangelogTopic.ContainsKey(stateStoreName))
                        //{
                        //    string changelogTopic = ProcessorStateManager<K, V>.storeChangelogTopic(applicationId, stateStoreName);
                        //    storeToChangelogTopic.Add(stateStoreName, changelogTopic);
                        //}
                        //stateStoreMap.Add(stateStoreName, stateStoreFactory.build());
                    }
                    else
                    {
                        //                        stateStoreMap.Add(stateStoreName, globalStateStores[stateStoreName]);
                    }
                }
            }
        }

        /**
         * Get any global {@link IStateStore}s that are part of the
         * topology
         * @return map containing all global {@link IStateStore}s
         */
        public Dictionary<string, IStateStore> GlobalStateStores()
        {
            applicationId = applicationId ?? throw new ArgumentNullException(nameof(applicationId));

            return _globalStateStores;
        }

        public HashSet<string> AllStateStoreName()
        {
            applicationId = applicationId ?? throw new ArgumentNullException(nameof(applicationId));

            var allNames = new HashSet<string>(stateFactories.Keys);

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
        public Dictionary<int, TopicsInfo> TopicGroups()
        {
            var topicGroups = new Dictionary<int, TopicsInfo>();

            if (_nodeGroups == null)
            {
                _nodeGroups = MakeNodeGroups();
            }

            foreach (var entry in _nodeGroups)
            {
                var sinkTopics = new HashSet<string>();
                var sourceTopics = new HashSet<string>();
                var repartitionTopics = new Dictionary<string, InternalTopicConfig>();
                var stateChangelogTopics = new Dictionary<string, InternalTopicConfig>();

                foreach (var node in entry.Value)
                {
                    // if the node is a source node,.Add to the source topics
                    List<string> topics = nodeToSourceTopics[node];
                    if (topics != null)
                    {
                        // if some of the topics are internal,.Add them to the internal topics
                        foreach (var _topic in topics)
                        {
                            // skip global topic as they don't need partition assignment
                            if (globalTopics.Contains(_topic))
                            {
                                continue;
                            }
                            if (internalTopicNames.Contains(_topic))
                            {
                                // prefix the internal topic name with the application id
                                var internalTopic = DecorateTopic(_topic);
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

                    // if the node is a sink node, add to the sink topics
                    var topic = nodeToSinkTopic[node];
                    if (topic != null)
                    {
                        if (internalTopicNames.Contains(topic))
                        {
                            // prefix the change log topic name with the application id
                            sinkTopics.Add(DecorateTopic(topic));
                        }
                        else
                        {

                            sinkTopics.Add(topic);
                        }
                    }

                    // if the node is connected to a state store whose changelog topics are not predefined,
                    // add to the changelog topics
                    foreach (var stateFactory in stateFactories.Values)
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

        private void SetRegexMatchedTopicsToSourceNodes()
        {
            if (SubscriptionUpdates.hasUpdates())
            {
                foreach (var stringPatternEntry in nodeToSourcePatterns)
                {
                    var sourceNode = (ISourceNodeFactory)nodeFactories[stringPatternEntry.Key];

                    //need to update nodeToSourceTopics with topics matched from given regex
                    nodeToSourceTopics.Add(
                        stringPatternEntry.Key,
                        sourceNode.GetTopics(SubscriptionUpdates.getUpdates()));

                    logger.LogDebug($"nodeToSourceTopics {nodeToSourceTopics}");
                }
            }
        }

        private void SetRegexMatchedTopicToStateStore()
        {
            if (SubscriptionUpdates.hasUpdates())
            {
                foreach (var storePattern in stateStoreNameToSourceRegex)
                {
                    var updatedTopicsForStateStore = new HashSet<string>();
                    foreach (var subscriptionUpdateTopic in SubscriptionUpdates.getUpdates())
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
                        var storeTopics = _stateStoreNameToSourceTopics[storePattern.Key].ToList();
                        if (storeTopics != null)
                        {
                            updatedTopicsForStateStore.AddRange(storeTopics);
                        }

                        _stateStoreNameToSourceTopics.Add(
                            storePattern.Key,
                            updatedTopicsForStateStore);
                    }
                }
            }
        }

        private InternalTopicConfig CreateChangelogTopicConfig(
            IStateStoreFactory<IStateStore> factory,
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
        public Regex EarliestResetTopicsPattern()
        {
            return ResetTopicsPattern(earliestResetTopics, earliestResetPatterns);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Regex LatestResetTopicsPattern()
        {
            return ResetTopicsPattern(latestResetTopics, latestResetPatterns);
        }

        private Regex ResetTopicsPattern(HashSet<string> resetTopics, HashSet<Regex> resetPatterns)
        {
            var resetTopicsList = new List<string>(resetTopics);
            List<string> topics = MaybeDecorateInternalSourceTopics(resetTopicsList);

            return BuildPatternForOffsetResetTopics(topics, new List<Regex>(resetPatterns));
        }

        private static Regex BuildPatternForOffsetResetTopics(
            List<string> sourceTopics,
            List<Regex> sourcePatterns)
        {
            var builder = new StringBuilder();

            foreach (var topic in sourceTopics)
            {
                builder.Append(topic).Append("|");
            }

            foreach (Regex sourcePattern in sourcePatterns)
            {
                builder.Append(sourcePattern.ToString()).Append("|");
            }

            if (builder.Length > 0)
            {
                builder.Length -= 1;

                return new Regex(builder.ToString(), RegexOptions.Compiled);
            }

            return EMPTY_ZERO_LENGTH_PATTERN;
        }

        public Dictionary<string, List<string>> StateStoreNameToSourceTopics()
        {
            var results = new Dictionary<string, List<string>>();
            foreach (var entry in _stateStoreNameToSourceTopics)
            {
                results.Add(entry.Key, MaybeDecorateInternalSourceTopics(new List<string>(entry.Value)));
            }

            return results;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public List<HashSet<string>> copartitionGroups()
        {
            var list = new List<HashSet<string>>(copartitionSourceGroups.Count);
            foreach (HashSet<string> nodeNames in copartitionSourceGroups)
            {
                var copartitionGroup = new HashSet<string>();
                foreach (var node in nodeNames)
                {
                    List<string> topics = nodeToSourceTopics[node];
                    if (topics != null)
                    {
                        copartitionGroup.AddRange(MaybeDecorateInternalSourceTopics(topics));
                    }
                }

                list.Add(copartitionGroup);
            }

            return list;
        }

        internal List<string> MaybeDecorateInternalSourceTopics(List<string> sourceTopics)
        {
            var decoratedTopics = new List<string>();
            foreach (var topic in sourceTopics)
            {
                if (internalTopicNames.Contains(topic))
                {
                    decoratedTopics.Add(DecorateTopic(topic));
                }
                else
                {
                    decoratedTopics.Add(topic);
                }
            }

            return decoratedTopics;
        }

        internal string DecorateTopic(string topic)
        {
            if (applicationId == null)
            {
                throw new TopologyException("there are internal topics and "
                        + "applicationId hasn't been set. Call "
                        + "setApplicationId first");
            }

            return $"{applicationId}-{topic}";
        }


        [MethodImpl(MethodImplOptions.Synchronized)]
        public Regex SourceTopicPattern()
        {
            if (topicPattern == null)
            {
                var allSourceTopics = new List<string>();
                if (nodeToSourceTopics.Any())
                {
                    foreach (List<string> topics in nodeToSourceTopics.Values)
                    {
                        allSourceTopics.AddRange(MaybeDecorateInternalSourceTopics(topics));
                    }
                }

                allSourceTopics.Sort();

                topicPattern = BuildPatternForOffsetResetTopics(allSourceTopics, nodeToSourcePatterns.Values.ToList());
            }

            return topicPattern;
        }

        // package-private for testing only
        [MethodImpl(MethodImplOptions.Synchronized)]
        void UpdateSubscriptions(SubscriptionUpdates subscriptionUpdates, string logPrefix)
        {
            logger.LogDebug($"{logPrefix}updating builder with {subscriptionUpdates} topic(s) with possible matching regex subscription(s)");

            this.SubscriptionUpdates = subscriptionUpdates;
            SetRegexMatchedTopicsToSourceNodes();
            SetRegexMatchedTopicToStateStore();
        }

        private bool isGlobalSource(string nodeName)
        {
            var nodeFactory = nodeFactories[nodeName];

            if (nodeFactory is ISourceNodeFactory)
            {
                List<string> topics = ((ISourceNodeFactory)nodeFactory).Topics;

                return topics != null && topics.Count == 1 && globalTopics.Contains(topics[0]);
            }

            return false;
        }

        public TopologyDescription describe()
        {
            var description = new TopologyDescription();

            foreach (var nodeGroup in MakeNodeGroups())
            {
                HashSet<string> allNodesOfGroups = nodeGroup.Value;
                var isNodeGroupOfGlobalStores = nodeGroupContainsGlobalSourceNode(allNodesOfGroups);

                if (!isNodeGroupOfGlobalStores)
                {
                    DescribeSubtopology(description, nodeGroup.Key, allNodesOfGroups);
                }
                else
                {
                    DescribeGlobalStore(description, allNodesOfGroups, nodeGroup.Key);
                }
            }

            return description;
        }

        private void DescribeGlobalStore(
            TopologyDescription description,
            HashSet<string> nodes,
            int id)
        {
            IEnumerator<string> it = nodes.GetEnumerator();
            while (it.MoveNext())
            {
                var node = it.Current;

                if (isGlobalSource(node))
                {
                    // we found a GlobalStore node group; those contain exactly two node: {sourceNode,processorNode}
                    // it.Remove(); // Remove sourceNode from group
                    var processorNode = nodes.GetEnumerator().Current; // get remaining processorNode

                    description.addGlobalStore(new GlobalStore(
                        sourceName: node,
                        processorName: processorNode,
                        storeName: (nodeFactories[processorNode] as IProcessorNodeFactory)?.stateStoreNames.GetEnumerator().Current ?? "",
                        topicName: nodeToSourceTopics[node].First(),
                        id: id));

                    break;
                }
            }
        }

        private bool nodeGroupContainsGlobalSourceNode(HashSet<string> allNodesOfGroups)
        {
            foreach (var node in allNodesOfGroups)
            {
                if (isGlobalSource(node))
                {
                    return true;
                }
            }

            return false;
        }

        private static readonly NodeComparator NODE_COMPARATOR = new NodeComparator();

        private static void UpdateSize(AbstractNode node, int delta)
        {
            node.Size += delta;

            foreach (INode predecessor in node.Predecessors)
            {
                UpdateSize((AbstractNode)predecessor, delta);
            }
        }

        private void DescribeSubtopology(
            TopologyDescription description,
            int subtopologyId,
            HashSet<string> nodeNames)
        {
            var nodesByName = new Dictionary<string, AbstractNode>();

            // add all nodes
            foreach (var nodeName in nodeNames)
            {
                nodesByName.Add(nodeName, (AbstractNode)nodeFactories[nodeName].Describe());
            }

            // connect each node to its predecessors and successors
            foreach (AbstractNode node in nodesByName.Values)
            {
                foreach (var predecessorName in nodeFactories[node.Name].Predecessors)
                {
                    AbstractNode predecessor = nodesByName[predecessorName];
                    node.AddPredecessor(predecessor);
                    predecessor.AddSuccessor(node);
                    UpdateSize(predecessor, node.Size);
                }
            }

            description.addSubtopology(
                new Subtopology(
                    subtopologyId,
                    new HashSet<INode>(nodesByName.Values)));
        }

        public void AddSuccessor(INode successor)
        {
            throw new InvalidOperationException("Sinks don't have successors.");
        }

        private static readonly GlobalStoreComparator GLOBALSTORE_COMPARATOR = new GlobalStoreComparator();

        private static readonly SubtopologyComparator SUBTOPOLOGY_COMPARATOR = new SubtopologyComparator();

        internal static string GetNodeNames(HashSet<INode> nodes)
        {
            var sb = new StringBuilder();
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

        public void UpdateSubscribedTopics(
            HashSet<string> topics)//,
                                   //string logPrefix)
        {
            if (topics is null)
            {
                throw new ArgumentNullException(nameof(topics));
            }

            var logPrefix = "";
            var subscriptionUpdates = new SubscriptionUpdates();
            logger.LogDebug($"{logPrefix}found {topics.Count} topics possibly matching regex");
            // update the topic groups with the returned subscription set for regex pattern subscriptions
            subscriptionUpdates.updateTopics(topics.ToList());

            UpdateSubscriptions(subscriptionUpdates, logPrefix);
        }


        // following functions are for test only
        [MethodImpl(MethodImplOptions.Synchronized)]
        public HashSet<string> getSourceTopicNames()
        {
            return sourceTopicNames;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Dictionary<string, IStateStoreFactory<IStateStore>> getStateStores()
        {
            return stateFactories;
        }
    }
}
