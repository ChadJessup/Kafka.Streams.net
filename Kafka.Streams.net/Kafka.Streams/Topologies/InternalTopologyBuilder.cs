using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Factories;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
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
        private readonly ILogger<InternalTopologyBuilder> logger;
        internal KafkaStreamsContext context;
        private readonly IServiceProvider services;
        private StreamsConfig config;
        private static readonly Regex EMPTY_ZERO_LENGTH_PATTERN = new Regex("^$", RegexOptions.Compiled);
        private static readonly string[] NO_PREDECESSORS = Array.Empty<string>();

        public InternalTopologyBuilder(
            ILogger<InternalTopologyBuilder> logger,
            IServiceProvider services,
            StreamsConfig config)
        {
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

        // All topics subscribed from source processors (without application-id prefix for internal topics)
        private readonly HashSet<string> sourceTopicNames = new HashSet<string>();

        // All internal topics auto-created by the topology builder and used in source / sink processors
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

        internal ProcessorTopology BuildSubtopology(int topicGroupId)
        {
            throw new NotImplementedException();
        }

        // map from state store names to All the topics subscribed from source processors that
        // are connected to these state stores
        private readonly Dictionary<string, HashSet<string>> _stateStoreNameToSourceTopics = new Dictionary<string, HashSet<string>>();

        // map from state store names to All the regex subscribed topics from source processors that
        // are connected to these state stores
        private readonly Dictionary<string, HashSet<Regex>> stateStoreNameToSourceRegex = new Dictionary<string, HashSet<Regex>>();

        // map from state store names to this state store's corresponding changelog topic if possible
        private readonly Dictionary<string, string> storeToChangelogTopic = new Dictionary<string, string>();

        // All global topics
        private readonly HashSet<string> globalTopics = new HashSet<string>();

        private readonly HashSet<string> earliestResetTopics = new HashSet<string>();

        private readonly HashSet<string> latestResetTopics = new HashSet<string>();

        private readonly HashSet<Regex> earliestResetPatterns = new HashSet<Regex>();

        private readonly HashSet<Regex> latestResetPatterns = new HashSet<Regex>();

        private readonly QuickUnion<string> nodeGrouper = new QuickUnion<string>();

        public SubscriptionUpdates SubscriptionUpdates { get; internal set; } = new SubscriptionUpdates();

        public IClock Clock => this.context.Clock;

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
            this.SetApplicationId(config.ApplicationId);

            // maybe strip out caching layers
            if (config.CacheMaxBytesBuffering == 0L)
            {
                foreach (var storeFactory in this.stateFactories.Values)
                {
                    storeFactory.Builder.WithCachingDisabled();
                }

                foreach (var storeBuilder in this.globalStateBuilders.Values)
                {
                    storeBuilder.WithCachingDisabled();
                }
            }

            // build global state stores
            foreach (var storeBuilder in this.globalStateBuilders.Values)
            {
                this._globalStateStores.Add(storeBuilder.Name, storeBuilder.Build());
            }

            return this;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void AddSubscribedTopicsFromAssignment(List<TopicPartition> partitions, string logPrefix)
        {
            if (this.UsesPatternSubscription())
            {
                HashSet<string> assignedTopics = new HashSet<string>();
                foreach (TopicPartition topicPartition in partitions)
                {
                    assignedTopics.Add(topicPartition.Topic);
                }

                var existingTopics = this.SubscriptionUpdates.GetUpdates();

                if (!existingTopics.Equals(assignedTopics))
                {
                    assignedTopics.AddRange(existingTopics);
                    this.UpdateSubscribedTopics(assignedTopics, logPrefix);
                }
            }
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
            if (this.nodeFactories.ContainsKey(name))
            {
                throw new TopologyException("IProcessor " + name + " is already.Added.");
            }

            foreach (var topic in topics.Where(t => t != null))
            {
                this.ValidateTopicNotAlreadyRegistered(topic);
                this.MaybeAddToResetList(this.earliestResetTopics, this.latestResetTopics, offsetReset, topic);
                this.sourceTopicNames.Add(topic);
            }

            this.nodeToSourceTopics.Add(name, topics.ToList());

            this.nodeFactories.Add(
                name,
                new SourceNodeFactory<K, V>(
                    this.Clock,
                    name,
                    topics,
                    null,
                    timestampExtractor,
                    this.topicToPatterns,
                    this.nodeToSourceTopics,
                    this,
                    keyDeserializer,
                    valDeserializer));

            this.nodeGrouper.Add(name);
            this._nodeGroups = null;
        }

        public void AddSource<K, V>(
            AutoOffsetReset offsetReset,
            string name,
            ITimestampExtractor? timestampExtractor,
            IDeserializer<K>? keyDeserializer,
            IDeserializer<V>? valDeserializer,
            Regex topicPattern)
        {
            topicPattern = topicPattern ?? throw new ArgumentNullException(nameof(topicPattern));
            name = name ?? throw new ArgumentNullException(nameof(name));

            if (this.nodeFactories.ContainsKey(name))
            {
                throw new TopologyException("IProcessor " + name + " is already.Added.");
            }

            foreach (var sourceTopicName in this.sourceTopicNames)
            {
                if (topicPattern.IsMatch(sourceTopicName))
                {
                    throw new TopologyException("Regex " + topicPattern + " will match a topic that has already been registered by another source.");
                }
            }

            foreach (Regex otherPattern in this.earliestResetPatterns)
            {
                if (topicPattern.ToString().Contains(otherPattern.ToString()) || otherPattern.ToString().Contains(topicPattern.ToString()))
                {
                    throw new TopologyException($"Regex {topicPattern} will overlap with another pattern {otherPattern} already been registered by another source");
                }
            }

            foreach (Regex otherPattern in this.latestResetPatterns)
            {
                if (topicPattern.ToString().Contains(otherPattern.ToString()) || otherPattern.ToString().Contains(topicPattern.ToString()))
                {
                    throw new TopologyException($"Regex {topicPattern} will overlap with another pattern {otherPattern} already been registered by another source");
                }
            }

            this.MaybeAddToResetList(this.earliestResetPatterns, this.latestResetPatterns, offsetReset, topicPattern);

            this.nodeFactories.Add(name, new SourceNodeFactory<K, V>(
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

            this.nodeToSourcePatterns.Add(name, topicPattern);
            this.nodeGrouper.Add(name);
            this._nodeGroups = null;
        }

        public void AddSink<K, V>(
            string Name,
            string topic,
            ISerializer<K> keySerializer,
            ISerializer<V> valSerializer,
            IStreamPartitioner<K, V> partitioner,
            string predecessorName)
        {
            this.AddSink(
                Name,
                topic,
                keySerializer,
                valSerializer,
                partitioner,
                new[] { predecessorName });
        }

        public void AddSink<K, V>(
            string Name,
            string topic,
            ISerializer<K>? keySerializer,
            ISerializer<V>? valSerializer,
            IStreamPartitioner<K, V>? partitioner,
            params string[] predecessorNames)
        {
            Name = Name ?? throw new ArgumentNullException(nameof(Name));
            topic = topic ?? throw new ArgumentNullException(nameof(topic));
            predecessorNames = predecessorNames ?? throw new ArgumentNullException(nameof(predecessorNames));

            if (predecessorNames.Length == 0)
            {
                throw new TopologyException($"Sink {Name} must have at least one parent");
            }

            this.AddSink(
                Name,
                (k, v, c) => topic,
                keySerializer,
                valSerializer,
                partitioner,
                predecessorNames);

            this.nodeToSinkTopic.Add(Name, topic);
            this._nodeGroups = null;
        }

        public void AddSink<K, V>(
            string Name,
            TopicNameExtractor<K, V> topicExtractor,
            ISerializer<K>? keySerializer,
            ISerializer<V>? valSerializer,
            IStreamPartitioner<K, V>? partitioner,
            params string[] predecessorNames)
        {
            Name = Name ?? throw new ArgumentNullException(nameof(Name));
            topicExtractor = topicExtractor ?? throw new ArgumentNullException(nameof(topicExtractor));
            predecessorNames = predecessorNames ?? throw new ArgumentNullException(nameof(predecessorNames));

            if (this.nodeFactories.ContainsKey(Name))
            {
                throw new TopologyException($"IProcessor {Name} is already.Added.");
            }

            if (predecessorNames.Length == 0)
            {
                throw new TopologyException($"Sink {Name} must have at least one parent");
            }

            foreach (var predecessor in predecessorNames)
            {
                Objects.requireNonNull(predecessor, "predecessor Name can't be null");

                if (predecessor.Equals(Name))
                {
                    throw new TopologyException($"IProcessor {Name} cannot be a predecessor of itself.");
                }

                if (!this.nodeFactories.ContainsKey(predecessor))
                {
                    throw new TopologyException($"Predecessor processor {predecessor} is not added yet.");
                }

                if (this.nodeToSinkTopic.ContainsKey(predecessor))
                {
                    throw new TopologyException($"Sink {predecessor} cannot be used a parent.");
                }
            }

            this.nodeFactories.Add(
                Name,
                new SinkNodeFactory<K, V>(
                    this.Clock,
                    Name,
                    predecessorNames,
                    topicExtractor,
                    keySerializer,
                    valSerializer,
                    partitioner,
                    this));

            this.nodeGrouper.Add(Name);
            this.nodeGrouper.Unite(Name, predecessorNames);
            this._nodeGroups = null;
        }

        private bool UsesPatternSubscription()
        {
            return !this.nodeToSourcePatterns.IsEmpty();
        }

        public void AddProcessor<K, V>(
            string Name,
            IProcessorSupplier supplier,
            string predecessorNames)
        {
            this.AddProcessor<K, V>(
                Name,
                supplier,
                new[] { predecessorNames });
        }

        public void AddProcessor<K, V>(
            string Name,
            IProcessorSupplier supplier,
            params string[] predecessorNames)
        {
            Name = Name ?? throw new ArgumentNullException(nameof(Name));
            supplier = supplier ?? throw new ArgumentNullException(nameof(supplier));
            predecessorNames = predecessorNames ?? throw new ArgumentNullException(nameof(predecessorNames));

            if (this.nodeFactories.ContainsKey(Name))
            {
                throw new TopologyException($"IProcessor {Name} is already.Added.");
            }

            if (predecessorNames.Length == 0)
            {
                throw new TopologyException($"IProcessor {Name} must have at least one parent");
            }

            foreach (var predecessor in predecessorNames)
            {
                if (predecessor == null)
                {
                    throw new ArgumentNullException(nameof(predecessor));
                }

                if (predecessor.Equals(Name))
                {
                    throw new TopologyException($"IProcessor {Name} cannot be a predecessor of itself.");
                }

                if (!this.nodeFactories.ContainsKey(predecessor))
                {
                    throw new TopologyException($"Predecessor processor {predecessor} is not added yet for {Name}");
                }
            }

            this.nodeFactories.Add(Name, new ProcessorNodeFactory<K, V>(this.Clock, Name, predecessorNames, supplier));
            this.nodeGrouper.Add(Name);

            this.nodeGrouper.Unite(Name, predecessorNames);
            this._nodeGroups = null;
        }

        public void AddStateStore<K, V, T>(
            IStoreBuilder<T> storeBuilder,
            string[] processorNames)
            where T : IStateStore
        {
            this.AddStateStore<K, V, T>(
                storeBuilder,
                false,
                processorNames);
        }

        public void AddStateStore<K, V, T>(
            IStoreBuilder<T> storeBuilder,
            bool allowOverride,
            string[] processorNames)
            where T : IStateStore
        {
            storeBuilder = storeBuilder ?? throw new ArgumentNullException(nameof(storeBuilder));
            if (!allowOverride && this.stateFactories.ContainsKey(storeBuilder.Name))
            {
                throw new TopologyException($"IStateStore {storeBuilder.Name} is already.Added.");
            }

            this.stateFactories.Add(storeBuilder.Name, (IStateStoreFactory<IStateStore>)new StateStoreFactory<T>(storeBuilder));

            if (processorNames != null)
            {
                foreach (var processorName in processorNames)
                {
                    if (processorName == null)
                    {
                        throw new ArgumentNullException(nameof(processorName));
                    }

                    this.ConnectProcessorAndStateStore(processorName, storeBuilder.Name);
                }
            }

            this._nodeGroups = null;
        }

        public void AddGlobalStore<K, V, T>(
            IStoreBuilder<T> storeBuilder,
            string sourceName,
            ITimestampExtractor? timestampExtractor,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valueDeserializer,
            string topic,
            string processorName,
            IProcessorSupplier<K, V> stateUpdateSupplier)
            where T : IStateStore
        {
            storeBuilder = storeBuilder ?? throw new ArgumentNullException(nameof(storeBuilder));
            this.ValidateGlobalStoreArguments(
                sourceName,
                topic,
                processorName,
                stateUpdateSupplier,
                storeBuilder.Name,
                storeBuilder.loggingEnabled);

            this.ValidateTopicNotAlreadyRegistered(topic);

            string[] topics = { topic };
            string[] predecessors = { sourceName };

            var nodeFactory = new ProcessorNodeFactory<K, V>(
                this.Clock,
                processorName,
                predecessors,
                stateUpdateSupplier);

            this.globalTopics.Add(topic);

            this.nodeFactories.Add(
                sourceName,
                new SourceNodeFactory<K, V>(
                    this.Clock,
                    sourceName,
                    topics,
                    null,
                    timestampExtractor,
                    topicToPatterns: null,
                    this.nodeToSourceTopics,
                    this,
                    keyDeserializer,
                    valueDeserializer));

            this.globalStateBuilders.Add(storeBuilder.Name, (IStoreBuilder<IStateStore>)storeBuilder);
            this.ConnectSourceStoreAndTopic(storeBuilder.Name, topic);
            this.nodeToSourceTopics.Add(sourceName, topics.ToList());
            this.nodeGrouper.Unite(processorName, predecessors);
            this.nodeFactories.Add(processorName, nodeFactory);
            nodeFactory.AddStateStore(storeBuilder.Name);
            this.nodeGrouper.Add(processorName);
            this.nodeGrouper.Add(sourceName);
            this._nodeGroups = null;
        }

        private void ValidateTopicNotAlreadyRegistered(string topic)
        {
            if (this.sourceTopicNames.Contains(topic) || this.globalTopics.Contains(topic))
            {
                throw new TopologyException($"Topic {topic} has already been registered by another source.");
            }

            foreach (Regex pattern in this.nodeToSourcePatterns.Values)
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
                throw new TopologyException("Must provide at least one state store Name.");
            }

            foreach (var stateStoreName in stateStoreNames.Where(storeName => storeName != null))
            {
                this.ConnectProcessorAndStateStore(processorName, stateStoreName);
            }

            this._nodeGroups = null;
        }

        public void ConnectSourceStoreAndTopic(
            string sourceStoreName,
            string topic)
        {
            if (this.storeToChangelogTopic.ContainsKey(sourceStoreName))
            {
                throw new TopologyException("Source store " + sourceStoreName + " is already.Added.");
            }

            this.storeToChangelogTopic.Add(sourceStoreName, topic);
        }

        public void AddInternalTopic(string topicName)
        {
            topicName = topicName ?? throw new ArgumentNullException(nameof(topicName));
            this.internalTopicNames.Add(topicName);
        }

        public void CopartitionSources(HashSet<string> sourceNodes)
        {
            this.copartitionSourceGroups.Add(new HashSet<string>(sourceNodes));
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

            if (this.nodeFactories.ContainsKey(sourceName))
            {
                throw new TopologyException("IProcessor " + sourceName + " is already.Added.");
            }

            if (this.nodeFactories.ContainsKey(processorName))
            {
                throw new TopologyException("IProcessor " + processorName + " is already.Added.");
            }

            if (this.stateFactories.ContainsKey(storeName) || this.globalStateBuilders.ContainsKey(storeName))
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

        private void ConnectProcessorAndStateStore(
            string processorName,
            string stateStoreName)
        {
            if (this.globalStateBuilders.ContainsKey(stateStoreName))
            {
                throw new TopologyException("Global IStateStore " + stateStoreName +
                        " can be used by a IProcessor without being specified; it should not be explicitly passed.");
            }

            if (!this.stateFactories.ContainsKey(stateStoreName))
            {
                throw new TopologyException("IStateStore " + stateStoreName + " is not.Added yet.");
            }

            if (!this.nodeFactories.ContainsKey(processorName))
            {
                throw new TopologyException("IProcessor " + processorName + " is not.Added yet.");
            }

            var stateStoreFactory = this.stateFactories[stateStoreName];
            IEnumerator<string> iter = stateStoreFactory.Users.GetEnumerator();

            if (iter.MoveNext())
            {
                var user = iter.Current;
                this.nodeGrouper.Unite(user, processorName);
            }

            stateStoreFactory.Users.Add(processorName);

            var nodeFactory = this.nodeFactories[processorName];
            if (nodeFactory is IProcessorNodeFactory processorNodeFactory)
            {
                processorNodeFactory.AddStateStore(stateStoreName);
                this.ConnectStateStoreNameToSourceTopicsOrPattern(stateStoreName, processorNodeFactory);
            }
            else
            {
                throw new TopologyException("cannot connect a state store " + stateStoreName + " to a source node or a sink node.");
            }
        }

        private HashSet<ISourceNodeFactory> FindSourcesForProcessorPredecessors(IEnumerable<string> predecessors)
        {
            var sourceNodes = new HashSet<ISourceNodeFactory>();
            foreach (var predecessor in predecessors)
            {
                var nodeFactory = this.nodeFactories[predecessor];
                if (nodeFactory is ISourceNodeFactory sourceNodeFactory)
                {
                    sourceNodes.Add(sourceNodeFactory);
                }
                else if (nodeFactory is IProcessorNodeFactory)
                {
                    sourceNodes.AddRange(this.FindSourcesForProcessorPredecessors(nodeFactory.Predecessors));
                }
            }

            return sourceNodes;
        }

        private void ConnectStateStoreNameToSourceTopicsOrPattern(
            string stateStoreName,
            IProcessorNodeFactory processorNodeFactory)
        {
            // we should never update the mapping from state store names to source topics if the store Name already exists
            // in the map; this scenario is possible, for example, that a state store underlying a source KTable is
            // connecting to a join operator whose source topic is not the original KTable's source topic but an internal repartition topic.
            if (this._stateStoreNameToSourceTopics.ContainsKey(stateStoreName)
                || this.stateStoreNameToSourceRegex.ContainsKey(stateStoreName))
            {
                return;
            }

            var sourceTopics = new HashSet<string>();
            var sourcePatterns = new HashSet<Regex>();
            var sourceNodesForPredecessor =
                this.FindSourcesForProcessorPredecessors(processorNodeFactory.Predecessors);

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
                this._stateStoreNameToSourceTopics.Add(
                    stateStoreName,
                    sourceTopics);
            }

            if (sourcePatterns.Any())
            {
                this.stateStoreNameToSourceRegex.Add(
                    stateStoreName,
                    sourcePatterns);
            }
        }

        private void MaybeAddToResetList<T>(
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
            if (this._nodeGroups == null)
            {
                this._nodeGroups = this.MakeNodeGroups();
            }

            return this._nodeGroups;
        }

        private Dictionary<int, HashSet<string>> MakeNodeGroups()
        {
            var nodeGroups = new Dictionary<int, HashSet<string>>();
            var rootToNodeGroup = new Dictionary<string, HashSet<string>>();

            var nodeGroupId = 0;

            // Go through source nodes first. This makes the group id assignment easy to predict in tests
            var allSourceNodes = new HashSet<string>(this.nodeToSourceTopics.Keys);
            allSourceNodes.UnionWith(this.nodeToSourcePatterns.Keys);

            foreach (var nodeName in allSourceNodes)
            {
                nodeGroupId = this.PutNodeGroupName(nodeName, nodeGroupId, nodeGroups, rootToNodeGroup);
            }

            // Go through non-source nodes
            foreach (var nodeName in this.nodeFactories.Keys)
            {
                if (!this.nodeToSourceTopics.ContainsKey(nodeName))
                {
                    nodeGroupId = this.PutNodeGroupName(nodeName, nodeGroupId, nodeGroups, rootToNodeGroup);
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
            var root = this.nodeGrouper.Root(nodeName);
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
            return this.Build((int?)null);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public ProcessorTopology Build(int? topicGroupId)
        {
            HashSet<string> nodeGroup;
            if (topicGroupId != null)
            {
                nodeGroup = this.GetNodeGroups()[topicGroupId.Value];
            }
            else
            {
                // when topicGroupId is null, we build the full topology minus the global groups
                HashSet<string> globalNodeGroups = this.GetGlobalNodeGroups();
                var values = this.GetNodeGroups().Values;
                nodeGroup = new HashSet<string>();

                foreach (HashSet<string> value in values)
                {
                    nodeGroup.UnionWith(value);
                }

                nodeGroup = new HashSet<string>(nodeGroup.Except(globalNodeGroups));
            }

            return this.Build(nodeGroup);
        }

        /**
         * Builds the topology for any global state stores
         * @return ProcessorTopology
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public ProcessorTopology? BuildGlobalStateTopology()
        {
            this.applicationId = this.applicationId ?? throw new ArgumentNullException(nameof(this.applicationId));

            HashSet<string> globalGroups = this.GetGlobalNodeGroups();
            if (!globalGroups.Any())
            {
                return null;
            }

            return this.Build(globalGroups);
        }

        private HashSet<string> GetGlobalNodeGroups()
        {
            var globalGroups = new HashSet<string>();
            foreach (KeyValuePair<int, HashSet<string>> nodeGroup in this.GetNodeGroups())
            {
                HashSet<string> nodes = nodeGroup.Value;
                foreach (var node in nodes)
                {
                    if (this.IsGlobalSource(node))
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
            foreach (var factory in this.nodeFactories.Values)
            {
                if (nodeGroup == null || nodeGroup.Contains(factory.Name))
                {
                    var node = factory.Build();
                    processorMap.Add(node.Name, node);

                    if (factory is IProcessorNodeFactory)
                    {
                        this.BuildProcessorNode(
                            processorMap,
                            stateStoreMap,
                            (IProcessorNodeFactory)factory,
                            node);
                    }
                    else if (factory is ISourceNodeFactory)
                    {
                        this.BuildSourceNode(topicSourceMap,
                                        repartitionTopics,
                                        (ISourceNodeFactory)factory,
                                        (ISourceNode)node);
                    }
                    else if (factory is ISinkNodeFactory)
                    {
                        this.BuildSinkNode(processorMap,
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
                this._globalStateStores.Values,
                this.storeToChangelogTopic,
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
                    string topic = ((StaticTopicNameExtractor)sinkNodeFactory.TopicExtractor).TopicName;

                    if (this.internalTopicNames.Contains(topic))
                    {
                        // prefix the internal topic Name with the application id
                        string decoratedTopic = this.DecorateTopic(topic);
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
                ? sourceNodeFactory.GetTopics(this.SubscriptionUpdates.GetUpdates())
                : sourceNodeFactory.Topics;

            foreach (string topic in topics)
            {
                if (this.internalTopicNames.Contains(topic))
                {
                    // prefix the internal topic Name with the application id
                    string decoratedTopic = this.DecorateTopic(topic);
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
                    if (this.stateFactories.ContainsKey(stateStoreName))
                    {
                        IStateStoreFactory stateStoreFactory = this.stateFactories[stateStoreName];

                        // remember the changelog topic if this state store is change-logging enabled
                        //if (/*stateStoreFactory.loggingEnabled && */!storeToChangelogTopic.ContainsKey(stateStoreName))
                        //{
                        //    string changelogTopic = ProcessorStateManager<K, V>.StoreChangelogTopic(applicationId, stateStoreName);
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
         * @return map containing All global {@link IStateStore}s
         */
        public Dictionary<string, IStateStore> GlobalStateStores()
        {
            this.applicationId = this.applicationId ?? throw new ArgumentNullException(nameof(this.applicationId));

            return this._globalStateStores;
        }

        public HashSet<string> AllStateStoreName()
        {
            this.applicationId = this.applicationId ?? throw new ArgumentNullException(nameof(this.applicationId));

            var allNames = new HashSet<string>(this.stateFactories.Keys);

            allNames.UnionWith(this._globalStateStores.Keys);

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

            if (this._nodeGroups == null)
            {
                this._nodeGroups = this.MakeNodeGroups();
            }

            foreach (var entry in this._nodeGroups)
            {
                var sinkTopics = new HashSet<string>();
                var sourceTopics = new HashSet<string>();
                var repartitionTopics = new Dictionary<string, InternalTopicConfig>();
                var stateChangelogTopics = new Dictionary<string, InternalTopicConfig>();

                foreach (var node in entry.Value)
                {
                    // if the node is a source node,.Add to the source topics
                    List<string> topics = this.nodeToSourceTopics[node];
                    if (topics != null)
                    {
                        // if some of the topics are internal,.Add them to the internal topics
                        foreach (var _topic in topics)
                        {
                            // skip global topic as they don't need partition assignment
                            if (this.globalTopics.Contains(_topic))
                            {
                                continue;
                            }
                            if (this.internalTopicNames.Contains(_topic))
                            {
                                // prefix the internal topic Name with the application id
                                var internalTopic = this.DecorateTopic(_topic);
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
                    var topic = this.nodeToSinkTopic[node];
                    if (topic != null)
                    {
                        if (this.internalTopicNames.Contains(topic))
                        {
                            // prefix the change log topic Name with the application id
                            sinkTopics.Add(this.DecorateTopic(topic));
                        }
                        else
                        {

                            sinkTopics.Add(topic);
                        }
                    }

                    // if the node is connected to a state store whose changelog topics are not predefined,
                    // add to the changelog topics
                    foreach (var stateFactory in this.stateFactories.Values)
                    {
                        //if (stateFactory.loggingEnabled && stateFactory.users().Contains(node))
                        //{
                        //    string topicName = storeToChangelogTopic.ContainsKey(stateFactory.Name)
                        //        ? storeToChangelogTopic[stateFactory.Name]
                        //        : ProcessorStateManager.StoreChangelogTopic(applicationId, stateFactory.Name);

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
            if (this.SubscriptionUpdates.HasUpdates())
            {
                foreach (var stringPatternEntry in this.nodeToSourcePatterns)
                {
                    var sourceNode = (ISourceNodeFactory)this.nodeFactories[stringPatternEntry.Key];

                    //need to update nodeToSourceTopics with topics matched from given regex
                    this.nodeToSourceTopics.Add(
                        stringPatternEntry.Key,
                        sourceNode.GetTopics(this.SubscriptionUpdates.GetUpdates()));

                    this.logger.LogDebug($"nodeToSourceTopics {this.nodeToSourceTopics}");
                }
            }
        }

        private void SetRegexMatchedTopicToStateStore()
        {
            if (this.SubscriptionUpdates.HasUpdates())
            {
                foreach (var storePattern in this.stateStoreNameToSourceRegex)
                {
                    var updatedTopicsForStateStore = new HashSet<string>();
                    foreach (var subscriptionUpdateTopic in this.SubscriptionUpdates.GetUpdates())
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
                        var storeTopics = this._stateStoreNameToSourceTopics[storePattern.Key].ToList();
                        if (storeTopics != null)
                        {
                            updatedTopicsForStateStore.AddRange(storeTopics);
                        }

                        this._stateStoreNameToSourceTopics.Add(
                            storePattern.Key,
                            updatedTopicsForStateStore);
                    }
                }
            }
        }

        private InternalTopicConfig CreateChangelogTopicConfig(
            IStateStoreFactory<IStateStore> factory,
            string Name)
        {
            return null;
            //if (factory.isWindowStore())
            //{
            //    WindowedChangelogTopicConfig config = new WindowedChangelogTopicConfig(
            //        Name,
            //        factory.logConfig());

            //    config.setRetentionMs(factory.retentionPeriod());
            //    return config;
            //}
            //else
            //{

            //    return new UnwindowedChangelogTopicConfig(Name, factory.logConfig());
            //}
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Regex EarliestResetTopicsPattern()
        {
            return this.ResetTopicsPattern(this.earliestResetTopics, this.earliestResetPatterns);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Regex LatestResetTopicsPattern()
        {
            return this.ResetTopicsPattern(this.latestResetTopics, this.latestResetPatterns);
        }

        private Regex ResetTopicsPattern(HashSet<string> resetTopics, HashSet<Regex> resetPatterns)
        {
            var resetTopicsList = new List<string>(resetTopics);
            List<string> topics = this.MaybeDecorateInternalSourceTopics(resetTopicsList);

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
            foreach (var entry in this._stateStoreNameToSourceTopics)
            {
                results.Add(entry.Key, this.MaybeDecorateInternalSourceTopics(new List<string>(entry.Value)));
            }

            return results;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public List<HashSet<string>> CopartitionGroups()
        {
            var list = new List<HashSet<string>>(this.copartitionSourceGroups.Count);
            foreach (HashSet<string> nodeNames in this.copartitionSourceGroups)
            {
                var copartitionGroup = new HashSet<string>();
                foreach (var node in nodeNames)
                {
                    List<string> topics = this.nodeToSourceTopics[node];
                    if (topics != null)
                    {
                        copartitionGroup.AddRange(this.MaybeDecorateInternalSourceTopics(topics));
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
                if (this.internalTopicNames.Contains(topic))
                {
                    decoratedTopics.Add(this.DecorateTopic(topic));
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
            if (this.applicationId == null)
            {
                throw new TopologyException("there are internal topics and "
                        + "applicationId hasn't been set. Call "
                        + "setApplicationId first");
            }

            return $"{this.applicationId}-{topic}";
        }


        [MethodImpl(MethodImplOptions.Synchronized)]
        public Regex SourceTopicPattern()
        {
            if (this.topicPattern == null)
            {
                var allSourceTopics = new List<string>();
                if (this.nodeToSourceTopics.Any())
                {
                    foreach (List<string> topics in this.nodeToSourceTopics.Values)
                    {
                        allSourceTopics.AddRange(this.MaybeDecorateInternalSourceTopics(topics));
                    }
                }

                allSourceTopics.Sort();

                this.topicPattern = BuildPatternForOffsetResetTopics(
                    allSourceTopics,
                    this.nodeToSourcePatterns.Values.ToList());
            }

            return this.topicPattern;
        }

        // package-private for testing only
        [MethodImpl(MethodImplOptions.Synchronized)]
        private void UpdateSubscriptions(SubscriptionUpdates subscriptionUpdates, string logPrefix)
        {
            this.logger.LogDebug($"{logPrefix}updating builder with {subscriptionUpdates} topic(s) with possible matching regex subscription(s)");

            this.SubscriptionUpdates = subscriptionUpdates;
            this.SetRegexMatchedTopicsToSourceNodes();
            this.SetRegexMatchedTopicToStateStore();
        }

        private bool IsGlobalSource(string nodeName)
        {
            var nodeFactory = this.nodeFactories[nodeName];

            if (nodeFactory is ISourceNodeFactory)
            {
                List<string> topics = ((ISourceNodeFactory)nodeFactory).Topics;

                return topics != null && topics.Count == 1 && this.globalTopics.Contains(topics[0]);
            }

            return false;
        }

        public TopologyDescription Describe()
        {
            var description = new TopologyDescription();

            foreach (var nodeGroup in this.MakeNodeGroups())
            {
                HashSet<string> allNodesOfGroups = nodeGroup.Value;
                var isNodeGroupOfGlobalStores = this.NodeGroupContainsGlobalSourceNode(allNodesOfGroups);

                if (!isNodeGroupOfGlobalStores)
                {
                    this.DescribeSubtopology(description, nodeGroup.Key, allNodesOfGroups);
                }
                else
                {
                    this.DescribeGlobalStore(description, allNodesOfGroups, nodeGroup.Key);
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

                if (this.IsGlobalSource(node))
                {
                    // we found a GlobalStore node group; those contain exactly two node: {sourceNode,processorNode}
                    // it.Remove(); // Remove sourceNode from group
                    var processorNode = nodes.GetEnumerator().Current; // get remaining processorNode

                    description.AddGlobalStore(new GlobalStore(
                        sourceName: node,
                        processorName: processorNode,
                        storeName: (this.nodeFactories[processorNode] as IProcessorNodeFactory)?.stateStoreNames.GetEnumerator().Current ?? "",
                        topicName: this.nodeToSourceTopics[node].First(),
                        id: id));

                    break;
                }
            }
        }

        private bool NodeGroupContainsGlobalSourceNode(HashSet<string> allNodesOfGroups)
        {
            foreach (var node in allNodesOfGroups)
            {
                if (this.IsGlobalSource(node))
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

            // add All nodes
            foreach (var nodeName in nodeNames)
            {
                nodesByName.Add(nodeName, (AbstractNode)this.nodeFactories[nodeName].Describe());
            }

            // connect each node to its predecessors and successors
            foreach (AbstractNode node in nodesByName.Values)
            {
                foreach (var predecessorName in this.nodeFactories[node.Name].Predecessors)
                {
                    AbstractNode predecessor = nodesByName[predecessorName];
                    node.AddPredecessor(predecessor);
                    predecessor.AddSuccessor(node);
                    UpdateSize(predecessor, node.Size);
                }
            }

            description.AddSubtopology(
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

        public void UpdateSubscribedTopics(HashSet<string> topics, string logPrefix = "")
        {
            if (topics is null)
            {
                throw new ArgumentNullException(nameof(topics));
            }

            var subscriptionUpdates = new SubscriptionUpdates();
            this.logger.LogDebug($"{logPrefix}found {topics.Count} topics possibly matching regex");
            // update the topic groups with the returned subscription set for regex pattern subscriptions
            subscriptionUpdates.UpdateTopics(topics.ToList());

            this.UpdateSubscriptions(subscriptionUpdates, logPrefix);
        }


        // following functions are for test only
        [MethodImpl(MethodImplOptions.Synchronized)]
        public HashSet<string> GetSourceTopicNames()
        {
            return this.sourceTopicNames;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Dictionary<string, IStateStoreFactory<IStateStore>> GetStateStores()
        {
            return this.stateFactories;
        }
    }
}
