using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Extensions;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Graph;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValue;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NodaTime;
using Priority_Queue;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

namespace Kafka.Streams.KStream.Internals
{
    public class InternalStreamsBuilder : IInternalNameProvider
    {
        private static class Constants
        {
            public const string TopologyRoot = "root";
        }

        private readonly IClock clock;
        private readonly ILogger<InternalStreamsBuilder> logger;
        private readonly IServiceProvider services;

        private int index = 0;
        private int buildPriorityIndex = 0;

        private readonly Dictionary<IStreamsGraphNode, HashSet<IOptimizableRepartitionNode>> keyChangingOperationsToOptimizableRepartitionNodes
            = new Dictionary<IStreamsGraphNode, HashSet<IOptimizableRepartitionNode>>();

        private readonly HashSet<IStreamsGraphNode> mergeNodes = new HashSet<IStreamsGraphNode>();
        private readonly HashSet<ITableSourceNode> tableSourceNodes = new HashSet<ITableSourceNode>();

        private readonly StreamsGraphNode root = new StreamsGraphNode(Constants.TopologyRoot);

        public InternalStreamsBuilder(
            ILogger<InternalStreamsBuilder> logger,
            IClock clock,
            IServiceProvider services,
            InternalTopologyBuilder internalTopologyBuilder)
        {
            this.clock = clock;
            this.logger = logger;
            this.services = services;
            this.InternalTopologyBuilder = internalTopologyBuilder;
        }

        public InternalTopologyBuilder InternalTopologyBuilder { get; }

        public void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            // no-op for root node
        }

        public IKStream<K, V> Stream<K, V>(
            IEnumerable<string> topics,
            ConsumedInternal<K, V> consumed)
        {
            string name = new NamedInternal(consumed.name).OrElseGenerateWithPrefix(this, KStream.SourceName);
            StreamSourceNode<K, V> streamSourceNode = new StreamSourceNode<K, V>(name, topics, consumed);

            this.AddGraphNode<K, V>(new HashSet<StreamsGraphNode> { root }, streamSourceNode);

            return new KStream<K, V>(
                this.clock,
                name,
                consumed.keySerde,
                consumed.valueSerde,
                new HashSet<string> { name },
                false,
                streamSourceNode,
                this);
        }

        public IKStream<K, V> Stream<K, V>(
            Regex topicPattern,
            ConsumedInternal<K, V> consumed)
        {
            string name = NewProcessorName(KStream.SourceName);
            var streamPatternSourceNode = new StreamSourceNode<K, V>(name, topicPattern, consumed);

            AddGraphNode<K, V>(root, streamPatternSourceNode);

            return new KStream<K, V>(
                this.clock,
                name,
                consumed.keySerde,
                consumed.valueSerde,
                new HashSet<string> { name },
                repartitionRequired: false,
                streamPatternSourceNode,
                this);
        }

        public IKTable<K, V> table<K, V>(
            string topic,
            ConsumedInternal<K, V> consumed,
            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            string sourceName = new NamedInternal(consumed.name)
                   .OrElseGenerateWithPrefix(this, KStream.SourceName);

            string tableSourceName = new NamedInternal(consumed.name)
                   .suffixWithOrElseGet("-table-source", this, KTable.SourceName);

            var tableSource = ActivatorUtilities.CreateInstance<KTableSource<K, V>>(this.services, materialized.StoreName, materialized.queryableStoreName());
            var processorParameters = new ProcessorParameters<K, V>(tableSource, tableSourceName);

            var tableSourceNode = TableSourceNode<K, V>.tableSourceNodeBuilder<IKeyValueStore<Bytes, byte[]>>(this.clock)
                 .withTopic(topic)
                 .withSourceName(sourceName)
                 .withNodeName(tableSourceName)
                 .withConsumedInternal(consumed)
                 .withMaterializedInternal(materialized)
                 .withProcessorParameters(processorParameters)
                 .build();

            AddGraphNode<K, V>(root, tableSourceNode);

            return new KTable<K, V>(
                tableSourceName,
                consumed.keySerde,
                consumed.valueSerde,
                sourceName,
                materialized.queryableStoreName(),
                tableSource,
                tableSourceNode,
                this);
        }

        public IGlobalKTable<K, V> globalTable<K, V>(
            string topic,
            ConsumedInternal<K, V> consumed,
            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            consumed = consumed ?? throw new ArgumentNullException(nameof(consumed));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));
            // explicitly disable logging for global stores
            materialized.WithLoggingDisabled();
            string sourceName = NewProcessorName(KTable.SourceName);
            string processorName = NewProcessorName(KTable.SourceName);
            // enforce store name as queryable name to always materialize global table stores
            string storeName = materialized.StoreName;
            var tableSource = ActivatorUtilities.CreateInstance<KTableSource<K, V>>(this.services, storeName, storeName);
            var processorParameters = new ProcessorParameters<K, V>(tableSource, processorName);

            var tableSourceNode = TableSourceNode<K, V>.tableSourceNodeBuilder<IKeyValueStore<Bytes, byte[]>>(this.clock)
                  .withTopic(topic)
                  .isGlobalKTable(true)
                  .withSourceName(sourceName)
                  .withConsumedInternal(consumed)
                  .withMaterializedInternal(materialized)
                  .withProcessorParameters(processorParameters)
                  .build();

            this.AddGraphNode<K, V>(root, tableSourceNode);

            return new GlobalKTableImpl<K, V>(new KTableSourceValueGetterSupplier<K, V>(storeName), materialized.queryableStoreName());
        }

        public string NewProcessorName(string prefix)
            => $"{prefix}{index++,3:D10}";

        public string NewStoreName(string prefix)
            => $"{prefix}{KTable.StateStoreName}{index++,3:D10}";

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void AddStateStore<K, V, T>(IStoreBuilder<T> builder)
            where T : IStateStore
        {
            AddGraphNode<K, V>(root, new StateStoreNode<T>(builder));
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void AddGlobalStore<K, V, T>(
            IStoreBuilder<T> storeBuilder,
            string sourceName,
            string topic,
            ConsumedInternal<K, V> consumed,
            string processorName,
            IProcessorSupplier<K, V> stateUpdateSupplier)
            where T : IStateStore
        {
            StreamsGraphNode globalStoreNode = new GlobalStoreNode<K, V, T>(
                storeBuilder,
                sourceName,
                topic,
                consumed,
                processorName,
                stateUpdateSupplier);

            this.AddGraphNode<K, V>(root, globalStoreNode);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void AddGlobalStore<K, V, T>(
            IStoreBuilder<T> storeBuilder,
            string topic,
            ConsumedInternal<K, V> consumed,
            IProcessorSupplier<K, V> stateUpdateSupplier)
            where T : IStateStore
        {
            // explicitly disable logging for global stores
            storeBuilder.WithLoggingDisabled();
            string sourceName = NewProcessorName(KStream.SourceName);
            string processorName = NewProcessorName(KTable.SourceName);

            this.AddGlobalStore<K, V, T>(
                storeBuilder,
                sourceName,
                topic,
                consumed,
                processorName,
                stateUpdateSupplier);
        }

        public void AddGraphNode<K, V>(StreamsGraphNode parent, StreamsGraphNode child)
        {
            parent = parent ?? throw new ArgumentNullException(nameof(parent));
            child = child ?? throw new ArgumentNullException(nameof(child));

            parent.AddChild(child);
            MaybeAddNodeForOptimizationMetadata<K, V>(child);
        }

        public void AddGraphNode<K, V>(
            HashSet<StreamsGraphNode> parents,
            StreamsGraphNode child)
        {
            parents = parents ?? throw new ArgumentNullException(nameof(parents));
            child = child ?? throw new ArgumentNullException(nameof(child));

            if (!parents.Any())
            {
                throw new StreamsException("Parent node collection can't be empty");
            }

            foreach (StreamsGraphNode parent in parents)
            {
                AddGraphNode<K, V>(parent, child);
            }
        }

        private void MaybeAddNodeForOptimizationMetadata<K, V>(StreamsGraphNode node)
        {
            node.SetBuildPriority(buildPriorityIndex++);

            if (!node.ParentNodes.Any() && !node.NodeName.Equals(Constants.TopologyRoot))
            {
                throw new InvalidOperationException(
                    $"Nodes should not have a null parent node.  " +
                    $"Name: {node.NodeName} " +
                    $"Type: {node.GetType().Name}");
            }

            if (node.IsKeyChangingOperation)
            {
                keyChangingOperationsToOptimizableRepartitionNodes.Add(node, new HashSet<IOptimizableRepartitionNode>());
            }
            else if (node is IOptimizableRepartitionNode repartitionNode)
            {
                IStreamsGraphNode? parentNode = GetKeyChangingParentNode(repartitionNode);
                if (parentNode != null)
                {
                    keyChangingOperationsToOptimizableRepartitionNodes[parentNode].Add(repartitionNode);
                }
            }
            else if (node.IsMergeNode)
            {
                mergeNodes.Add(node);
            }
            else if (node is ITableSourceNode tableSourceNode)
            {
                tableSourceNodes.Add(tableSourceNode);
            }
        }

        // use this method for testing only
        public void BuildAndOptimizeTopology()
        {
            BuildAndOptimizeTopology(null);
        }

        public void BuildAndOptimizeTopology(StreamsConfig config)
        {
            MaybePerformOptimizations(config);

            var graphNodePriorityQueue = new SimplePriorityQueue<IStreamsGraphNode>(new StreamsGraphNodeComparer());

            graphNodePriorityQueue.Enqueue(root, root.BuildPriority);

            while (graphNodePriorityQueue.Any())
            {
                IStreamsGraphNode streamGraphNode = graphNodePriorityQueue.Dequeue();

                this.logger.LogDebug($"Adding nodes to topology {streamGraphNode} child nodes {streamGraphNode.Children().ToJoinedString()}");

                if (streamGraphNode.AllParentsWrittenToTopology() && !streamGraphNode.HasWrittenToTopology)
                {
                    streamGraphNode.WriteToTopology(this.InternalTopologyBuilder);
                    streamGraphNode.SetHasWrittenToTopology(hasWrittenToTopology: true);
                }

                foreach (IStreamsGraphNode graphNode in streamGraphNode.Children())
                {
                    graphNodePriorityQueue.Enqueue(graphNode, graphNode.BuildPriority);
                }
            }
        }

        private void MaybePerformOptimizations(StreamsConfig config)
        {
            if (config != null
                && StreamsConfigPropertyNames.OPTIMIZE.Equals(config.Get(StreamsConfigPropertyNames.TOPOLOGY_OPTIMIZATION)))
            {
                this.logger.LogDebug("Optimizing the Kafka Streams graph for repartition nodes");

                OptimizeKTableSourceTopics();
                MaybeOptimizeRepartitionOperations();
            }
        }

        private void OptimizeKTableSourceTopics()
        {
            this.logger.LogDebug("Marking KTable source nodes to optimize using source topic for changelogs ");
            foreach (var node in tableSourceNodes)
            {
                node.ShouldReuseSourceTopicForChangelog = true;
            }
        }

        private void MaybeOptimizeRepartitionOperations()
        {
            MaybeUpdateKeyChangingRepartitionNodeMap();

            foreach (var entry in keyChangingOperationsToOptimizableRepartitionNodes)
            {
                IStreamsGraphNode keyChangingNode = entry.Key;

                if (!entry.Value.Any())
                {
                    continue;
                }

                //var groupedInternal = new GroupedInternal(GetRepartitionSerdes(entry.Value.ToList()));

                //string repartitionTopicName = GetFirstRepartitionTopicName(entry.Value.ToList());

                //// passing in the name of the first repartition topic, re-used to create the optimized repartition topic
                //var optimizedSingleRepartition = CreateRepartitionNode(
                //    repartitionTopicName,
                //    groupedInternal.keySerde,
                //    groupedInternal.valueSerde);

                //// re-use parent buildPriority to make sure the single repartition graph node is evaluated before downstream nodes
                //optimizedSingleRepartition.SetBuildPriority(keyChangingNode.BuildPriority);

                //foreach (var repartitionNodeToBeReplaced in entry.Value)
                //{
                //    var keyChangingNodeChild = FindParentNodeMatching(
                //        repartitionNodeToBeReplaced,
                //        gn => gn.ParentNodes.Contains(keyChangingNode));

                //    if (keyChangingNodeChild == null)
                //    {
                //        throw new StreamsException($"Found a null keyChangingChild node for {repartitionNodeToBeReplaced}");
                //    }

                //    this.logger.LogDebug($"Found the child node of the key changer {keyChangingNodeChild} from the repartition {repartitionNodeToBeReplaced}.");

                //    // need to add children of key-changing node as children of optimized repartition
                //    // in order to process records from re-partitioning
                //    optimizedSingleRepartition.AddChild(keyChangingNodeChild);

                //    this.logger.LogDebug($"Removing {keyChangingNodeChild} from {keyChangingNode}  children {keyChangingNode.Children()}");

                //    // now Remove children from key-changing node
                //    keyChangingNode.RemoveChild(keyChangingNodeChild);

                //    // now need to get children of repartition node so we can Remove repartition node
                //    var repartitionNodeToBeReplacedChildren = repartitionNodeToBeReplaced.Children();
                //    var parentsOfRepartitionNodeToBeReplaced = repartitionNodeToBeReplaced.ParentNodes;

                //    foreach (var repartitionNodeToBeReplacedChild in repartitionNodeToBeReplacedChildren)
                //    {
                //        foreach (var parentNode in parentsOfRepartitionNodeToBeReplaced)
                //        {
                //            parentNode.AddChild(repartitionNodeToBeReplacedChild);
                //        }
                //    }

                //    foreach (var parentNode in parentsOfRepartitionNodeToBeReplaced)
                //    {
                //        parentNode.RemoveChild(repartitionNodeToBeReplaced);
                //    }

                //    repartitionNodeToBeReplaced.ClearChildren();

                //    this.logger.LogDebug($"Updated node {optimizedSingleRepartition} children {optimizedSingleRepartition.Children()}");
                //}

                //keyChangingNode.AddChild(optimizedSingleRepartition);
                // entryIterator.Remove();
            }
        }

        private void MaybeUpdateKeyChangingRepartitionNodeMap()
        {
            var mergeNodesToKeyChangers = new Dictionary<IStreamsGraphNode, HashSet<IStreamsGraphNode>>();
            foreach (var mergeNode in mergeNodes)
            {
                mergeNodesToKeyChangers.Add(mergeNode, new HashSet<IStreamsGraphNode>());
                var keys = new List<IStreamsGraphNode>(keyChangingOperationsToOptimizableRepartitionNodes.Keys);

                foreach (var key in keys)
                {
                    IStreamsGraphNode? maybeParentKey = FindParentNodeMatching(mergeNode, node => node.ParentNodes.Contains(key));
                    if (maybeParentKey != null)
                    {
                        mergeNodesToKeyChangers[mergeNode].Add(key);
                    }
                }
            }

            foreach (var entry in mergeNodesToKeyChangers)
            {
                IStreamsGraphNode mergeKey = entry.Key;
                var keyChangingParents = entry.Value.ToList();
                var repartitionNodes = new HashSet<IOptimizableRepartitionNode>();

                foreach (var keyChangingParent in keyChangingParents)
                {
                    repartitionNodes.UnionWith(keyChangingOperationsToOptimizableRepartitionNodes[keyChangingParent]);
                    keyChangingOperationsToOptimizableRepartitionNodes.Remove(keyChangingParent);
                }

                keyChangingOperationsToOptimizableRepartitionNodes.Add(mergeKey, repartitionNodes);
            }
        }

        private OptimizableRepartitionNode<K, V> CreateRepartitionNode<K, V>(
            string repartitionTopicName,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
        {
            OptimizableRepartitionNodeBuilder<K, V> repartitionNodeBuilder =
                OptimizableRepartitionNode.GetOptimizableRepartitionNodeBuilder<K, V>();

            KStream<K, V>.CreateRepartitionedSource(
                this,
                keySerde,
                valueSerde,
                repartitionTopicName,
                repartitionNodeBuilder);

            // ensures setting the repartition topic to the name of the
            // first repartition topic to get merged
            // this may be an auto-generated name or a user specified name
            repartitionNodeBuilder.WithRepartitionTopic(repartitionTopicName);

            return repartitionNodeBuilder.Build();
        }

        private IStreamsGraphNode? GetKeyChangingParentNode(IStreamsGraphNode repartitionNode)
        {
            var shouldBeKeyChangingNode = FindParentNodeMatching(repartitionNode, n => n.IsKeyChangingOperation || n.IsValueChangingOperation);

            var keyChangingNode = FindParentNodeMatching(repartitionNode, n => n.IsKeyChangingOperation);

            if (shouldBeKeyChangingNode != null && shouldBeKeyChangingNode.Equals(keyChangingNode))
            {
                return keyChangingNode;
            }

            return null;
        }

        private string GetFirstRepartitionTopicName(List<IOptimizableRepartitionNode> repartitionNodes)
        {
            return repartitionNodes.First().NodeName;
        }

        private GroupedInternal<K, V> GetRepartitionSerdes<K, V>(List<OptimizableRepartitionNode<K, V>> repartitionNodes)
        {
            ISerde<K>? keySerde = null;
            ISerde<V>? valueSerde = null;

            foreach (var repartitionNode in repartitionNodes)
            {
                if (keySerde == null && repartitionNode.keySerde != null)
                {
                    keySerde = repartitionNode.keySerde;
                }

                if (valueSerde == null && repartitionNode.valueSerde != null)
                {
                    valueSerde = repartitionNode.valueSerde;
                }

                if (keySerde != null && valueSerde != null)
                {
                    break;
                }
            }

            return new GroupedInternal<K, V>(Grouped<K, V>.With(keySerde, valueSerde));
        }

        private IStreamsGraphNode? FindParentNodeMatching(
            IStreamsGraphNode startSeekingNode,
            Predicate<IStreamsGraphNode> parentNodePredicate)
        {
            if (parentNodePredicate(startSeekingNode))
            {
                return startSeekingNode;
            }

            IStreamsGraphNode? foundParentNode = null;

            foreach (var parentNode in startSeekingNode.ParentNodes)
            {
                if (parentNodePredicate(parentNode))
                {
                    return parentNode;
                }

                foundParentNode = FindParentNodeMatching(parentNode, parentNodePredicate);
            }

            return foundParentNode;
        }
    }
}
