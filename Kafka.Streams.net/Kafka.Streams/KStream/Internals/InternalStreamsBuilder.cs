using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.KStream.Graph;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Priority_Queue;

namespace Kafka.Streams.KStream.Internals
{
    public class InternalStreamsBuilder : IInternalNameProvider
    {
        private static class Constants
        {
            public const string TopologyRoot = "root";
        }

        private readonly KafkaStreamsContext context;
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
            KafkaStreamsContext context,
            IServiceProvider services,
            InternalTopologyBuilder internalTopologyBuilder)
        {
            this.context = context;
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
            var Name = new NamedInternal(consumed.Name).OrElseGenerateWithPrefix(this, KStream.SourceName);
            var streamSourceNode = new StreamSourceNode<K, V>(Name, topics, consumed);

            this.AddGraphNode<K, V>(new HashSet<StreamsGraphNode> { this.root }, streamSourceNode);

            return new KStream<K, V>(
                this.context,
                Name,
                consumed.KeySerde,
                consumed.ValueSerde,
                new HashSet<string> { Name },
                false,
                streamSourceNode,
                this);
        }

        public IKStream<K, V> Stream<K, V>(
            Regex topicPattern,
            ConsumedInternal<K, V> consumed)
        {
            var Name = this.NewProcessorName(KStream.SourceName);
            var streamPatternSourceNode = new StreamSourceNode<K, V>(Name, topicPattern, consumed);

            this.AddGraphNode<K, V>(this.root, streamPatternSourceNode);

            return new KStream<K, V>(
                this.context,
                Name,
                consumed.KeySerde,
                consumed.ValueSerde,
                new HashSet<string> { Name },
                repartitionRequired: false,
                streamPatternSourceNode,
                this);
        }

        public IKTable<K, V> Table<K, V>(
            string topic,
            ConsumedInternal<K, V> consumed,
            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            var sourceName = new NamedInternal(consumed.Name)
                   .OrElseGenerateWithPrefix(this, KStream.SourceName);

            var tableSourceName = new NamedInternal(consumed.Name)
                   .SuffixWithOrElseGet("-table-source", this, KTable.SourceName);

            var tableSource = new KTableSource<K, V>(
                this.services.GetRequiredService<ILogger<KTableSource<K, V>>>(),
                this.services,
                materialized?.StoreName,
                materialized?.QueryableStoreName());

            var processorParameters = new ProcessorParameters<K, V>(tableSource, tableSourceName);

            var tableSourceNode = TableSourceNode<K, V>.TableSourceNodeBuilder<IKeyValueStore<Bytes, byte[]>>(this.context)
                 .WithTopic(topic)
                 .WithSourceName(sourceName)
                 .WithNodeName(tableSourceName)
                 .WithConsumedInternal(consumed)
                 .WithMaterializedInternal(materialized)
                 .WithProcessorParameters(processorParameters)
                 .Build();

            this.AddGraphNode<K, V>(this.root, tableSourceNode);

            return new KTable<K, IKeyValueStore<Bytes, byte[]>, V>(
                this.context,
                tableSourceName,
                consumed.KeySerde,
                consumed.ValueSerde,
                new HashSet<string> { sourceName },
                materialized.QueryableStoreName(),
                tableSource,
                tableSourceNode,
                this);
        }

        public IGlobalKTable<K, V> GlobalTable<K, V>(
            string topic,
            ConsumedInternal<K, V> consumed,
            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            consumed = consumed ?? throw new ArgumentNullException(nameof(consumed));
            materialized = materialized ?? throw new ArgumentNullException(nameof(materialized));
            // explicitly disable logging for global stores
            materialized.WithLoggingDisabled();
            var sourceName = this.NewProcessorName(KTable.SourceName);
            var processorName = this.NewProcessorName(KTable.SourceName);
            // enforce store Name as queryable Name to always materialize global table stores
            var storeName = materialized.StoreName;
            var tableSource = ActivatorUtilities.CreateInstance<KTableSource<K, V>>(this.services, storeName, storeName);
            var processorParameters = new ProcessorParameters<K, V>(tableSource, processorName);

            var tableSourceNode = TableSourceNode<K, V>.TableSourceNodeBuilder<IKeyValueStore<Bytes, byte[]>>(this.context)
                  .WithTopic(topic)
                  .IsGlobalKTable(true)
                  .WithSourceName(sourceName)
                  .WithConsumedInternal(consumed)
                  .WithMaterializedInternal(materialized)
                  .WithProcessorParameters(processorParameters)
                  .Build();

            this.AddGraphNode<K, V>(this.root, tableSourceNode);

            return new GlobalKTableImpl<K, V>(new KTableSourceValueGetterSupplier<K, V>(this.context, storeName), materialized.QueryableStoreName());
        }

        public string NewProcessorName(string prefix)
            => $"{prefix}{this.index++,3:D10}";

        public string NewStoreName(string prefix)
            => $"{prefix}{KTable.StateStoreName}{this.index++,3:D10}";

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void AddStateStore<K, V, T>(IStoreBuilder<T> builder)
            where T : IStateStore
        {
            this.AddGraphNode<K, V>(this.root, new StateStoreNode<T>(builder));
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

            this.AddGraphNode<K, V>(this.root, globalStoreNode);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void AddGlobalStore<K, V, T>(
            IStoreBuilder<T> storeBuilder,
            string topic,
            ConsumedInternal<K, V> consumed,
            IProcessorSupplier<K, V> stateUpdateSupplier)
            where T : IStateStore
        {
            if (storeBuilder is null)
            {
                throw new ArgumentNullException(nameof(storeBuilder));
            }

            if (string.IsNullOrEmpty(topic))
            {
                throw new ArgumentException("message", nameof(topic));
            }

            if (consumed is null)
            {
                throw new ArgumentNullException(nameof(consumed));
            }

            if (stateUpdateSupplier is null)
            {
                throw new ArgumentNullException(nameof(stateUpdateSupplier));
            }

            // explicitly disable logging for global stores
            storeBuilder.WithLoggingDisabled();
            var sourceName = this.NewProcessorName(KStream.SourceName);
            var processorName = this.NewProcessorName(KTable.SourceName);

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
            this.MaybeAddNodeForOptimizationMetadata<K, V>(child);
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
                this.AddGraphNode<K, V>(parent, child);
            }
        }

        private void MaybeAddNodeForOptimizationMetadata<K, V>(StreamsGraphNode node)
        {
            node.SetBuildPriority(this.buildPriorityIndex++);

            if (!node.ParentNodes.Any() && !node.NodeName.Equals(Constants.TopologyRoot))
            {
                throw new InvalidOperationException(
                    $"Nodes should not have a null parent node.  " +
                    $"Name: {node.NodeName} " +
                    $"Type: {node.GetType().Name}");
            }

            if (node.IsKeyChangingOperation)
            {
                this.keyChangingOperationsToOptimizableRepartitionNodes.Add(node, new HashSet<IOptimizableRepartitionNode>());
            }
            else if (node is IOptimizableRepartitionNode repartitionNode)
            {
                IStreamsGraphNode? parentNode = this.GetKeyChangingParentNode(repartitionNode);
                if (parentNode != null)
                {
                    this.keyChangingOperationsToOptimizableRepartitionNodes[parentNode].Add(repartitionNode);
                }
            }
            else if (node.IsMergeNode)
            {
                this.mergeNodes.Add(node);
            }
            else if (node is ITableSourceNode tableSourceNode)
            {
                this.tableSourceNodes.Add(tableSourceNode);
            }
        }

        // use this method for testing only
        public void BuildAndOptimizeTopology()
        {
            this.BuildAndOptimizeTopology(config: null);
        }

        public void BuildAndOptimizeTopology(StreamsConfig? config)
        {
            this.MaybePerformOptimizations(config);

            var graphNodePriorityQueue = new SimplePriorityQueue<IStreamsGraphNode>(new StreamsGraphNodeComparer());

            graphNodePriorityQueue.Enqueue(this.root, this.root.BuildPriority);

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

        private void MaybePerformOptimizations(StreamsConfig? config)
        {
            if (config != null
                && StreamsConfig.OPTIMIZEConfig.Equals(config.Get(config.Get(StreamsConfig.TOPOLOGY_OPTIMIZATIONConfig))))
            {
                this.logger.LogDebug("Optimizing the Kafka Streams graph for repartition nodes");

                this.OptimizeKTableSourceTopics();
                this.MaybeOptimizeRepartitionOperations();
            }
        }

        private void OptimizeKTableSourceTopics()
        {
            this.logger.LogDebug("Marking KTable source nodes to optimize using source topic for changelogs ");
            foreach (var node in this.tableSourceNodes)
            {
                node.ShouldReuseSourceTopicForChangelog = true;
            }
        }

        private void MaybeOptimizeRepartitionOperations()
        {
            this.MaybeUpdateKeyChangingRepartitionNodeMap();

            foreach (var entry in this.keyChangingOperationsToOptimizableRepartitionNodes)
            {
                IStreamsGraphNode keyChangingNode = entry.Key;

                if (!entry.Value.Any())
                {
                    continue;
                }

                //var groupedInternal = new GroupedInternal(GetRepartitionSerdes(entry.Value.ToList()));

                //string repartitionTopicName = GetFirstRepartitionTopicName(entry.Value.ToList());

                //// passing in the Name of the first repartition topic, re-used to create the optimized repartition topic
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
            foreach (var mergeNode in this.mergeNodes)
            {
                mergeNodesToKeyChangers.Add(mergeNode, new HashSet<IStreamsGraphNode>());
                var keys = new List<IStreamsGraphNode>(this.keyChangingOperationsToOptimizableRepartitionNodes.Keys);

                foreach (var key in keys)
                {
                    IStreamsGraphNode? maybeParentKey = this.FindParentNodeMatching(mergeNode, node => node.ParentNodes.Contains(key));
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
                    repartitionNodes.UnionWith(this.keyChangingOperationsToOptimizableRepartitionNodes[keyChangingParent]);
                    this.keyChangingOperationsToOptimizableRepartitionNodes.Remove(keyChangingParent);
                }

                this.keyChangingOperationsToOptimizableRepartitionNodes.Add(mergeKey, repartitionNodes);
            }
        }

        private IStreamsGraphNode? GetKeyChangingParentNode(IStreamsGraphNode repartitionNode)
        {
            var shouldBeKeyChangingNode = this.FindParentNodeMatching(repartitionNode, n => n.IsKeyChangingOperation || n.IsValueChangingOperation);

            var keyChangingNode = this.FindParentNodeMatching(repartitionNode, n => n.IsKeyChangingOperation);

            if (shouldBeKeyChangingNode != null && shouldBeKeyChangingNode.Equals(keyChangingNode))
            {
                return keyChangingNode;
            }

            return null;
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

                foundParentNode = this.FindParentNodeMatching(parentNode, parentNodePredicate);
            }

            return foundParentNode;
        }
    }
}
