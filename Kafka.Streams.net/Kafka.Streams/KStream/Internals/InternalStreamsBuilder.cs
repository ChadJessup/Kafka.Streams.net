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
using Kafka.Common.Utils;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.Processor;
using Kafka.Streams.Processor.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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

        private readonly ILogger<InternalStreamsBuilder> logger;
        private readonly IServiceProvider services;

        private int index = 0;
        private int buildPriorityIndex = 0;

        private Dictionary<StreamsGraphNode, HashSet<OptimizableRepartitionNode>> keyChangingOperationsToOptimizableRepartitionNodes
            = new Dictionary<StreamsGraphNode, HashSet<OptimizableRepartitionNode>>();

        private HashSet<StreamsGraphNode> mergeNodes = new HashSet<StreamsGraphNode>();
        private HashSet<StreamsGraphNode> tableSourceNodes = new HashSet<StreamsGraphNode>();

        protected StreamsGraphNode root = new StreamsGraphNode(Constants.TopologyRoot);

        public InternalStreamsBuilder(
            ILogger<InternalStreamsBuilder> logger,
            IServiceProvider services,
            InternalTopologyBuilder internalTopologyBuilder)
        {
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

            AddGraphNode(root, streamSourceNode);

            return new KStream<K, V>(
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
            //string name = newProcessorName(KStreamImpl.SourceName);
            //            StreamSourceNode<K, V> streamPatternSourceNode = new StreamSourceNode<K, V>(name, topicPattern, consumed);

            //addGraphNode(root, streamPatternSourceNode);

            return null;
            //new KStreamImpl<K, V>(
            //name,
            //consumed.keySerde,
            //consumed.valueSerde,
            //name,
            //false,
            //streamPatternSourceNode,
            //this);
        }

        //        public IKTable<K, V> table<K, V>(
        //            string topic,
        //            ConsumedInternal<K, V> consumed,
        //            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        //        {
        //            //string sourceName = new NamedInternal(consumed.name)
        //            //       .orElseGenerateWithPrefix(this, KStreamImpl<K, V>.SourceName);

        //            //string tableSourceName = new NamedInternal(consumed.name)
        //            //       .suffixWithOrElseGet("-table-source", this, KTableImpl.SourceName);

        //  //          KTableSource<K, V> tableSource = new KTableSource<K, V>(materialized.storeName(), materialized.queryableStoreName());

        ////            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(tableSource, tableSourceName);

        //            //var tableSourceNode = TableSourceNode<K, V, IKeyValueStore<Bytes, byte[]>>.tableSourceNodeBuilder()
        //            //     .withTopic(topic)
        //            //     .withSourceName(sourceName)
        //            //     .withNodeName(tableSourceName)
        //            //     .withConsumedInternal(consumed)
        //            //     .withMaterializedInternal(materialized)
        //            //     .withProcessorParameters(processorParameters)
        //            //     .build();

        //            //addGraphNode(root, tableSourceNode);

        //            return null;
        //            //new KTableImpl<K, V>(tableSourceName,
        //            //                        consumed.keySerde,
        //            //                        consumed.valueSerde,
        //            //                        sourceName,
        //            //                        materialized.queryableStoreName(),
        //            //                        tableSource,
        //            //                        tableSourceNode,
        //            //                        this);
        //        }

        //        public IGlobalKTable<K, V> globalTable<K, V>(
        //            string topic,
        //            ConsumedInternal<K, V> consumed,
        //            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        //        {
        //            consumed = consumed ?? throw new System.ArgumentNullException("consumed can't be null", nameof(consumed));
        //            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));
        //            // explicitly disable logging for global stores
        //            //materialized.withLoggingDisabled();
        //            //string sourceName = newProcessorName(KTableImpl.SourceName);
        //            //string processorName = newProcessorName(KTableImpl.SourceName);
        //            //// enforce store name as queryable name to always materialize global table stores
        //            //string storeName = materialized.storeName();
        //            //KTableSource<K, V> tableSource = new KTableSource<K, V>(storeName, storeName);

        //            //ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(tableSource, processorName);

        //            //var tableSourceNode = TableSourceNode<K, V, IKeyValueStore<Bytes, byte[]>>.tableSourceNodeBuilder()
        //            //      .withTopic(topic)
        //            //      .isGlobalKTable(true)
        //            //      .withSourceName(sourceName)
        //            //      .withConsumedInternal(consumed)
        //            //      .withMaterializedInternal(materialized)
        //            //      .withProcessorParameters(processorParameters)
        //            //      .build();

        //            //addGraphNode(root, tableSourceNode);

        //            return null; // new GlobalKTableImpl<K, V>(new KTableSourceValueGetterSupplier<K, V>(storeName), materialized.queryableStoreName());
        //        }


        public string NewProcessorName(string prefix)
            => $"{prefix}{index++,3:D3}";

        public string NewStoreName(string prefix)
            => $"{prefix}{KTableImpl.StateStoreName}{index++,3:D3}";

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void AddStateStore(IStoreBuilder builder)
        {
            //AddGraphNode(root, new StateStoreNode(builder));
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void AddGlobalStore<K, V, T>(
            IStoreBuilder storeBuilder,
            string sourceName,
            string topic,
            ConsumedInternal<K, V> consumed,
            string processorName,
            IProcessorSupplier<K, V> stateUpdateSupplier)
        {

            //StreamsGraphNode globalStoreNode = new GlobalStoreNode<K, V, T>(
            //    storeBuilder,
            //    sourceName,
            //    topic,
            //    consumed,
            //    processorName,
            //    stateUpdateSupplier);

            //addGraphNode(root, globalStoreNode);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void AddGlobalStore<K, V>(
            //            IStoreBuilder<IKeyValueStore<K, V>> storeBuilder,
            string topic,
            ConsumedInternal<K, V> consumed,
            IProcessorSupplier<K, V> stateUpdateSupplier)
        {
            // explicitly disable logging for global stores
            //storeBuilder.withLoggingDisabled();
            //string sourceName = newProcessorName(KStreamImpl.SourceName);
            //string processorName = newProcessorName(KTableImpl<K, V>.SourceName);
            //addGlobalStore(storeBuilder,
            //                sourceName,
            //                topic,
            //                consumed,
            //                processorName,
            //                stateUpdateSupplier);
        }

        public void AddGraphNode(
            StreamsGraphNode parent,
            StreamsGraphNode child)
        {
            parent = parent ?? throw new ArgumentNullException(nameof(parent));
            child = child ?? throw new ArgumentNullException(nameof(child));

            parent.AddChild(child);
            MaybeAddNodeForOptimizationMetadata(child);
        }

        public void AddGraphNode(
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
                AddGraphNode(parent, child);
            }
        }

        private void MaybeAddNodeForOptimizationMetadata(StreamsGraphNode node)
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
                keyChangingOperationsToOptimizableRepartitionNodes.Add(node, new HashSet<OptimizableRepartitionNode>());
            }
            //else if (node is OptimizableRepartitionNode<K, V>)
            //{
            //    StreamsGraphNode parentNode = getKeyChangingParentNode(node);
            //    if (parentNode != null)
            //    {
            //        keyChangingOperationsToOptimizableRepartitionNodes[parentNode].Add((OptimizableRepartitionNode)node);
            //    }
            //}
            else if (node.IsMergeNode)
            {
                mergeNodes.Add(node);
            }
            //else if (node is TableSourceNode)
            //{
            //    tableSourceNodes.Add(node);
            //}
        }

        // use this method for testing only
        public void buildAndOptimizeTopology()
        {
            buildAndOptimizeTopology(null);
        }

        public void buildAndOptimizeTopology(StreamsConfig config)
        {
            maybePerformOptimizations(config);

            //            PriorityQueue<StreamsGraphNode> graphNodePriorityQueue = new PriorityQueue<StreamsGraphNode>(5, Comparator.comparing(StreamsGraphNode.buildPriority));

            //            //graphNodePriorityQueue.offer(root);

            //            while (graphNodePriorityQueue.Any())
            //            {
            //                StreamsGraphNode streamGraphNode = graphNodePriorityQueue.Remove();

            //                LOG.LogDebug("Adding nodes to topology {} child nodes {}", streamGraphNode, streamGraphNode.children());

            //                if (streamGraphNode.allParentsWrittenToTopology() && !streamGraphNode.hasWrittenToTopology)
            //                {
            //                    streamGraphNode.writeToTopology(internalTopologyBuilder);
            //                    streamGraphNode.setHasWrittenToTopology(true);
            //                }

            //                foreach (StreamsGraphNode graphNode in streamGraphNode.children())
            //                {
            ////                    graphNodePriorityQueue.offer(graphNode);
            //                }
            //            }
        }

        private void maybePerformOptimizations(StreamsConfig config)
        {
            if (config != null
                && StreamsConfigPropertyNames.OPTIMIZE.Equals(config.Get(StreamsConfigPropertyNames.TOPOLOGY_OPTIMIZATION)))
            {
                this.logger.LogDebug("Optimizing the Kafka Streams graph for repartition nodes");

                optimizeKTableSourceTopics();
                maybeOptimizeRepartitionOperations();
            }
        }

        private void optimizeKTableSourceTopics()
        {
            this.logger.LogDebug("Marking KTable source nodes to optimize using source topic for changelogs ");
            //          tableSourceNodes.ForEach(node => ((TableSourceNode)node).reuseSourceTopicForChangeLog(true));
        }


        private void maybeOptimizeRepartitionOperations()
        {
            maybeUpdateKeyChangingRepartitionNodeMap();
            //IEnumerator<Entry<StreamsGraphNode, HashSet<OptimizableRepartitionNode>>> entryIterator = keyChangingOperationsToOptimizableRepartitionNodes.iterator();

            //while (entryIterator.hasNext())
            //{
            //    KeyValuePair<StreamsGraphNode, HashSet<OptimizableRepartitionNode>> entry = entryIterator.next();

            //    StreamsGraphNode keyChangingNode = entry.Key;

            //    if (entry.Value.isEmpty())
            //    {
            //        continue;
            //    }

            //    GroupedInternal groupedInternal = new GroupedInternal(getRepartitionSerdes(entry.Value));

            //    string repartitionTopicName = getFirstRepartitionTopicName(entry.Value);
            //    //passing in the name of the first repartition topic, re-used to create the optimized repartition topic
            //    StreamsGraphNode optimizedSingleRepartition = createRepartitionNode(repartitionTopicName,
            //                                                                             groupedInternal.keySerde,
            //                                                                             groupedInternal.valueSerde);

            //    // re-use parent buildPriority to make sure the single repartition graph node is evaluated before downstream nodes
            //    optimizedSingleRepartition.setBuildPriority(keyChangingNode.buildPriority);

            //    foreach (var repartitionNodeToBeReplaced in entry.Value)
            //    {

            //        StreamsGraphNode keyChangingNodeChild = findParentNodeMatching(repartitionNodeToBeReplaced, gn => gn.parentNodes.Contains(keyChangingNode));

            //        if (keyChangingNodeChild == null)
            //        {
            //            throw new StreamsException(string.Format("Found a null keyChangingChild node for %s", repartitionNodeToBeReplaced));
            //        }

            //        LOG.LogDebug("Found the child node of the key changer {} from the repartition {}.", keyChangingNodeChild, repartitionNodeToBeReplaced);

            //        // need to.Add children of key-changing node as children of optimized repartition
            //        // in order to process records from re-partitioning
            //        optimizedSingleRepartition.AddChild(keyChangingNodeChild);

            //        LOG.LogDebug("Removing {} from {}  children {}", keyChangingNodeChild, keyChangingNode, keyChangingNode.children());
            //        // now Remove children from key-changing node
            //        keyChangingNode.removeChild(keyChangingNodeChild);

            //        // now need to get children of repartition node so we can Remove repartition node
            //        List<StreamsGraphNode> repartitionNodeToBeReplacedChildren = repartitionNodeToBeReplaced.children();
            //        List<StreamsGraphNode> parentsOfRepartitionNodeToBeReplaced = repartitionNodeToBeReplaced.parentNodes;

            //        foreach (StreamsGraphNode repartitionNodeToBeReplacedChild in repartitionNodeToBeReplacedChildren)
            //        {
            //            foreach (StreamsGraphNode parentNode in parentsOfRepartitionNodeToBeReplaced)
            //            {
            //                parentNode.AddChild(repartitionNodeToBeReplacedChild);
            //            }
            //        }

            //        foreach (StreamsGraphNode parentNode in parentsOfRepartitionNodeToBeReplaced)
            //        {
            //            parentNode.removeChild(repartitionNodeToBeReplaced);
            //        }
            //        repartitionNodeToBeReplaced.clearChildren();

            //        LOG.LogDebug("Updated node {} children {}", optimizedSingleRepartition, optimizedSingleRepartition.children());
            //    }

            //    keyChangingNode.AddChild(optimizedSingleRepartition);
            //    entryIterator.Remove();
            //}
        }

        private void maybeUpdateKeyChangingRepartitionNodeMap()
        {
            var mergeNodesToKeyChangers = new Dictionary<StreamsGraphNode, HashSet<StreamsGraphNode>>();
            foreach (StreamsGraphNode mergeNode in mergeNodes)
            {
                mergeNodesToKeyChangers.Add(mergeNode, new HashSet<StreamsGraphNode>());
                List<StreamsGraphNode> keys = new List<StreamsGraphNode>(); // keyChangingOperationsToOptimizableRepartitionNodes.Keys;

                foreach (StreamsGraphNode key in keys)
                {
                    StreamsGraphNode maybeParentKey = FindParentNodeMatching(mergeNode, node => node.ParentNodes.Contains(key));
                    if (maybeParentKey != null)
                    {
                        mergeNodesToKeyChangers[mergeNode].Add(key);
                    }
                }
            }

            foreach (KeyValuePair<StreamsGraphNode, HashSet<StreamsGraphNode>> entry in mergeNodesToKeyChangers)
            {
                StreamsGraphNode mergeKey = entry.Key;
                List<StreamsGraphNode> keyChangingParents = entry.Value.ToList();
                var repartitionNodes = new HashSet<OptimizableRepartitionNode>();

                foreach (var keyChangingParent in keyChangingParents)
                {
                    repartitionNodes.UnionWith(keyChangingOperationsToOptimizableRepartitionNodes[keyChangingParent]);
                    keyChangingOperationsToOptimizableRepartitionNodes.Remove(keyChangingParent);
                }

                //keyChangingOperationsToOptimizableRepartitionNodes.Add(mergeKey, repartitionNodes);
            }
        }


        //private OptimizableRepartitionNode<K, V> createRepartitionNode<K, V>(
        //    string repartitionTopicName,
        //    ISerde<K> keySerde,
        //    ISerde<V> valueSerde)
        //{
        //    OptimizableRepartitionNodeBuilder<K, V> repartitionNodeBuilder =
        //        OptimizableRepartitionNode.optimizableRepartitionNodeBuilder<K, V>();

        //    KStreamImpl<K, V>.createRepartitionedSource(
        //        this,
        //        keySerde,
        //        valueSerde,
        //        repartitionTopicName,
        //        repartitionNodeBuilder);

        //    // ensures setting the repartition topic to the name of the
        //    // first repartition topic to get merged
        //    // this may be an auto-generated name or a user specified name
        //    repartitionNodeBuilder.withRepartitionTopic(repartitionTopicName);

        //    return repartitionNodeBuilder.build();

        //}

        private StreamsGraphNode? GetKeyChangingParentNode(StreamsGraphNode repartitionNode)
        {
            var shouldBeKeyChangingNode = FindParentNodeMatching(repartitionNode, n => n.IsKeyChangingOperation || n.IsValueChangingOperation);

            var keyChangingNode = FindParentNodeMatching(repartitionNode, n => n.IsKeyChangingOperation);

            if (shouldBeKeyChangingNode != null && shouldBeKeyChangingNode.Equals(keyChangingNode))
            {
                return keyChangingNode;
            }

            return null;
        }

        private string GetFirstRepartitionTopicName(List<OptimizableRepartitionNode> repartitionNodes)
        {
            //.repartitionTopic()
            return ""; // repartitionNodes.First();
        }


        //private GroupedInternal<K, V> getRepartitionSerdes<K, V>(List<OptimizableRepartitionNode<K, V>> repartitionNodes)
        //{
        //    ISerde<K> keySerde = null;
        //    ISerde<V> valueSerde = null;

        //    foreach (var repartitionNode in repartitionNodes)
        //    {
        //        if (keySerde == null && repartitionNode.keySerde != null)
        //        {
        //            keySerde = repartitionNode.keySerde;
        //        }

        //        if (valueSerde == null && repartitionNode.valueSerde != null)
        //        {
        //            valueSerde = repartitionNode.valueSerde;
        //        }

        //        if (keySerde != null && valueSerde != null)
        //        {
        //            break;
        //        }
        //    }

        //    return null;
        //        //new GroupedInternal<K, V>(Grouped<K, V>
        //        //.with(keySerde, valueSerde));
        //}

        private StreamsGraphNode? FindParentNodeMatching(
            StreamsGraphNode startSeekingNode,
            Predicate<StreamsGraphNode> parentNodePredicate)
        {
            if (parentNodePredicate(startSeekingNode))
            {
                return startSeekingNode;
            }

            StreamsGraphNode? foundParentNode = null;

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
