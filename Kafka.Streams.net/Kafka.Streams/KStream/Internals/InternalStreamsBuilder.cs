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
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.Processor;
using Kafka.Streams.Processor.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Microsoft.Extensions.Logging;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Runtime.CompilerServices;

namespace Kafka.Streams.KStream.Internals
{
    public class InternalStreamsBuilder : InternalNameProvider
    {
        InternalTopologyBuilder internalTopologyBuilder;
        private AtomicInteger index = new AtomicInteger(0);

        private AtomicInteger buildPriorityIndex = new AtomicInteger(0);
        private LinkedHashMap<StreamsGraphNode, HashSet<OptimizableRepartitionNode>> keyChangingOperationsToOptimizableRepartitionNodes = new LinkedHashMap<>();
        private HashSet<StreamsGraphNode> mergeNodes = new HashSet<StreamsGraphNod>();
        private HashSet<StreamsGraphNode> tableSourceNodes = new HashSet<StreamsGraphNod>();

        private static string TOPOLOGY_ROOT = "root";
        private static ILogger LOG = new LoggerFactory().CreateLogger<InternalStreamsBuilder>();

        protected StreamsGraphNode root = new StreamsGraphNode(TOPOLOGY_ROOT);


        public void writeToTopology(InternalTopologyBuilder topologyBuilder)
        {
            // no-op for root node
        }


        public InternalStreamsBuilder(InternalTopologyBuilder internalTopologyBuilder)
        {
            this.internalTopologyBuilder = internalTopologyBuilder;
        }

        public KStream<K, V> stream<K, V>(Collection<string> topics,
                                            ConsumedInternal<K, V> consumed)
        {

            string name = new NamedInternal(consumed.name()).orElseGenerateWithPrefix(this, KStreamImpl.SOURCE_NAME);
            StreamSourceNode<K, V> streamSourceNode = new StreamSourceNode<K, V>(name, topics, consumed);

            addGraphNode(root, streamSourceNode);

            return new KStreamImpl<>(name,
                                     consumed.keySerde(),
                                     consumed.valueSerde(),
                                     Collections.singleton(name),
                                     false,
                                     streamSourceNode,
                                     this);
        }

        public KStream<K, V> stream(Pattern topicPattern,
                                            ConsumedInternal<K, V> consumed)
        {
            string name = newProcessorName(KStreamImpl.SOURCE_NAME);
            StreamSourceNode<K, V> streamPatternSourceNode = new StreamSourceNode<>(name, topicPattern, consumed);

            addGraphNode(root, streamPatternSourceNode);

            return new KStreamImpl<>(name,
                                     consumed.keySerde(),
                                     consumed.valueSerde(),
                                     Collections.singleton(name),
                                     false,
                                     streamPatternSourceNode,
                                     this);
        }

        public KTable<K, V> table(string topic,
                                          ConsumedInternal<K, V> consumed,
                                          MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            string sourceName = new NamedInternal(consumed.name())
                   .orElseGenerateWithPrefix(this, KStreamImpl.SOURCE_NAME);
            string tableSourceName = new NamedInternal(consumed.name())
                   .suffixWithOrElseGet("-table-source", this, KTableImpl.SOURCE_NAME);
            KTableSource<K, V> tableSource = new KTableSource<K, V>(materialized.storeName(), materialized.queryableStoreName());

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(tableSource, tableSourceName);

            TableSourceNode<K, V> tableSourceNode = TableSourceNode.< K, V> tableSourceNodeBuilder()
                .withTopic(topic)
                .withSourceName(sourceName)
                .withNodeName(tableSourceName)
                .withConsumedInternal(consumed)
                .withMaterializedInternal(materialized)
                .withProcessorParameters(processorParameters)
                .build();

            addGraphNode(root, tableSourceNode);

            return new KTableImpl<>(tableSourceName,
                                    consumed.keySerde(),
                                    consumed.valueSerde(),
                                    Collections.singleton(sourceName),
                                    materialized.queryableStoreName(),
                                    tableSource,
                                    tableSourceNode,
                                    this);
        }

        public GlobalKTable<K, V> globalTable(string topic,
                                                      ConsumedInternal<K, V> consumed,
                                                      MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            consumed = consumed ?? throw new System.ArgumentNullException("consumed can't be null", nameof(consumed));
            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));
            // explicitly disable logging for global stores
            materialized.withLoggingDisabled();
            string sourceName = newProcessorName(KTableImpl.SOURCE_NAME);
            string processorName = newProcessorName(KTableImpl.SOURCE_NAME);
            // enforce store name as queryable name to always materialize global table stores
            string storeName = materialized.storeName();
            KTableSource<K, V> tableSource = new KTableSource<>(storeName, storeName);

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<>(tableSource, processorName);

            TableSourceNode<K, V> tableSourceNode = TableSourceNode.< K, V> tableSourceNodeBuilder()
                .withTopic(topic)
                .isGlobalKTable(true)
                .withSourceName(sourceName)
                .withConsumedInternal(consumed)
                .withMaterializedInternal(materialized)
                .withProcessorParameters(processorParameters)
                .build();

            addGraphNode(root, tableSourceNode);

            return new GlobalKTableImpl<>(new KTableSourceValueGetterSupplier<>(storeName), materialized.queryableStoreName());
        }


        public string newProcessorName(string prefix)
        {
            return prefix + string.Format("%010d", index.getAndIncrement());
        }


        public string newStoreName(string prefix)
        {
            return prefix + string.Format(KTableImpl.STATE_STORE_NAME + "%010d", index.getAndIncrement());
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void addStateStore(StoreBuilder builder)
        {
            addGraphNode(root, new StateStoreNode(builder));
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void addGlobalStore(StoreBuilder<IKeyValueStore> storeBuilder,
                                                 string sourceName,
                                                 string topic,
                                                 ConsumedInternal consumed,
                                                 string processorName,
                                                 ProcessorSupplier stateUpdateSupplier)
        {

            StreamsGraphNode globalStoreNode = new GlobalStoreNode(storeBuilder,
                                                                        sourceName,
                                                                        topic,
                                                                        consumed,
                                                                        processorName,
                                                                        stateUpdateSupplier);

            addGraphNode(root, globalStoreNode);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void addGlobalStore(StoreBuilder<IKeyValueStore> storeBuilder,
                                                 string topic,
                                                 ConsumedInternal consumed,
                                                 ProcessorSupplier stateUpdateSupplier)
        {
            // explicitly disable logging for global stores
            storeBuilder.withLoggingDisabled();
            string sourceName = newProcessorName(KStreamImpl.SOURCE_NAME);
            string processorName = newProcessorName(KTableImpl.SOURCE_NAME);
            addGlobalStore(storeBuilder,
                            sourceName,
                            topic,
                            consumed,
                            processorName,
                            stateUpdateSupplier);
        }

        public void addGraphNode(StreamsGraphNode parent,
                           StreamsGraphNode child)
        {
            parent = parent ?? throw new System.ArgumentNullException("parent node can't be null", nameof(parent));
            child = child ?? throw new System.ArgumentNullException("child node can't be null", nameof(child));
            parent.addChild(child);
            maybeAddNodeForOptimizationMetadata(child);
        }


        void addGraphNode(Collection<StreamsGraphNode> parents,
                           StreamsGraphNode child)
        {
            parents = parents ?? throw new System.ArgumentNullException("parent node can't be null", nameof(parents));
            child = child ?? throw new System.ArgumentNullException("child node can't be null", nameof(child));

            if (parents.isEmpty())
            {
                throw new StreamsException("Parent node collection can't be empty");
            }

            foreach (StreamsGraphNode parent in parents)
            {
                addGraphNode(parent, child);
            }
        }

        private void maybeAddNodeForOptimizationMetadata(StreamsGraphNode node)
        {
            node.setBuildPriority(buildPriorityIndex.getAndIncrement());

            if (node.parentNodes().isEmpty() && !node.nodeName().Equals(TOPOLOGY_ROOT))
            {
                throw new InvalidOperationException(
                    "Nodes should not have a null parent node.  Name: " + node.nodeName() + " Type: "
                    + node.getClass().getSimpleName());
            }

            if (node.isKeyChangingOperation())
            {
                keyChangingOperationsToOptimizableRepartitionNodes.Add(node, new HashSet<>());
            }
            else if (node is OptimizableRepartitionNode)
            {
                StreamsGraphNode parentNode = getKeyChangingParentNode(node);
                if (parentNode != null)
                {
                    keyChangingOperationsToOptimizableRepartitionNodes[parentNode].Add((OptimizableRepartitionNode)node);
                }
            }
            else if (node.isMergeNode())
            {
                mergeNodes.Add(node);
            }
            else if (node is TableSourceNode)
            {
                tableSourceNodes.Add(node);
            }
        }

        // use this method for testing only
        public void buildAndOptimizeTopology()
        {
            buildAndOptimizeTopology(null);
        }

        public void buildAndOptimizeTopology(Properties props)
        {

            maybePerformOptimizations(props);

            PriorityQueue<StreamsGraphNode> graphNodePriorityQueue = new PriorityQueue<StreamsGraphNode>(5, Comparator.comparing(StreamsGraphNode::buildPriority));

            graphNodePriorityQueue.offer(root);

            while (!graphNodePriorityQueue.isEmpty())
            {
                StreamsGraphNode streamGraphNode = graphNodePriorityQueue.Remove();

                if (LOG.isDebugEnabled())
                {
                    LOG.LogDebug("Adding nodes to topology {} child nodes {}", streamGraphNode, streamGraphNode.children());
                }

                if (streamGraphNode.allParentsWrittenToTopology() && !streamGraphNode.hasWrittenToTopology())
                {
                    streamGraphNode.writeToTopology(internalTopologyBuilder);
                    streamGraphNode.setHasWrittenToTopology(true);
                }

                foreach (StreamsGraphNode graphNode in streamGraphNode.children())
                {
                    graphNodePriorityQueue.offer(graphNode);
                }
            }
        }

        private void maybePerformOptimizations(Properties props)
        {

            if (props != null && StreamsConfig.OPTIMIZE.Equals(props.getProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION)))
            {
                LOG.LogDebug("Optimizing the Kafka Streams graph for repartition nodes");
                optimizeKTableSourceTopics();
                maybeOptimizeRepartitionOperations();
            }
        }

        private void optimizeKTableSourceTopics()
        {
            LOG.LogDebug("Marking KTable source nodes to optimize using source topic for changelogs ");
            tableSourceNodes.ForEach(node => ((TableSourceNode)node).reuseSourceTopicForChangeLog(true));
        }


        private void maybeOptimizeRepartitionOperations()
        {
            maybeUpdateKeyChangingRepartitionNodeMap();
            IEnumerator<Entry<StreamsGraphNode, HashSet<OptimizableRepartitionNode>>> entryIterator = keyChangingOperationsToOptimizableRepartitionNodes.entrySet().iterator();

            while (entryIterator.hasNext())
            {
                KeyValuePair<StreamsGraphNode, HashSet<OptimizableRepartitionNode>> entry = entryIterator.next();

                StreamsGraphNode keyChangingNode = entry.Key;

                if (entry.Value.isEmpty())
                {
                    continue;
                }

                GroupedInternal groupedInternal = new GroupedInternal(getRepartitionSerdes(entry.Value));

                string repartitionTopicName = getFirstRepartitionTopicName(entry.Value);
                //passing in the name of the first repartition topic, re-used to create the optimized repartition topic
                StreamsGraphNode optimizedSingleRepartition = createRepartitionNode(repartitionTopicName,
                                                                                         groupedInternal.keySerde(),
                                                                                         groupedInternal.valueSerde());

                // re-use parent buildPriority to make sure the single repartition graph node is evaluated before downstream nodes
                optimizedSingleRepartition.setBuildPriority(keyChangingNode.buildPriority());

                foreach (var repartitionNodeToBeReplaced in entry.Value)
                {

                    StreamsGraphNode keyChangingNodeChild = findParentNodeMatching(repartitionNodeToBeReplaced, gn => gn.parentNodes().contains(keyChangingNode));

                    if (keyChangingNodeChild == null)
                    {
                        throw new StreamsException(string.Format("Found a null keyChangingChild node for %s", repartitionNodeToBeReplaced));
                    }

                    LOG.LogDebug("Found the child node of the key changer {} from the repartition {}.", keyChangingNodeChild, repartitionNodeToBeReplaced);

                    // need to.Add children of key-changing node as children of optimized repartition
                    // in order to process records from re-partitioning
                    optimizedSingleRepartition.AddChild(keyChangingNodeChild);

                    LOG.LogDebug("Removing {} from {}  children {}", keyChangingNodeChild, keyChangingNode, keyChangingNode.children());
                    // now Remove children from key-changing node
                    keyChangingNode.removeChild(keyChangingNodeChild);

                    // now need to get children of repartition node so we can Remove repartition node
                    Collection<StreamsGraphNode> repartitionNodeToBeReplacedChildren = repartitionNodeToBeReplaced.children();
                    Collection<StreamsGraphNode> parentsOfRepartitionNodeToBeReplaced = repartitionNodeToBeReplaced.parentNodes();

                    foreach (StreamsGraphNode repartitionNodeToBeReplacedChild in repartitionNodeToBeReplacedChildren)
                    {
                        foreach (StreamsGraphNode parentNode in parentsOfRepartitionNodeToBeReplaced)
                        {
                            parentNode.AddChild(repartitionNodeToBeReplacedChild);
                        }
                    }

                    foreach (StreamsGraphNode parentNode in parentsOfRepartitionNodeToBeReplaced)
                    {
                        parentNode.removeChild(repartitionNodeToBeReplaced);
                    }
                    repartitionNodeToBeReplaced.clearChildren();

                    LOG.LogDebug("Updated node {} children {}", optimizedSingleRepartition, optimizedSingleRepartition.children());
                }

                keyChangingNode.AddChild(optimizedSingleRepartition);
                entryIterator.Remove();
            }
        }

        private void maybeUpdateKeyChangingRepartitionNodeMap()
        {
            Dictionary<StreamsGraphNode, HashSet<StreamsGraphNode>> mergeNodesToKeyChangers = new HashMap<>();
            foreach (StreamsGraphNode mergeNode in mergeNodes)
            {
                mergeNodesToKeyChangers.Add(mergeNode, new HashSet<>());
                Collection<StreamsGraphNode> keys = keyChangingOperationsToOptimizableRepartitionNodes.keySet();
                foreach (StreamsGraphNode key in keys)
                {
                    StreamsGraphNode maybeParentKey = findParentNodeMatching(mergeNode, node->node.parentNodes().contains(key));
                    if (maybeParentKey != null)
                    {
                        mergeNodesToKeyChangers[mergeNode].Add(key);
                    }
                }
            }

            foreach (KeyValuePair<StreamsGraphNode, HashSet<StreamsGraphNode>> entry in mergeNodesToKeyChangers.entrySet())
            {
                StreamsGraphNode mergeKey = entry.Key;
                Collection<StreamsGraphNode> keyChangingParents = entry.Value;
                HashSet<OptimizableRepartitionNode> repartitionNodes = new HashSet<>();
                foreach (StreamsGraphNode keyChangingParent in keyChangingParents)
                {
                    repartitionNodes.AddAll(keyChangingOperationsToOptimizableRepartitionNodes[keyChangingParent]);
                    keyChangingOperationsToOptimizableRepartitionNodes.Remove(keyChangingParent);
                }

                keyChangingOperationsToOptimizableRepartitionNodes.Add(mergeKey, repartitionNodes);
            }
        }


        private OptimizableRepartitionNode<K, V> createRepartitionNode<K, V>(string repartitionTopicName,
                                                                  ISerde<K> keySerde,
                                                                  ISerde<V> valueSerde)
        {

            OptimizableRepartitionNode.OptimizableRepartitionNodeBuilder repartitionNodeBuilder = OptimizableRepartitionNode.optimizableRepartitionNodeBuilder();
            KStreamImpl.createRepartitionedSource(this,
                                                  keySerde,
                                                  valueSerde,
                                                  repartitionTopicName,
                                                  repartitionNodeBuilder);

            // ensures setting the repartition topic to the name of the
            // first repartition topic to get merged
            // this may be an auto-generated name or a user specified name
            repartitionNodeBuilder.withRepartitionTopic(repartitionTopicName);

            return repartitionNodeBuilder.build();

        }

        private StreamsGraphNode getKeyChangingParentNode(StreamsGraphNode repartitionNode)
        {
            StreamsGraphNode shouldBeKeyChangingNode = findParentNodeMatching(repartitionNode, n => n.isKeyChangingOperation() || n.isValueChangingOperation());

            StreamsGraphNode keyChangingNode = findParentNodeMatching(repartitionNode, StreamsGraphNode::isKeyChangingOperation);
            if (shouldBeKeyChangingNode != null && shouldBeKeyChangingNode.Equals(keyChangingNode))
            {
                return keyChangingNode;
            }
            return null;
        }

        private string getFirstRepartitionTopicName(Collection<OptimizableRepartitionNode> repartitionNodes)
        {
            return repartitionNodes.iterator().next().repartitionTopic();
        }


        private GroupedInternal getRepartitionSerdes<K, V>(Collection<OptimizableRepartitionNode<K, V>> repartitionNodes)
        {
            ISerde<K> keySerde = null;
            ISerde<V> valueSerde = null;

            foreach (var repartitionNode in repartitionNodes)
            {
                if (keySerde == null && repartitionNode.keySerde() != null)
                {
                    keySerde = repartitionNode.keySerde();
                }

                if (valueSerde == null && repartitionNode.valueSerde() != null)
                {
                    valueSerde = repartitionNode.valueSerde();
                }

                if (keySerde != null && valueSerde != null)
                {
                    break;
                }
            }

            return new GroupedInternal(Grouped.with(keySerde, valueSerde));
        }

        private StreamsGraphNode findParentNodeMatching(StreamsGraphNode startSeekingNode,
                                                         Predicate<StreamsGraphNode> parentNodePredicate)
        {
            if (parentNodePredicate(startSeekingNode))
            {
                return startSeekingNode;
            }

            StreamsGraphNode foundParentNode = null;

            foreach (StreamsGraphNode parentNode in startSeekingNode.parentNodes())
            {
                if (parentNodePredicate.test(parentNode))
                {
                    return parentNode;
                }
                foundParentNode = findParentNodeMatching(parentNode, parentNodePredicate);
            }
            return foundParentNode;
        }
    }
}
