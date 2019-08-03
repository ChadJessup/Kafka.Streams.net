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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.internals.graph.GlobalStoreNode;
import org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StateStoreNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamSourceNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.TableSourceNode;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class InternalStreamsBuilder implements InternalNameProvider {

     InternalTopologyBuilder internalTopologyBuilder;
    private  AtomicInteger index = new AtomicInteger(0);

    private  AtomicInteger buildPriorityIndex = new AtomicInteger(0);
    private  LinkedHashMap<StreamsGraphNode, LinkedHashSet<OptimizableRepartitionNode>> keyChangingOperationsToOptimizableRepartitionNodes = new LinkedHashMap<>();
    private  LinkedHashSet<StreamsGraphNode> mergeNodes = new LinkedHashSet<>();
    private  LinkedHashSet<StreamsGraphNode> tableSourceNodes = new LinkedHashSet<>();

    private static  string TOPOLOGY_ROOT = "root";
    private static  Logger LOG = LoggerFactory.getLogger(InternalStreamsBuilder.class);

    protected  StreamsGraphNode root = new StreamsGraphNode(TOPOLOGY_ROOT) {
        @Override
        public void writeToTopology( InternalTopologyBuilder topologyBuilder) {
            // no-op for root node
        }
    };

    public InternalStreamsBuilder( InternalTopologyBuilder internalTopologyBuilder) {
        this.internalTopologyBuilder = internalTopologyBuilder;
    }

    public <K, V> KStream<K, V> stream( Collection<string> topics,
                                        ConsumedInternal<K, V> consumed) {

         string name = new NamedInternal(consumed.name()).orElseGenerateWithPrefix(this, KStreamImpl.SOURCE_NAME);
         StreamSourceNode<K, V> streamSourceNode = new StreamSourceNode<>(name, topics, consumed);

        addGraphNode(root, streamSourceNode);

        return new KStreamImpl<>(name,
                                 consumed.keySerde(),
                                 consumed.valueSerde(),
                                 Collections.singleton(name),
                                 false,
                                 streamSourceNode,
                                 this);
    }

    public <K, V> KStream<K, V> stream( Pattern topicPattern,
                                        ConsumedInternal<K, V> consumed) {
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

    public <K, V> KTable<K, V> table( string topic,
                                      ConsumedInternal<K, V> consumed,
                                      MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
         string sourceName = new NamedInternal(consumed.name())
                .orElseGenerateWithPrefix(this, KStreamImpl.SOURCE_NAME);
         string tableSourceName = new NamedInternal(consumed.name())
                .suffixWithOrElseGet("-table-source", this, KTableImpl.SOURCE_NAME);
         KTableSource<K, V> tableSource = new KTableSource<>(materialized.storeName(), materialized.queryableStoreName());
         ProcessorParameters<K, V> processorParameters = new ProcessorParameters<>(tableSource, tableSourceName);

         TableSourceNode<K, V> tableSourceNode = TableSourceNode.<K, V>tableSourceNodeBuilder()
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

    public <K, V> GlobalKTable<K, V> globalTable( string topic,
                                                  ConsumedInternal<K, V> consumed,
                                                  MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(consumed, "consumed can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        // explicitly disable logging for global stores
        materialized.withLoggingDisabled();
         string sourceName = newProcessorName(KTableImpl.SOURCE_NAME);
         string processorName = newProcessorName(KTableImpl.SOURCE_NAME);
        // enforce store name as queryable name to always materialize global table stores
         string storeName = materialized.storeName();
         KTableSource<K, V> tableSource = new KTableSource<>(storeName, storeName);

         ProcessorParameters<K, V> processorParameters = new ProcessorParameters<>(tableSource, processorName);

         TableSourceNode<K, V> tableSourceNode = TableSourceNode.<K, V>tableSourceNodeBuilder()
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

    @Override
    public string newProcessorName( string prefix) {
        return prefix + string.format("%010d", index.getAndIncrement());
    }

    @Override
    public string newStoreName( string prefix) {
        return prefix + string.format(KTableImpl.STATE_STORE_NAME + "%010d", index.getAndIncrement());
    }

    public synchronized void addStateStore( StoreBuilder builder) {
        addGraphNode(root, new StateStoreNode(builder));
    }

    public synchronized void addGlobalStore( StoreBuilder<KeyValueStore> storeBuilder,
                                             string sourceName,
                                             string topic,
                                             ConsumedInternal consumed,
                                             string processorName,
                                             ProcessorSupplier stateUpdateSupplier) {

         StreamsGraphNode globalStoreNode = new GlobalStoreNode(storeBuilder,
                                                                     sourceName,
                                                                     topic,
                                                                     consumed,
                                                                     processorName,
                                                                     stateUpdateSupplier);

        addGraphNode(root, globalStoreNode);
    }

    public synchronized void addGlobalStore( StoreBuilder<KeyValueStore> storeBuilder,
                                             string topic,
                                             ConsumedInternal consumed,
                                             ProcessorSupplier stateUpdateSupplier) {
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

    void addGraphNode( StreamsGraphNode parent,
                       StreamsGraphNode child) {
        Objects.requireNonNull(parent, "parent node can't be null");
        Objects.requireNonNull(child, "child node can't be null");
        parent.addChild(child);
        maybeAddNodeForOptimizationMetadata(child);
    }


    void addGraphNode( Collection<StreamsGraphNode> parents,
                       StreamsGraphNode child) {
        Objects.requireNonNull(parents, "parent node can't be null");
        Objects.requireNonNull(child, "child node can't be null");

        if (parents.isEmpty()) {
            throw new StreamsException("Parent node collection can't be empty");
        }

        for ( StreamsGraphNode parent : parents) {
            addGraphNode(parent, child);
        }
    }

    private void maybeAddNodeForOptimizationMetadata( StreamsGraphNode node) {
        node.setBuildPriority(buildPriorityIndex.getAndIncrement());

        if (node.parentNodes().isEmpty() && !node.nodeName().equals(TOPOLOGY_ROOT)) {
            throw new InvalidOperationException(
                "Nodes should not have a null parent node.  Name: " + node.nodeName() + " Type: "
                + node.getClass().getSimpleName());
        }

        if (node.isKeyChangingOperation()) {
            keyChangingOperationsToOptimizableRepartitionNodes.put(node, new LinkedHashSet<>());
        } else if (node is OptimizableRepartitionNode) {
             StreamsGraphNode parentNode = getKeyChangingParentNode(node);
            if (parentNode != null) {
                keyChangingOperationsToOptimizableRepartitionNodes.get(parentNode).add((OptimizableRepartitionNode) node);
            }
        } else if (node.isMergeNode()) {
            mergeNodes.add(node);
        } else if (node is TableSourceNode) {
            tableSourceNodes.add(node);
        }
    }

    // use this method for testing only
    public void buildAndOptimizeTopology() {
        buildAndOptimizeTopology(null);
    }

    public void buildAndOptimizeTopology( Properties props) {

        maybePerformOptimizations(props);

         PriorityQueue<StreamsGraphNode> graphNodePriorityQueue = new PriorityQueue<>(5, Comparator.comparing(StreamsGraphNode::buildPriority));

        graphNodePriorityQueue.offer(root);

        while (!graphNodePriorityQueue.isEmpty()) {
             StreamsGraphNode streamGraphNode = graphNodePriorityQueue.remove();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Adding nodes to topology {} child nodes {}", streamGraphNode, streamGraphNode.children());
            }

            if (streamGraphNode.allParentsWrittenToTopology() && !streamGraphNode.hasWrittenToTopology()) {
                streamGraphNode.writeToTopology(internalTopologyBuilder);
                streamGraphNode.setHasWrittenToTopology(true);
            }

            for ( StreamsGraphNode graphNode : streamGraphNode.children()) {
                graphNodePriorityQueue.offer(graphNode);
            }
        }
    }

    private void maybePerformOptimizations( Properties props) {

        if (props != null && StreamsConfig.OPTIMIZE.equals(props.getProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION))) {
            LOG.debug("Optimizing the Kafka Streams graph for repartition nodes");
            optimizeKTableSourceTopics();
            maybeOptimizeRepartitionOperations();
        }
    }

    private void optimizeKTableSourceTopics() {
        LOG.debug("Marking KTable source nodes to optimize using source topic for changelogs ");
        tableSourceNodes.forEach(node -> ((TableSourceNode) node).reuseSourceTopicForChangeLog(true));
    }

    @SuppressWarnings("unchecked")
    private void maybeOptimizeRepartitionOperations() {
        maybeUpdateKeyChangingRepartitionNodeMap();
         Iterator<Entry<StreamsGraphNode, LinkedHashSet<OptimizableRepartitionNode>>> entryIterator =  keyChangingOperationsToOptimizableRepartitionNodes.entrySet().iterator();

        while (entryIterator.hasNext()) {
             Map.Entry<StreamsGraphNode, LinkedHashSet<OptimizableRepartitionNode>> entry = entryIterator.next();

             StreamsGraphNode keyChangingNode = entry.getKey();

            if (entry.getValue().isEmpty()) {
                continue;
            }

             GroupedInternal groupedInternal = new GroupedInternal(getRepartitionSerdes(entry.getValue()));

             string repartitionTopicName = getFirstRepartitionTopicName(entry.getValue());
            //passing in the name of the first repartition topic, re-used to create the optimized repartition topic
             StreamsGraphNode optimizedSingleRepartition = createRepartitionNode(repartitionTopicName,
                                                                                      groupedInternal.keySerde(),
                                                                                      groupedInternal.valueSerde());

            // re-use parent buildPriority to make sure the single repartition graph node is evaluated before downstream nodes
            optimizedSingleRepartition.setBuildPriority(keyChangingNode.buildPriority());

            for ( OptimizableRepartitionNode repartitionNodeToBeReplaced : entry.getValue()) {

                 StreamsGraphNode keyChangingNodeChild = findParentNodeMatching(repartitionNodeToBeReplaced, gn -> gn.parentNodes().contains(keyChangingNode));

                if (keyChangingNodeChild == null) {
                    throw new StreamsException(string.format("Found a null keyChangingChild node for %s", repartitionNodeToBeReplaced));
                }

                LOG.debug("Found the child node of the key changer {} from the repartition {}.", keyChangingNodeChild, repartitionNodeToBeReplaced);

                // need to add children of key-changing node as children of optimized repartition
                // in order to process records from re-partitioning
                optimizedSingleRepartition.addChild(keyChangingNodeChild);

                LOG.debug("Removing {} from {}  children {}", keyChangingNodeChild, keyChangingNode, keyChangingNode.children());
                // now remove children from key-changing node
                keyChangingNode.removeChild(keyChangingNodeChild);

                // now need to get children of repartition node so we can remove repartition node
                 Collection<StreamsGraphNode> repartitionNodeToBeReplacedChildren = repartitionNodeToBeReplaced.children();
                 Collection<StreamsGraphNode> parentsOfRepartitionNodeToBeReplaced = repartitionNodeToBeReplaced.parentNodes();

                for ( StreamsGraphNode repartitionNodeToBeReplacedChild : repartitionNodeToBeReplacedChildren) {
                    for ( StreamsGraphNode parentNode : parentsOfRepartitionNodeToBeReplaced) {
                        parentNode.addChild(repartitionNodeToBeReplacedChild);
                    }
                }

                for ( StreamsGraphNode parentNode : parentsOfRepartitionNodeToBeReplaced) {
                    parentNode.removeChild(repartitionNodeToBeReplaced);
                }
                repartitionNodeToBeReplaced.clearChildren();

                LOG.debug("Updated node {} children {}", optimizedSingleRepartition, optimizedSingleRepartition.children());
            }

            keyChangingNode.addChild(optimizedSingleRepartition);
            entryIterator.remove();
        }
    }

    private void maybeUpdateKeyChangingRepartitionNodeMap() {
         Map<StreamsGraphNode, Set<StreamsGraphNode>> mergeNodesToKeyChangers = new HashMap<>();
        for ( StreamsGraphNode mergeNode : mergeNodes) {
            mergeNodesToKeyChangers.put(mergeNode, new LinkedHashSet<>());
             Collection<StreamsGraphNode> keys = keyChangingOperationsToOptimizableRepartitionNodes.keySet();
            for ( StreamsGraphNode key : keys) {
                 StreamsGraphNode maybeParentKey = findParentNodeMatching(mergeNode, node -> node.parentNodes().contains(key));
                if (maybeParentKey != null) {
                    mergeNodesToKeyChangers.get(mergeNode).add(key);
                }
            }
        }

        for ( Map.Entry<StreamsGraphNode, Set<StreamsGraphNode>> entry : mergeNodesToKeyChangers.entrySet()) {
             StreamsGraphNode mergeKey = entry.getKey();
             Collection<StreamsGraphNode> keyChangingParents = entry.getValue();
             LinkedHashSet<OptimizableRepartitionNode> repartitionNodes = new LinkedHashSet<>();
            for ( StreamsGraphNode keyChangingParent : keyChangingParents) {
                repartitionNodes.addAll(keyChangingOperationsToOptimizableRepartitionNodes.get(keyChangingParent));
                keyChangingOperationsToOptimizableRepartitionNodes.remove(keyChangingParent);
            }

            keyChangingOperationsToOptimizableRepartitionNodes.put(mergeKey, repartitionNodes);
        }
    }

    @SuppressWarnings("unchecked")
    private OptimizableRepartitionNode createRepartitionNode( string repartitionTopicName,
                                                              Serde keySerde,
                                                              Serde valueSerde) {

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

    private StreamsGraphNode getKeyChangingParentNode( StreamsGraphNode repartitionNode) {
         StreamsGraphNode shouldBeKeyChangingNode = findParentNodeMatching(repartitionNode, n -> n.isKeyChangingOperation() || n.isValueChangingOperation());

         StreamsGraphNode keyChangingNode = findParentNodeMatching(repartitionNode, StreamsGraphNode::isKeyChangingOperation);
        if (shouldBeKeyChangingNode != null && shouldBeKeyChangingNode.equals(keyChangingNode)) {
            return keyChangingNode;
        }
        return null;
    }

    private string getFirstRepartitionTopicName( Collection<OptimizableRepartitionNode> repartitionNodes) {
        return repartitionNodes.iterator().next().repartitionTopic();
    }

    @SuppressWarnings("unchecked")
    private GroupedInternal getRepartitionSerdes( Collection<OptimizableRepartitionNode> repartitionNodes) {
        Serde keySerde = null;
        Serde valueSerde = null;

        for ( OptimizableRepartitionNode repartitionNode : repartitionNodes) {
            if (keySerde == null && repartitionNode.keySerde() != null) {
                keySerde = repartitionNode.keySerde();
            }

            if (valueSerde == null && repartitionNode.valueSerde() != null) {
                valueSerde = repartitionNode.valueSerde();
            }

            if (keySerde != null && valueSerde != null) {
                break;
            }
        }

        return new GroupedInternal(Grouped.with(keySerde, valueSerde));
    }

    private StreamsGraphNode findParentNodeMatching( StreamsGraphNode startSeekingNode,
                                                     Predicate<StreamsGraphNode> parentNodePredicate) {
        if (parentNodePredicate.test(startSeekingNode)) {
            return startSeekingNode;
        }
        StreamsGraphNode foundParentNode = null;

        for ( StreamsGraphNode parentNode : startSeekingNode.parentNodes()) {
            if (parentNodePredicate.test(parentNode)) {
                return parentNode;
            }
            foundParentNode = findParentNodeMatching(parentNode, parentNodePredicate);
        }
        return foundParentNode;
    }

    public StreamsGraphNode root() {
        return root;
    }
}
