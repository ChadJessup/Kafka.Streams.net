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
package org.apache.kafka.streams.processor.internals;

using Kafka.Common.serialization.Deserializer;
using Kafka.Common.serialization.Serializer;
using Kafka.Common.Utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.IStateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.SessionStoreBuilder;
import org.apache.kafka.streams.state.internals.WindowStoreBuilder;
import org.apache.kafka.streams.state.internals.TimestampedWindowStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

public class InternalTopologyBuilder {

    private static Logger log = LoggerFactory.getLogger(InternalTopologyBuilder.class);
    private static Pattern EMPTY_ZERO_LENGTH_PATTERN = Pattern.compile("");
    private static string[] NO_PREDECESSORS = {};

    // node factories in a topological order
    private Dictionary<string, NodeFactory> nodeFactories = new LinkedHashMap<>();

    // state factories
    private Dictionary<string, StateStoreFactory> stateFactories = new HashMap<>();

    // built global state stores
    private Dictionary<string, StoreBuilder> globalStateBuilders = new LinkedHashMap<>();

    // built global state stores
    private Dictionary<string, IStateStore> globalStateStores = new LinkedHashMap<>();

    // all topics subscribed from source processors (without application-id prefix for internal topics)
    private Set<string> sourceTopicNames = new HashSet<>();

    // all internal topics auto-created by the topology builder and used in source / sink processors
    private Set<string> internalTopicNames = new HashSet<>();

    // groups of source processors that need to be copartitioned
    private List<Set<string>> copartitionSourceGroups = new ArrayList<>();

    // map from source processor names to subscribed topics (without application-id prefix for internal topics)
    private Dictionary<string, List<string>> nodeToSourceTopics = new HashMap<>();

    // map from source processor names to regex subscription patterns
    private Dictionary<string, Pattern> nodeToSourcePatterns = new LinkedHashMap<>();

    // map from sink processor names to subscribed topic (without application-id prefix for internal topics)
    private Dictionary<string, string> nodeToSinkTopic = new HashMap<>();

    // map from topics to their matched regex patterns, this is to ensure one topic is passed through on source node
    // even if it can be matched by multiple regex patterns
    private Dictionary<string, Pattern> topicToPatterns = new HashMap<>();

    // map from state store names to all the topics subscribed from source processors that
    // are connected to these state stores
    private Dictionary<string, Set<string>> stateStoreNameToSourceTopics = new HashMap<>();

    // map from state store names to all the regex subscribed topics from source processors that
    // are connected to these state stores
    private Dictionary<string, Set<Pattern>> stateStoreNameToSourceRegex = new HashMap<>();

    // map from state store names to this state store's corresponding changelog topic if possible
    private Dictionary<string, string> storeToChangelogTopic = new HashMap<>();

    // all global topics
    private Set<string> globalTopics = new HashSet<>();

    private Set<string> earliestResetTopics = new HashSet<>();

    private Set<string> latestResetTopics = new HashSet<>();

    private Set<Pattern> earliestResetPatterns = new HashSet<>();

    private Set<Pattern> latestResetPatterns = new HashSet<>();

    private QuickUnion<string> nodeGrouper = new QuickUnion<>();

    private SubscriptionUpdates subscriptionUpdates = new SubscriptionUpdates();

    private string applicationId = null;

    private Pattern topicPattern = null;

    private Dictionary<Integer, Set<string>> nodeGroups = null;

    public static class StateStoreFactory {
        private StoreBuilder builder;
        private Set<string> users = new HashSet<>();

        private StateStoreFactory(StoreBuilder<?> builder) {
            this.builder = builder;
        }

        public IStateStore build() {
            return builder.build();
        }

        long retentionPeriod() {
            if (builder is WindowStoreBuilder) {
                return ((WindowStoreBuilder) builder).retentionPeriod();
            } else if (builder is TimestampedWindowStoreBuilder) {
                return ((TimestampedWindowStoreBuilder) builder).retentionPeriod();
            } else if (builder is SessionStoreBuilder) {
                return ((SessionStoreBuilder) builder).retentionPeriod();
            } else {
                throw new InvalidOperationException("retentionPeriod is not supported when not a window store");
            }
        }

        private Set<string> users() {
            return users;
        }

        public bool loggingEnabled() {
            return builder.loggingEnabled();
        }

        private string name() {
            return builder.name();
        }

        private bool isWindowStore() {
            return builder is WindowStoreBuilder
                || builder is TimestampedWindowStoreBuilder
                || builder is SessionStoreBuilder;
        }

        // Apparently Java strips the generics from this method because we're using the raw type for builder,
        // even though this method doesn't use builder's (missing) type parameter. Our usage seems obviously
        // correct, though, hence the suppression.
        @SuppressWarnings("unchecked")
        private Dictionary<string, string> logConfig() {
            return builder.logConfig();
        }
    }

    private static abstract class NodeFactory {
        string name;
        string[] predecessors;

        NodeFactory(string name,
                    string[] predecessors) {
            this.name = name;
            this.predecessors = predecessors;
        }

        public abstract ProcessorNode build();

        abstract AbstractNode describe();
    }

    private static class ProcessorNodeFactory : NodeFactory {
        private ProcessorSupplier<?, ?> supplier;
        private Set<string> stateStoreNames = new HashSet<>();

        ProcessorNodeFactory(string name,
                             string[] predecessors,
                             ProcessorSupplier<?, ?> supplier) {
            super(name, predecessors.clone());
            this.supplier = supplier;
        }

        public void addStateStore(string stateStoreName) {
            stateStoreNames.add(stateStoreName);
        }

        @Override
        public ProcessorNode build() {
            return new ProcessorNode<>(name, supplier.get(), stateStoreNames);
        }

        @Override
        Processor describe() {
            return new Processor(name, new HashSet<>(stateStoreNames));
        }
    }

    private class SourceNodeFactory : NodeFactory {
        private List<string> topics;
        private Pattern pattern;
        private Deserializer<?> keyDeserializer;
        private Deserializer<?> valDeserializer;
        private TimestampExtractor timestampExtractor;

        private SourceNodeFactory(string name,
                                  string[] topics,
                                  Pattern pattern,
                                  TimestampExtractor timestampExtractor,
                                  Deserializer<?> keyDeserializer,
                                  Deserializer<?> valDeserializer) {
            super(name, NO_PREDECESSORS);
            this.topics = topics != null ? Arrays.asList(topics) : new ArrayList<>();
            this.pattern = pattern;
            this.keyDeserializer = keyDeserializer;
            this.valDeserializer = valDeserializer;
            this.timestampExtractor = timestampExtractor;
        }

        List<string> getTopics(Collection<string> subscribedTopics) {
            // if it is subscribed via patterns, it is possible that the topic metadata has not been updated
            // yet and hence the map from source node to topics is stale, in this case we put the pattern as a place holder;
            // this should only happen for debugging since during runtime this function should always be called after the metadata has updated.
            if (subscribedTopics.isEmpty()) {
                return Collections.singletonList(string.valueOf(pattern));
            }

            List<string> matchedTopics = new ArrayList<>();
            for (string update : subscribedTopics) {
                if (pattern == topicToPatterns.get(update)) {
                    matchedTopics.add(update);
                } else if (topicToPatterns.containsKey(update) && isMatch(update)) {
                    // the same topic cannot be matched to more than one pattern
                    // TODO: we should lift this requirement in the future
                    throw new TopologyException("Topic " + update +
                        " is already matched for another regex pattern " + topicToPatterns.get(update) +
                        " and hence cannot be matched to this regex pattern " + pattern + " any more.");
                } else if (isMatch(update)) {
                    topicToPatterns.put(update, pattern);
                    matchedTopics.add(update);
                }
            }
            return matchedTopics;
        }

        @Override
        public ProcessorNode build() {
            List<string> sourceTopics = nodeToSourceTopics.get(name);

            // if it is subscribed via patterns, it is possible that the topic metadata has not been updated
            // yet and hence the map from source node to topics is stale, in this case we put the pattern as a place holder;
            // this should only happen for debugging since during runtime this function should always be called after the metadata has updated.
            if (sourceTopics == null) {
                return new SourceNode<>(name, Collections.singletonList(string.valueOf(pattern)), timestampExtractor, keyDeserializer, valDeserializer);
            } else {
                return new SourceNode<>(name, maybeDecorateInternalSourceTopics(sourceTopics), timestampExtractor, keyDeserializer, valDeserializer);
            }
        }

        private bool isMatch(string topic) {
            return pattern.matcher(topic).matches();
        }

        @Override
        Source describe() {
            return new Source(name, topics.size() == 0 ? null : new HashSet<>(topics), pattern);
        }
    }

    private class SinkNodeFactory<K, V> : NodeFactory {
        private Serializer<K> keySerializer;
        private Serializer<V> valSerializer;
        private StreamPartitioner<? super K, ? super V> partitioner;
        private TopicNameExtractor<K, V> topicExtractor;

        private SinkNodeFactory(string name,
                                string[] predecessors,
                                TopicNameExtractor<K, V> topicExtractor,
                                Serializer<K> keySerializer,
                                Serializer<V> valSerializer,
                                StreamPartitioner<? super K, ? super V> partitioner) {
            super(name, predecessors.clone());
            this.topicExtractor = topicExtractor;
            this.keySerializer = keySerializer;
            this.valSerializer = valSerializer;
            this.partitioner = partitioner;
        }

        @Override
        public ProcessorNode build() {
            if (topicExtractor is StaticTopicNameExtractor) {
                string topic = ((StaticTopicNameExtractor) topicExtractor).topicName;
                if (internalTopicNames.contains(topic)) {
                    // prefix the internal topic name with the application id
                    return new SinkNode<>(name, new StaticTopicNameExtractor<>(decorateTopic(topic)), keySerializer, valSerializer, partitioner);
                } else {
                    return new SinkNode<>(name, topicExtractor, keySerializer, valSerializer, partitioner);
                }
            } else {
                return new SinkNode<>(name, topicExtractor, keySerializer, valSerializer, partitioner);
            }
        }

        @Override
        Sink describe() {
            return new Sink(name, topicExtractor);
        }
    }

    // public for testing only
    public synchronized InternalTopologyBuilder setApplicationId(string applicationId) {
        Objects.requireNonNull(applicationId, "applicationId can't be null");
        this.applicationId = applicationId;

        return this;
    }

    public synchronized InternalTopologyBuilder rewriteTopology(StreamsConfig config) {
        Objects.requireNonNull(config, "config can't be null");

        // set application id
        setApplicationId(config.getString(StreamsConfig.APPLICATION_ID_CONFIG));

        // maybe strip out caching layers
        if (config.getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG) == 0L) {
            for (StateStoreFactory storeFactory : stateFactories.values()) {
                storeFactory.builder.withCachingDisabled();
            }

            for (StoreBuilder storeBuilder : globalStateBuilders.values()) {
                storeBuilder.withCachingDisabled();
            }
        }

        // build global state stores
        for (StoreBuilder storeBuilder : globalStateBuilders.values()) {
            globalStateStores.put(storeBuilder.name(), storeBuilder.build());
        }

        return this;
    }

    public void addSource(Topology.AutoOffsetReset offsetReset,
                                string name,
                                TimestampExtractor timestampExtractor,
                                Deserializer keyDeserializer,
                                Deserializer valDeserializer,
                                string... topics) {
        if (topics.length == 0) {
            throw new TopologyException("You must provide at least one topic");
        }
        Objects.requireNonNull(name, "name must not be null");
        if (nodeFactories.containsKey(name)) {
            throw new TopologyException("Processor " + name + " is already added.");
        }

        for (string topic : topics) {
            Objects.requireNonNull(topic, "topic names cannot be null");
            validateTopicNotAlreadyRegistered(topic);
            maybeAddToResetList(earliestResetTopics, latestResetTopics, offsetReset, topic);
            sourceTopicNames.add(topic);
        }

        nodeFactories.put(name, new SourceNodeFactory(name, topics, null, timestampExtractor, keyDeserializer, valDeserializer));
        nodeToSourceTopics.put(name, Arrays.asList(topics));
        nodeGrouper.add(name);
        nodeGroups = null;
    }

    public void addSource(Topology.AutoOffsetReset offsetReset,
                                string name,
                                TimestampExtractor timestampExtractor,
                                Deserializer keyDeserializer,
                                Deserializer valDeserializer,
                                Pattern topicPattern) {
        Objects.requireNonNull(topicPattern, "topicPattern can't be null");
        Objects.requireNonNull(name, "name can't be null");

        if (nodeFactories.containsKey(name)) {
            throw new TopologyException("Processor " + name + " is already added.");
        }

        for (string sourceTopicName : sourceTopicNames) {
            if (topicPattern.matcher(sourceTopicName).matches()) {
                throw new TopologyException("Pattern " + topicPattern + " will match a topic that has already been registered by another source.");
            }
        }

        for (Pattern otherPattern : earliestResetPatterns) {
            if (topicPattern.pattern().contains(otherPattern.pattern()) || otherPattern.pattern().contains(topicPattern.pattern())) {
                throw new TopologyException("Pattern " + topicPattern + " will overlap with another pattern " + otherPattern + " already been registered by another source");
            }
        }

        for (Pattern otherPattern : latestResetPatterns) {
            if (topicPattern.pattern().contains(otherPattern.pattern()) || otherPattern.pattern().contains(topicPattern.pattern())) {
                throw new TopologyException("Pattern " + topicPattern + " will overlap with another pattern " + otherPattern + " already been registered by another source");
            }
        }

        maybeAddToResetList(earliestResetPatterns, latestResetPatterns, offsetReset, topicPattern);

        nodeFactories.put(name, new SourceNodeFactory(name, null, topicPattern, timestampExtractor, keyDeserializer, valDeserializer));
        nodeToSourcePatterns.put(name, topicPattern);
        nodeGrouper.add(name);
        nodeGroups = null;
    }

    public <K, V> void addSink(string name,
                                     string topic,
                                     Serializer<K> keySerializer,
                                     Serializer<V> valSerializer,
                                     StreamPartitioner<? super K, ? super V> partitioner,
                                     string... predecessorNames) {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(predecessorNames, "predecessor names must not be null");
        if (predecessorNames.length == 0) {
            throw new TopologyException("Sink " + name + " must have at least one parent");
        }

        addSink(name, new StaticTopicNameExtractor<>(topic), keySerializer, valSerializer, partitioner, predecessorNames);
        nodeToSinkTopic.put(name, topic);
        nodeGroups = null;
    }

    public <K, V> void addSink(string name,
                                     TopicNameExtractor<K, V> topicExtractor,
                                     Serializer<K> keySerializer,
                                     Serializer<V> valSerializer,
                                     StreamPartitioner<? super K, ? super V> partitioner,
                                     string... predecessorNames) {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(topicExtractor, "topic extractor must not be null");
        Objects.requireNonNull(predecessorNames, "predecessor names must not be null");
        if (nodeFactories.containsKey(name)) {
            throw new TopologyException("Processor " + name + " is already added.");
        }
        if (predecessorNames.length == 0) {
            throw new TopologyException("Sink " + name + " must have at least one parent");
        }

        for (string predecessor : predecessorNames) {
            Objects.requireNonNull(predecessor, "predecessor name can't be null");
            if (predecessor.Equals(name)) {
                throw new TopologyException("Processor " + name + " cannot be a predecessor of itself.");
            }
            if (!nodeFactories.containsKey(predecessor)) {
                throw new TopologyException("Predecessor processor " + predecessor + " is not added yet.");
            }
            if (nodeToSinkTopic.containsKey(predecessor)) {
                throw new TopologyException("Sink " + predecessor + " cannot be used a parent.");
            }
        }

        nodeFactories.put(name, new SinkNodeFactory<>(name, predecessorNames, topicExtractor, keySerializer, valSerializer, partitioner));
        nodeGrouper.add(name);
        nodeGrouper.unite(name, predecessorNames);
        nodeGroups = null;
    }

    public void addProcessor(string name,
                                   ProcessorSupplier supplier,
                                   string... predecessorNames) {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(supplier, "supplier must not be null");
        Objects.requireNonNull(predecessorNames, "predecessor names must not be null");
        if (nodeFactories.containsKey(name)) {
            throw new TopologyException("Processor " + name + " is already added.");
        }
        if (predecessorNames.length == 0) {
            throw new TopologyException("Processor " + name + " must have at least one parent");
        }

        for (string predecessor : predecessorNames) {
            Objects.requireNonNull(predecessor, "predecessor name must not be null");
            if (predecessor.Equals(name)) {
                throw new TopologyException("Processor " + name + " cannot be a predecessor of itself.");
            }
            if (!nodeFactories.containsKey(predecessor)) {
                throw new TopologyException("Predecessor processor " + predecessor + " is not added yet for " + name);
            }
        }

        nodeFactories.put(name, new ProcessorNodeFactory(name, predecessorNames, supplier));
        nodeGrouper.add(name);
        nodeGrouper.unite(name, predecessorNames);
        nodeGroups = null;
    }

    public void addStateStore(StoreBuilder<?> storeBuilder,
                                    string... processorNames) {
        addStateStore(storeBuilder, false, processorNames);
    }

    public void addStateStore(StoreBuilder<?> storeBuilder,
                                    bool allowOverride,
                                    string... processorNames) {
        Objects.requireNonNull(storeBuilder, "storeBuilder can't be null");
        if (!allowOverride && stateFactories.containsKey(storeBuilder.name())) {
            throw new TopologyException("IStateStore " + storeBuilder.name() + " is already added.");
        }

        stateFactories.put(storeBuilder.name(), new StateStoreFactory(storeBuilder));

        if (processorNames != null) {
            for (string processorName : processorNames) {
                Objects.requireNonNull(processorName, "processor name must not be null");
                connectProcessorAndStateStore(processorName, storeBuilder.name());
            }
        }
        nodeGroups = null;
    }

    public void addGlobalStore(StoreBuilder storeBuilder,
                                     string sourceName,
                                     TimestampExtractor timestampExtractor,
                                     Deserializer keyDeserializer,
                                     Deserializer valueDeserializer,
                                     string topic,
                                     string processorName,
                                     ProcessorSupplier stateUpdateSupplier) {
        Objects.requireNonNull(storeBuilder, "store builder must not be null");
        validateGlobalStoreArguments(sourceName,
                                     topic,
                                     processorName,
                                     stateUpdateSupplier,
                                     storeBuilder.name(),
                                     storeBuilder.loggingEnabled());
        validateTopicNotAlreadyRegistered(topic);

        string[] topics = {topic};
        string[] predecessors = {sourceName};

        ProcessorNodeFactory nodeFactory = new ProcessorNodeFactory(processorName,
            predecessors,
            stateUpdateSupplier);

        globalTopics.add(topic);
        nodeFactories.put(sourceName, new SourceNodeFactory(sourceName,
            topics,
            null,
            timestampExtractor,
            keyDeserializer,
            valueDeserializer));
        nodeToSourceTopics.put(sourceName, Arrays.asList(topics));
        nodeGrouper.add(sourceName);
        nodeFactory.addStateStore(storeBuilder.name());
        nodeFactories.put(processorName, nodeFactory);
        nodeGrouper.add(processorName);
        nodeGrouper.unite(processorName, predecessors);
        globalStateBuilders.put(storeBuilder.name(), storeBuilder);
        connectSourceStoreAndTopic(storeBuilder.name(), topic);
        nodeGroups = null;
    }

    private void validateTopicNotAlreadyRegistered(string topic) {
        if (sourceTopicNames.contains(topic) || globalTopics.contains(topic)) {
            throw new TopologyException("Topic " + topic + " has already been registered by another source.");
        }

        for (Pattern pattern : nodeToSourcePatterns.values()) {
            if (pattern.matcher(topic).matches()) {
                throw new TopologyException("Topic " + topic + " matches a Pattern already registered by another source.");
            }
        }
    }

    public void connectProcessorAndStateStores(string processorName,
                                                     string... stateStoreNames) {
        Objects.requireNonNull(processorName, "processorName can't be null");
        Objects.requireNonNull(stateStoreNames, "state store list must not be null");
        if (stateStoreNames.length == 0) {
            throw new TopologyException("Must provide at least one state store name.");
        }
        for (string stateStoreName : stateStoreNames) {
            Objects.requireNonNull(stateStoreName, "state store name must not be null");
            connectProcessorAndStateStore(processorName, stateStoreName);
        }
        nodeGroups = null;
    }

    public void connectSourceStoreAndTopic(string sourceStoreName,
                                            string topic) {
        if (storeToChangelogTopic.containsKey(sourceStoreName)) {
            throw new TopologyException("Source store " + sourceStoreName + " is already added.");
        }
        storeToChangelogTopic.put(sourceStoreName, topic);
    }

    public void addInternalTopic(string topicName) {
        Objects.requireNonNull(topicName, "topicName can't be null");
        internalTopicNames.add(topicName);
    }

    public void copartitionSources(Collection<string> sourceNodes) {
        copartitionSourceGroups.add(Collections.unmodifiableSet(new HashSet<>(sourceNodes)));
    }

    private void validateGlobalStoreArguments(string sourceName,
                                              string topic,
                                              string processorName,
                                              ProcessorSupplier stateUpdateSupplier,
                                              string storeName,
                                              bool loggingEnabled) {
        Objects.requireNonNull(sourceName, "sourceName must not be null");
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(stateUpdateSupplier, "supplier must not be null");
        Objects.requireNonNull(processorName, "processorName must not be null");
        if (nodeFactories.containsKey(sourceName)) {
            throw new TopologyException("Processor " + sourceName + " is already added.");
        }
        if (nodeFactories.containsKey(processorName)) {
            throw new TopologyException("Processor " + processorName + " is already added.");
        }
        if (stateFactories.containsKey(storeName) || globalStateBuilders.containsKey(storeName)) {
            throw new TopologyException("IStateStore " + storeName + " is already added.");
        }
        if (loggingEnabled) {
            throw new TopologyException("IStateStore " + storeName + " for global table must not have logging enabled.");
        }
        if (sourceName.Equals(processorName)) {
            throw new TopologyException("sourceName and processorName must be different.");
        }
    }

    private void connectProcessorAndStateStore(string processorName,
                                               string stateStoreName) {
        if (globalStateBuilders.containsKey(stateStoreName)) {
            throw new TopologyException("Global IStateStore " + stateStoreName +
                    " can be used by a Processor without being specified; it should not be explicitly passed.");
        }
        if (!stateFactories.containsKey(stateStoreName)) {
            throw new TopologyException("IStateStore " + stateStoreName + " is not added yet.");
        }
        if (!nodeFactories.containsKey(processorName)) {
            throw new TopologyException("Processor " + processorName + " is not added yet.");
        }

        StateStoreFactory stateStoreFactory = stateFactories.get(stateStoreName);
        Iterator<string> iter = stateStoreFactory.users().iterator();
        if (iter.hasNext()) {
            string user = iter.next();
            nodeGrouper.unite(user, processorName);
        }
        stateStoreFactory.users().add(processorName);

        NodeFactory nodeFactory = nodeFactories.get(processorName);
        if (nodeFactory is ProcessorNodeFactory) {
            ProcessorNodeFactory processorNodeFactory = (ProcessorNodeFactory) nodeFactory;
            processorNodeFactory.addStateStore(stateStoreName);
            connectStateStoreNameToSourceTopicsOrPattern(stateStoreName, processorNodeFactory);
        } else {
            throw new TopologyException("cannot connect a state store " + stateStoreName + " to a source node or a sink node.");
        }
    }

    private Set<SourceNodeFactory> findSourcesForProcessorPredecessors(string[] predecessors) {
        Set<SourceNodeFactory> sourceNodes = new HashSet<>();
        for (string predecessor : predecessors) {
            NodeFactory nodeFactory = nodeFactories.get(predecessor);
            if (nodeFactory is SourceNodeFactory) {
                sourceNodes.add((SourceNodeFactory) nodeFactory);
            } else if (nodeFactory is ProcessorNodeFactory) {
                sourceNodes.addAll(findSourcesForProcessorPredecessors(((ProcessorNodeFactory) nodeFactory).predecessors));
            }
        }
        return sourceNodes;
    }

    private void connectStateStoreNameToSourceTopicsOrPattern(string stateStoreName,
                                                              ProcessorNodeFactory processorNodeFactory) {
        // we should never update the mapping from state store names to source topics if the store name already exists
        // in the map; this scenario is possible, for example, that a state store underlying a source KTable is
        // connecting to a join operator whose source topic is not the original KTable's source topic but an internal repartition topic.

        if (stateStoreNameToSourceTopics.containsKey(stateStoreName)
            || stateStoreNameToSourceRegex.containsKey(stateStoreName)) {
            return;
        }

        Set<string> sourceTopics = new HashSet<>();
        Set<Pattern> sourcePatterns = new HashSet<>();
        Set<SourceNodeFactory> sourceNodesForPredecessor =
            findSourcesForProcessorPredecessors(processorNodeFactory.predecessors);

        for (SourceNodeFactory sourceNodeFactory : sourceNodesForPredecessor) {
            if (sourceNodeFactory.pattern != null) {
                sourcePatterns.add(sourceNodeFactory.pattern);
            } else {
                sourceTopics.addAll(sourceNodeFactory.topics);
            }
        }

        if (!sourceTopics.isEmpty()) {
            stateStoreNameToSourceTopics.put(stateStoreName,
                    Collections.unmodifiableSet(sourceTopics));
        }

        if (!sourcePatterns.isEmpty()) {
            stateStoreNameToSourceRegex.put(stateStoreName,
                    Collections.unmodifiableSet(sourcePatterns));
        }

    }

    private <T> void maybeAddToResetList(Collection<T> earliestResets,
                                         Collection<T> latestResets,
                                         Topology.AutoOffsetReset offsetReset,
                                         T item) {
        if (offsetReset != null) {
            switch (offsetReset) {
                case EARLIEST:
                    earliestResets.add(item);
                    break;
                case LATEST:
                    latestResets.add(item);
                    break;
                default:
                    throw new TopologyException(string.format("Unrecognized reset format %s", offsetReset));
            }
        }
    }

    public synchronized Dictionary<Integer, Set<string>> nodeGroups() {
        if (nodeGroups == null) {
            nodeGroups = makeNodeGroups();
        }
        return nodeGroups;
    }

    private Dictionary<Integer, Set<string>> makeNodeGroups() {
        Dictionary<Integer, Set<string>> nodeGroups = new LinkedHashMap<>();
        Dictionary<string, Set<string>> rootToNodeGroup = new HashMap<>();

        int nodeGroupId = 0;

        // Go through source nodes first. This makes the group id assignment easy to predict in tests
        Set<string> allSourceNodes = new HashSet<>(nodeToSourceTopics.keySet());
        allSourceNodes.addAll(nodeToSourcePatterns.keySet());

        for (string nodeName : Utils.sorted(allSourceNodes)) {
            nodeGroupId = putNodeGroupName(nodeName, nodeGroupId, nodeGroups, rootToNodeGroup);
        }

        // Go through non-source nodes
        for (string nodeName : Utils.sorted(nodeFactories.keySet())) {
            if (!nodeToSourceTopics.containsKey(nodeName)) {
                nodeGroupId = putNodeGroupName(nodeName, nodeGroupId, nodeGroups, rootToNodeGroup);
            }
        }

        return nodeGroups;
    }

    private int putNodeGroupName(string nodeName,
                                 int nodeGroupId,
                                 Dictionary<Integer, Set<string>> nodeGroups,
                                 Dictionary<string, Set<string>> rootToNodeGroup) {
        int newNodeGroupId = nodeGroupId;
        string root = nodeGrouper.root(nodeName);
        Set<string> nodeGroup = rootToNodeGroup.get(root);
        if (nodeGroup == null) {
            nodeGroup = new HashSet<>();
            rootToNodeGroup.put(root, nodeGroup);
            nodeGroups.put(newNodeGroupId++, nodeGroup);
        }
        nodeGroup.add(nodeName);
        return newNodeGroupId;
    }

    public synchronized ProcessorTopology build() {
        return build((Integer) null);
    }

    public synchronized ProcessorTopology build(Integer topicGroupId) {
        Set<string> nodeGroup;
        if (topicGroupId != null) {
            nodeGroup = nodeGroups().get(topicGroupId);
        } else {
            // when topicGroupId is null, we build the full topology minus the global groups
            Set<string> globalNodeGroups = globalNodeGroups();
            Collection<Set<string>> values = nodeGroups().values();
            nodeGroup = new HashSet<>();
            for (Set<string> value : values) {
                nodeGroup.addAll(value);
            }
            nodeGroup.removeAll(globalNodeGroups);
        }
        return build(nodeGroup);
    }

    /**
     * Builds the topology for any global state stores
     * @return ProcessorTopology
     */
    public synchronized ProcessorTopology buildGlobalStateTopology() {
        Objects.requireNonNull(applicationId, "topology has not completed optimization");

        Set<string> globalGroups = globalNodeGroups();
        if (globalGroups.isEmpty()) {
            return null;
        }
        return build(globalGroups);
    }

    private Set<string> globalNodeGroups() {
        Set<string> globalGroups = new HashSet<>();
        for (Map.Entry<Integer, Set<string>> nodeGroup : nodeGroups().entrySet()) {
            Set<string> nodes = nodeGroup.getValue();
            for (string node : nodes) {
                if (isGlobalSource(node)) {
                    globalGroups.addAll(nodes);
                }
            }
        }
        return globalGroups;
    }

    private ProcessorTopology build(Set<string> nodeGroup) {
        Objects.requireNonNull(applicationId, "topology has not completed optimization");

        Dictionary<string, ProcessorNode> processorMap = new LinkedHashMap<>();
        Dictionary<string, SourceNode> topicSourceMap = new HashMap<>();
        Dictionary<string, SinkNode> topicSinkMap = new HashMap<>();
        Dictionary<string, IStateStore> stateStoreMap = new LinkedHashMap<>();
        Set<string> repartitionTopics = new HashSet<>();

        // create processor nodes in a topological order ("nodeFactories" is already topologically sorted)
        // also make sure the state store map values following the insertion ordering
        for (NodeFactory factory : nodeFactories.values()) {
            if (nodeGroup == null || nodeGroup.contains(factory.name)) {
                ProcessorNode node = factory.build();
                processorMap.put(node.name(), node);

                if (factory is ProcessorNodeFactory) {
                    buildProcessorNode(processorMap,
                                       stateStoreMap,
                                       (ProcessorNodeFactory) factory,
                                       node);

                } else if (factory is SourceNodeFactory) {
                    buildSourceNode(topicSourceMap,
                                    repartitionTopics,
                                    (SourceNodeFactory) factory,
                                    (SourceNode) node);

                } else if (factory is SinkNodeFactory) {
                    buildSinkNode(processorMap,
                                  topicSinkMap,
                                  repartitionTopics,
                                  (SinkNodeFactory) factory,
                                  (SinkNode) node);
                } else {
                    throw new TopologyException("Unknown definition class: " + factory.GetType().getName());
                }
            }
        }

        return new ProcessorTopology(new ArrayList<>(processorMap.values()),
                                     topicSourceMap,
                                     topicSinkMap,
                                     new ArrayList<>(stateStoreMap.values()),
                                     new ArrayList<>(globalStateStores.values()),
                                     storeToChangelogTopic,
                                     repartitionTopics);
    }

    @SuppressWarnings("unchecked")
    private void buildSinkNode(Dictionary<string, ProcessorNode> processorMap,
                               Dictionary<string, SinkNode> topicSinkMap,
                               Set<string> repartitionTopics,
                               SinkNodeFactory sinkNodeFactory,
                               SinkNode node) {

        for (string predecessor : sinkNodeFactory.predecessors) {
            processorMap.get(predecessor).addChild(node);
            if (sinkNodeFactory.topicExtractor is StaticTopicNameExtractor) {
                string topic = ((StaticTopicNameExtractor) sinkNodeFactory.topicExtractor).topicName;

                if (internalTopicNames.contains(topic)) {
                    // prefix the internal topic name with the application id
                    string decoratedTopic = decorateTopic(topic);
                    topicSinkMap.put(decoratedTopic, node);
                    repartitionTopics.add(decoratedTopic);
                } else {
                    topicSinkMap.put(topic, node);
                }

            }
        }
    }

    private void buildSourceNode(Dictionary<string, SourceNode> topicSourceMap,
                                 Set<string> repartitionTopics,
                                 SourceNodeFactory sourceNodeFactory,
                                 SourceNode node) {

        List<string> topics = (sourceNodeFactory.pattern != null) ?
                                    sourceNodeFactory.getTopics(subscriptionUpdates.getUpdates()) :
                                    sourceNodeFactory.topics;

        for (string topic : topics) {
            if (internalTopicNames.contains(topic)) {
                // prefix the internal topic name with the application id
                string decoratedTopic = decorateTopic(topic);
                topicSourceMap.put(decoratedTopic, node);
                repartitionTopics.add(decoratedTopic);
            } else {
                topicSourceMap.put(topic, node);
            }
        }
    }

    private void buildProcessorNode(Dictionary<string, ProcessorNode> processorMap,
                                    Dictionary<string, IStateStore> stateStoreMap,
                                    ProcessorNodeFactory factory,
                                    ProcessorNode node) {

        for (string predecessor : factory.predecessors) {
            ProcessorNode<?, ?> predecessorNode = processorMap.get(predecessor);
            predecessorNode.addChild(node);
        }
        for (string stateStoreName : factory.stateStoreNames) {
            if (!stateStoreMap.containsKey(stateStoreName)) {
                if (stateFactories.containsKey(stateStoreName)) {
                    StateStoreFactory stateStoreFactory = stateFactories.get(stateStoreName);

                    // remember the changelog topic if this state store is change-logging enabled
                    if (stateStoreFactory.loggingEnabled() && !storeToChangelogTopic.containsKey(stateStoreName)) {
                        string changelogTopic = ProcessorStateManager.storeChangelogTopic(applicationId, stateStoreName);
                        storeToChangelogTopic.put(stateStoreName, changelogTopic);
                    }
                    stateStoreMap.put(stateStoreName, stateStoreFactory.build());
                } else {
                    stateStoreMap.put(stateStoreName, globalStateStores.get(stateStoreName));
                }
            }
        }
    }

    /**
     * Get any global {@link IStateStore}s that are part of the
     * topology
     * @return map containing all global {@link IStateStore}s
     */
    public Dictionary<string, IStateStore> globalStateStores() {
        Objects.requireNonNull(applicationId, "topology has not completed optimization");

        return Collections.unmodifiableMap(globalStateStores);
    }

    public Set<string> allStateStoreName() {
        Objects.requireNonNull(applicationId, "topology has not completed optimization");

        Set<string> allNames = new HashSet<>(stateFactories.keySet());
        allNames.addAll(globalStateStores.keySet());
        return Collections.unmodifiableSet(allNames);
    }

    /**
     * Returns the map of topic groups keyed by the group id.
     * A topic group is a group of topics in the same task.
     *
     * @return groups of topic names
     */
    public synchronized Dictionary<Integer, TopicsInfo> topicGroups() {
        Dictionary<Integer, TopicsInfo> topicGroups = new LinkedHashMap<>();

        if (nodeGroups == null) {
            nodeGroups = makeNodeGroups();
        }

        for (Map.Entry<Integer, Set<string>> entry : nodeGroups.entrySet()) {
            Set<string> sinkTopics = new HashSet<>();
            Set<string> sourceTopics = new HashSet<>();
            Dictionary<string, InternalTopicConfig> repartitionTopics = new HashMap<>();
            Dictionary<string, InternalTopicConfig> stateChangelogTopics = new HashMap<>();
            for (string node : entry.getValue()) {
                // if the node is a source node, add to the source topics
                List<string> topics = nodeToSourceTopics.get(node);
                if (topics != null) {
                    // if some of the topics are internal, add them to the internal topics
                    for (string topic : topics) {
                        // skip global topic as they don't need partition assignment
                        if (globalTopics.contains(topic)) {
                            continue;
                        }
                        if (internalTopicNames.contains(topic)) {
                            // prefix the internal topic name with the application id
                            string internalTopic = decorateTopic(topic);
                            repartitionTopics.put(
                                internalTopic,
                                new RepartitionTopicConfig(internalTopic, Collections.emptyMap()));
                            sourceTopics.add(internalTopic);
                        } else {
                            sourceTopics.add(topic);
                        }
                    }
                }

                // if the node is a sink node, add to the sink topics
                string topic = nodeToSinkTopic.get(node);
                if (topic != null) {
                    if (internalTopicNames.contains(topic)) {
                        // prefix the change log topic name with the application id
                        sinkTopics.add(decorateTopic(topic));
                    } else {
                        sinkTopics.add(topic);
                    }
                }

                // if the node is connected to a state store whose changelog topics are not predefined,
                // add to the changelog topics
                for (StateStoreFactory stateFactory : stateFactories.values()) {
                    if (stateFactory.loggingEnabled() && stateFactory.users().contains(node)) {
                        string topicName = storeToChangelogTopic.containsKey(stateFactory.name()) ?
                                storeToChangelogTopic.get(stateFactory.name()) :
                                ProcessorStateManager.storeChangelogTopic(applicationId, stateFactory.name());
                        if (!stateChangelogTopics.containsKey(topicName)) {
                            InternalTopicConfig internalTopicConfig =
                                createChangelogTopicConfig(stateFactory, topicName);
                            stateChangelogTopics.put(topicName, internalTopicConfig);
                        }
                    }
                }
            }
            if (!sourceTopics.isEmpty()) {
                topicGroups.put(entry.getKey(), new TopicsInfo(
                        Collections.unmodifiableSet(sinkTopics),
                        Collections.unmodifiableSet(sourceTopics),
                        Collections.unmodifiableMap(repartitionTopics),
                        Collections.unmodifiableMap(stateChangelogTopics)));
            }
        }

        return Collections.unmodifiableMap(topicGroups);
    }

    private void setRegexMatchedTopicsToSourceNodes() {
        if (subscriptionUpdates.hasUpdates()) {
            for (Map.Entry<string, Pattern> stringPatternEntry : nodeToSourcePatterns.entrySet()) {
                SourceNodeFactory sourceNode =
                    (SourceNodeFactory) nodeFactories.get(stringPatternEntry.getKey());
                //need to update nodeToSourceTopics with topics matched from given regex
                nodeToSourceTopics.put(
                    stringPatternEntry.getKey(),
                    sourceNode.getTopics(subscriptionUpdates.getUpdates()));
                log.debug("nodeToSourceTopics {}", nodeToSourceTopics);
            }
        }
    }

    private void setRegexMatchedTopicToStateStore() {
        if (subscriptionUpdates.hasUpdates()) {
            for (Map.Entry<string, Set<Pattern>> storePattern : stateStoreNameToSourceRegex.entrySet()) {
                Set<string> updatedTopicsForStateStore = new HashSet<>();
                for (string subscriptionUpdateTopic : subscriptionUpdates.getUpdates()) {
                    for (Pattern pattern : storePattern.getValue()) {
                        if (pattern.matcher(subscriptionUpdateTopic).matches()) {
                            updatedTopicsForStateStore.add(subscriptionUpdateTopic);
                        }
                    }
                }
                if (!updatedTopicsForStateStore.isEmpty()) {
                    Collection<string> storeTopics = stateStoreNameToSourceTopics.get(storePattern.getKey());
                    if (storeTopics != null) {
                        updatedTopicsForStateStore.addAll(storeTopics);
                    }
                    stateStoreNameToSourceTopics.put(
                        storePattern.getKey(),
                        Collections.unmodifiableSet(updatedTopicsForStateStore));
                }
            }
        }
    }

    private InternalTopicConfig createChangelogTopicConfig(StateStoreFactory factory,
                                                           string name) {
        if (factory.isWindowStore()) {
            WindowedChangelogTopicConfig config = new WindowedChangelogTopicConfig(name, factory.logConfig());
            config.setRetentionMs(factory.retentionPeriod());
            return config;
        } else {
            return new UnwindowedChangelogTopicConfig(name, factory.logConfig());
        }
    }

    public synchronized Pattern earliestResetTopicsPattern() {
        return resetTopicsPattern(earliestResetTopics, earliestResetPatterns);
    }

    public synchronized Pattern latestResetTopicsPattern() {
        return resetTopicsPattern(latestResetTopics, latestResetPatterns);
    }

    private Pattern resetTopicsPattern(Set<string> resetTopics,
                                       Set<Pattern> resetPatterns) {
        List<string> topics = maybeDecorateInternalSourceTopics(resetTopics);

        return buildPatternForOffsetResetTopics(topics, resetPatterns);
    }

    private static Pattern buildPatternForOffsetResetTopics(Collection<string> sourceTopics,
                                                            Collection<Pattern> sourcePatterns) {
        StringBuilder builder = new StringBuilder();

        for (string topic : sourceTopics) {
            builder.append(topic).append("|");
        }

        for (Pattern sourcePattern : sourcePatterns) {
            builder.append(sourcePattern.pattern()).append("|");
        }

        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
            return Pattern.compile(builder.toString());
        }

        return EMPTY_ZERO_LENGTH_PATTERN;
    }

    public Dictionary<string, List<string>> stateStoreNameToSourceTopics() {
        Dictionary<string, List<string>> results = new HashMap<>();
        for (Map.Entry<string, Set<string>> entry : stateStoreNameToSourceTopics.entrySet()) {
            results.put(entry.getKey(), maybeDecorateInternalSourceTopics(entry.getValue()));
        }
        return results;
    }

    public synchronized Collection<Set<string>> copartitionGroups() {
        List<Set<string>> list = new ArrayList<>(copartitionSourceGroups.size());
        for (Set<string> nodeNames : copartitionSourceGroups) {
            Set<string> copartitionGroup = new HashSet<>();
            for (string node : nodeNames) {
                List<string> topics = nodeToSourceTopics.get(node);
                if (topics != null) {
                    copartitionGroup.addAll(maybeDecorateInternalSourceTopics(topics));
                }
            }
            list.add(Collections.unmodifiableSet(copartitionGroup));
        }
        return Collections.unmodifiableList(list);
    }

    private List<string> maybeDecorateInternalSourceTopics(Collection<string> sourceTopics) {
        List<string> decoratedTopics = new ArrayList<>();
        for (string topic : sourceTopics) {
            if (internalTopicNames.contains(topic)) {
                decoratedTopics.add(decorateTopic(topic));
            } else {
                decoratedTopics.add(topic);
            }
        }
        return decoratedTopics;
    }

    private string decorateTopic(string topic) {
        if (applicationId == null) {
            throw new TopologyException("there are internal topics and "
                    + "applicationId hasn't been set. Call "
                    + "setApplicationId first");
        }

        return applicationId + "-" + topic;
    }

    SubscriptionUpdates subscriptionUpdates() {
        return subscriptionUpdates;
    }

    synchronized Pattern sourceTopicPattern() {
        if (topicPattern == null) {
            List<string> allSourceTopics = new ArrayList<>();
            if (!nodeToSourceTopics.isEmpty()) {
                for (List<string> topics : nodeToSourceTopics.values()) {
                    allSourceTopics.addAll(maybeDecorateInternalSourceTopics(topics));
                }
            }
            Collections.sort(allSourceTopics);

            topicPattern = buildPatternForOffsetResetTopics(allSourceTopics, nodeToSourcePatterns.values());
        }

        return topicPattern;
    }

    // package-private for testing only
    synchronized void updateSubscriptions(SubscriptionUpdates subscriptionUpdates,
                                          string logPrefix) {
        log.debug("{}updating builder with {} topic(s) with possible matching regex subscription(s)",
                logPrefix, subscriptionUpdates);
        this.subscriptionUpdates = subscriptionUpdates;
        setRegexMatchedTopicsToSourceNodes();
        setRegexMatchedTopicToStateStore();
    }

    private bool isGlobalSource(string nodeName) {
        NodeFactory nodeFactory = nodeFactories.get(nodeName);

        if (nodeFactory is SourceNodeFactory) {
            List<string> topics = ((SourceNodeFactory) nodeFactory).topics;
            return topics != null && topics.size() == 1 && globalTopics.contains(topics.get(0));
        }
        return false;
    }

    public TopologyDescription describe() {
        TopologyDescription description = new TopologyDescription();

        for (Map.Entry<Integer, Set<string>> nodeGroup : makeNodeGroups().entrySet()) {

            Set<string> allNodesOfGroups = nodeGroup.getValue();
            bool isNodeGroupOfGlobalStores = nodeGroupContainsGlobalSourceNode(allNodesOfGroups);

            if (!isNodeGroupOfGlobalStores) {
                describeSubtopology(description, nodeGroup.getKey(), allNodesOfGroups);
            } else {
                describeGlobalStore(description, allNodesOfGroups, nodeGroup.getKey());
            }
        }

        return description;
    }

    private void describeGlobalStore(TopologyDescription description,
                                     Set<string> nodes,
                                     int id) {
        Iterator<string> it = nodes.iterator();
        while (it.hasNext()) {
            string node = it.next();

            if (isGlobalSource(node)) {
                // we found a GlobalStore node group; those contain exactly two node: {sourceNode,processorNode}
                it.remove(); // remove sourceNode from group
                string processorNode = nodes.iterator().next(); // get remaining processorNode

                description.addGlobalStore(new GlobalStore(
                    node,
                    processorNode,
                    ((ProcessorNodeFactory) nodeFactories.get(processorNode)).stateStoreNames.iterator().next(),
                    nodeToSourceTopics.get(node).get(0),
                    id
                ));
                break;
            }
        }
    }

    private bool nodeGroupContainsGlobalSourceNode(Set<string> allNodesOfGroups) {
        for (string node : allNodesOfGroups) {
            if (isGlobalSource(node)) {
                return true;
            }
        }
        return false;
    }

    private static class NodeComparator implements Comparator<TopologyDescription.Node>, Serializable {

        @Override
        public int compare(TopologyDescription.Node node1,
                           TopologyDescription.Node node2) {
            if (node1.Equals(node2)) {
                return 0;
            }
            int size1 = ((AbstractNode) node1).size;
            int size2 = ((AbstractNode) node2).size;

            // it is possible that two nodes have the same sub-tree size (think two nodes connected via state stores)
            // in this case default to processor name string
            if (size1 != size2) {
                return size2 - size1;
            } else {
                return node1.name().compareTo(node2.name());
            }
        }
    }

    private static NodeComparator NODE_COMPARATOR = new NodeComparator();

    private static void updateSize(AbstractNode node,
                                   int delta) {
        node.size += delta;

        for (TopologyDescription.Node predecessor : node.predecessors()) {
            updateSize((AbstractNode) predecessor, delta);
        }
    }

    private void describeSubtopology(TopologyDescription description,
                                     Integer subtopologyId,
                                     Set<string> nodeNames) {

        Dictionary<string, AbstractNode> nodesByName = new HashMap<>();

        // add all nodes
        for (string nodeName : nodeNames) {
            nodesByName.put(nodeName, nodeFactories.get(nodeName).describe());
        }

        // connect each node to its predecessors and successors
        for (AbstractNode node : nodesByName.values()) {
            for (string predecessorName : nodeFactories.get(node.name()).predecessors) {
                AbstractNode predecessor = nodesByName.get(predecessorName);
                node.addPredecessor(predecessor);
                predecessor.addSuccessor(node);
                updateSize(predecessor, node.size);
            }
        }

        description.addSubtopology(new Subtopology(
                subtopologyId,
                new HashSet<>(nodesByName.values())));
    }

    public static class GlobalStore implements TopologyDescription.GlobalStore {
        private Source source;
        private Processor processor;
        private int id;

        public GlobalStore(string sourceName,
                           string processorName,
                           string storeName,
                           string topicName,
                           int id) {
            source = new Source(sourceName, Collections.singleton(topicName), null);
            processor = new Processor(processorName, Collections.singleton(storeName));
            source.successors.add(processor);
            processor.predecessors.add(source);
            this.id = id;
        }

        @Override
        public int id() {
            return id;
        }

        @Override
        public TopologyDescription.Source source() {
            return source;
        }

        @Override
        public TopologyDescription.Processor processor() {
            return processor;
        }

        @Override
        public string toString() {
            return "Sub-topology: " + id + " for global store (will not generate tasks)\n"
                    + "    " + source.toString() + "\n"
                    + "    " + processor.toString() + "\n";
        }

        @Override
        public bool equals(object o) {
            if (this == o) {
                return true;
            }
            if (o == null || GetType() != o.GetType()) {
                return false;
            }

            GlobalStore that = (GlobalStore) o;
            return source.Equals(that.source)
                && processor.Equals(that.processor);
        }

        @Override
        public int GetHashCode()() {
            return Objects.hash(source, processor);
        }
    }

    public abstract static class AbstractNode implements TopologyDescription.Node {
        string name;
        Set<TopologyDescription.Node> predecessors = new TreeSet<>(NODE_COMPARATOR);
        Set<TopologyDescription.Node> successors = new TreeSet<>(NODE_COMPARATOR);

        // size of the sub-topology rooted at this node, including the node itself
        int size;

        AbstractNode(string name) {
            Objects.requireNonNull(name, "name cannot be null");
            this.name = name;
            this.size = 1;
        }

        @Override
        public string name() {
            return name;
        }

        @Override
        public Set<TopologyDescription.Node> predecessors() {
            return Collections.unmodifiableSet(predecessors);
        }

        @Override
        public Set<TopologyDescription.Node> successors() {
            return Collections.unmodifiableSet(successors);
        }

        public void addPredecessor(TopologyDescription.Node predecessor) {
            predecessors.add(predecessor);
        }

        public void addSuccessor(TopologyDescription.Node successor) {
            successors.add(successor);
        }
    }

    public static class Source : AbstractNode implements TopologyDescription.Source {
        private Set<string> topics;
        private Pattern topicPattern;

        public Source(string name,
                      Set<string> topics,
                      Pattern pattern) {
            super(name);
            if (topics == null && pattern == null) {
                throw new IllegalArgumentException("Either topics or pattern must be not-null, but both are null.");
            }
            if (topics != null && pattern != null) {
                throw new IllegalArgumentException("Either topics or pattern must be null, but both are not null.");
            }

            this.topics = topics;
            this.topicPattern = pattern;
        }

        @Deprecated
        @Override
        public string topics() {
            return topics.toString();
        }

        @Override
        public Set<string> topicSet() {
            return topics;
        }

        @Override
        public Pattern topicPattern() {
            return topicPattern;
        }

        @Override
        public void addPredecessor(TopologyDescription.Node predecessor) {
            throw new UnsupportedOperationException("Sources don't have predecessors.");
        }

        @Override
        public string toString() {
            string topicsString = topics == null ? topicPattern.toString() : topics.toString();
            
            return "Source: " + name + " (topics: " + topicsString + ")\n      --> " + nodeNames(successors);
        }

        @Override
        public bool equals(object o) {
            if (this == o) {
                return true;
            }
            if (o == null || GetType() != o.GetType()) {
                return false;
            }

            Source source = (Source) o;
            // omit successor to avoid infinite loops
            return name.Equals(source.name)
                && Objects.Equals(topics, source.topics)
                && (topicPattern == null ? source.topicPattern == null :
                    topicPattern.pattern().Equals(source.topicPattern.pattern()));
        }

        @Override
        public int GetHashCode()() {
            // omit successor as it might change and alter the hash code
            return Objects.hash(name, topics, topicPattern);
        }
    }

    public static class Processor : AbstractNode implements TopologyDescription.Processor {
        private Set<string> stores;

        public Processor(string name,
                         Set<string> stores) {
            super(name);
            this.stores = stores;
        }

        @Override
        public Set<string> stores() {
            return Collections.unmodifiableSet(stores);
        }

        @Override
        public string toString() {
            return "Processor: " + name + " (stores: " + stores + ")\n      --> "
                + nodeNames(successors) + "\n      <-- " + nodeNames(predecessors);
        }

        @Override
        public bool equals(object o) {
            if (this == o) {
                return true;
            }
            if (o == null || GetType() != o.GetType()) {
                return false;
            }

            Processor processor = (Processor) o;
            // omit successor to avoid infinite loops
            return name.Equals(processor.name)
                && stores.Equals(processor.stores)
                && predecessors.Equals(processor.predecessors);
        }

        @Override
        public int GetHashCode()() {
            // omit successor as it might change and alter the hash code
            return Objects.hash(name, stores);
        }
    }

    public static class Sink : AbstractNode implements TopologyDescription.Sink {
        private TopicNameExtractor topicNameExtractor;

        public Sink(string name,
                    TopicNameExtractor topicNameExtractor) {
            super(name);
            this.topicNameExtractor = topicNameExtractor;
        }

        public Sink(string name,
                    string topic) {
            super(name);
            this.topicNameExtractor = new StaticTopicNameExtractor(topic);
        }

        @Override
        public string topic() {
            if (topicNameExtractor is StaticTopicNameExtractor) {
                return ((StaticTopicNameExtractor) topicNameExtractor).topicName;
            } else {
                return null;
            }
        }

        @Override
        public TopicNameExtractor topicNameExtractor() {
            if (topicNameExtractor is StaticTopicNameExtractor) {
                return null;
            } else {
                return topicNameExtractor;
            }
        }

        @Override
        public void addSuccessor(TopologyDescription.Node successor) {
            throw new UnsupportedOperationException("Sinks don't have successors.");
        }

        @Override
        public string toString() {
            if (topicNameExtractor is StaticTopicNameExtractor) {
                return "Sink: " + name + " (topic: " + topic() + ")\n      <-- " + nodeNames(predecessors);
            }
            return "Sink: " + name + " (extractor class: " + topicNameExtractor + ")\n      <-- "
                + nodeNames(predecessors);
        }

        @Override
        public bool equals(object o) {
            if (this == o) {
                return true;
            }
            if (o == null || GetType() != o.GetType()) {
                return false;
            }

            Sink sink = (Sink) o;
            return name.Equals(sink.name)
                && topicNameExtractor.Equals(sink.topicNameExtractor)
                && predecessors.Equals(sink.predecessors);
        }

        @Override
        public int GetHashCode()() {
            // omit predecessors as it might change and alter the hash code
            return Objects.hash(name, topicNameExtractor);
        }
    }

    public static class Subtopology implements org.apache.kafka.streams.TopologyDescription.Subtopology {
        private int id;
        private Set<TopologyDescription.Node> nodes;

        public Subtopology(int id, Set<TopologyDescription.Node> nodes) {
            this.id = id;
            this.nodes = new TreeSet<>(NODE_COMPARATOR);
            this.nodes.addAll(nodes);
        }

        @Override
        public int id() {
            return id;
        }

        @Override
        public Set<TopologyDescription.Node> nodes() {
            return Collections.unmodifiableSet(nodes);
        }

        // visible for testing
        Iterator<TopologyDescription.Node> nodesInOrder() {
            return nodes.iterator();
        }

        @Override
        public string toString() {
            return "Sub-topology: " + id + "\n" + nodesAsString() + "\n";
        }

        private string nodesAsString() {
            StringBuilder sb = new StringBuilder();
            for (TopologyDescription.Node node : nodes) {
                sb.append("    ");
                sb.append(node);
                sb.append('\n');
            }
            return sb.toString();
        }

        @Override
        public bool equals(object o) {
            if (this == o) {
                return true;
            }
            if (o == null || GetType() != o.GetType()) {
                return false;
            }

            Subtopology that = (Subtopology) o;
            return id == that.id
                && nodes.Equals(that.nodes);
        }

        @Override
        public int GetHashCode()() {
            return Objects.hash(id, nodes);
        }
    }

    public static class TopicsInfo {
        Set<string> sinkTopics;
        Set<string> sourceTopics;
        public Dictionary<string, InternalTopicConfig> stateChangelogTopics;
        public Dictionary<string, InternalTopicConfig> repartitionSourceTopics;

        TopicsInfo(Set<string> sinkTopics,
                   Set<string> sourceTopics,
                   Dictionary<string, InternalTopicConfig> repartitionSourceTopics,
                   Dictionary<string, InternalTopicConfig> stateChangelogTopics) {
            this.sinkTopics = sinkTopics;
            this.sourceTopics = sourceTopics;
            this.stateChangelogTopics = stateChangelogTopics;
            this.repartitionSourceTopics = repartitionSourceTopics;
        }

        @Override
        public bool equals(object o) {
            if (o is TopicsInfo) {
                TopicsInfo other = (TopicsInfo) o;
                return other.sourceTopics.Equals(sourceTopics) && other.stateChangelogTopics.Equals(stateChangelogTopics);
            } else {
                return false;
            }
        }

        @Override
        public int GetHashCode()() {
            long n = ((long) sourceTopics.GetHashCode()() << 32) | (long) stateChangelogTopics.GetHashCode()();
            return (int) (n % 0xFFFFFFFFL);
        }

        @Override
        public string toString() {
            return "TopicsInfo{" +
                "sinkTopics=" + sinkTopics +
                ", sourceTopics=" + sourceTopics +
                ", repartitionSourceTopics=" + repartitionSourceTopics +
                ", stateChangelogTopics=" + stateChangelogTopics +
                '}';
        }
    }

    private static class GlobalStoreComparator implements Comparator<TopologyDescription.GlobalStore>, Serializable {
        @Override
        public int compare(TopologyDescription.GlobalStore globalStore1,
                           TopologyDescription.GlobalStore globalStore2) {
            if (globalStore1.Equals(globalStore2)) {
                return 0;
            }
            return globalStore1.id() - globalStore2.id();
        }
    }

    private static GlobalStoreComparator GLOBALSTORE_COMPARATOR = new GlobalStoreComparator();

    private static class SubtopologyComparator implements Comparator<TopologyDescription.Subtopology>, Serializable {
        @Override
        public int compare(TopologyDescription.Subtopology subtopology1,
                           TopologyDescription.Subtopology subtopology2) {
            if (subtopology1.Equals(subtopology2)) {
                return 0;
            }
            return subtopology1.id() - subtopology2.id();
        }
    }

    private static SubtopologyComparator SUBTOPOLOGY_COMPARATOR = new SubtopologyComparator();

    public static class TopologyDescription implements org.apache.kafka.streams.TopologyDescription {
        private TreeSet<TopologyDescription.Subtopology> subtopologies = new TreeSet<>(SUBTOPOLOGY_COMPARATOR);
        private TreeSet<TopologyDescription.GlobalStore> globalStores = new TreeSet<>(GLOBALSTORE_COMPARATOR);

        public void addSubtopology(TopologyDescription.Subtopology subtopology) {
            subtopologies.add(subtopology);
        }

        public void addGlobalStore(TopologyDescription.GlobalStore globalStore) {
            globalStores.add(globalStore);
        }

        @Override
        public Set<TopologyDescription.Subtopology> subtopologies() {
            return Collections.unmodifiableSet(subtopologies);
        }

        @Override
        public Set<TopologyDescription.GlobalStore> globalStores() {
            return Collections.unmodifiableSet(globalStores);
        }

        @Override
        public string toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Topologies:\n ");
            TopologyDescription.Subtopology[] sortedSubtopologies =
                subtopologies.descendingSet().toArray(new Subtopology[0]);
            TopologyDescription.GlobalStore[] sortedGlobalStores =
                globalStores.descendingSet().toArray(new GlobalStore[0]);
            int expectedId = 0;
            int subtopologiesIndex = sortedSubtopologies.length - 1;
            int globalStoresIndex = sortedGlobalStores.length - 1;
            while (subtopologiesIndex != -1 && globalStoresIndex != -1) {
                sb.append("  ");
                TopologyDescription.Subtopology subtopology = sortedSubtopologies[subtopologiesIndex];
                TopologyDescription.GlobalStore globalStore = sortedGlobalStores[globalStoresIndex];
                if (subtopology.id() == expectedId) {
                    sb.append(subtopology);
                    subtopologiesIndex--;
                } else {
                    sb.append(globalStore);
                    globalStoresIndex--;
                }
                expectedId++;
            }
            while (subtopologiesIndex != -1) {
                TopologyDescription.Subtopology subtopology = sortedSubtopologies[subtopologiesIndex];
                sb.append("  ");
                sb.append(subtopology);
                subtopologiesIndex--;
            }
            while (globalStoresIndex != -1) {
                TopologyDescription.GlobalStore globalStore = sortedGlobalStores[globalStoresIndex];
                sb.append("  ");
                sb.append(globalStore);
                globalStoresIndex--;
            }
            return sb.toString();
        }

        @Override
        public bool equals(object o) {
            if (this == o) {
                return true;
            }
            if (o == null || GetType() != o.GetType()) {
                return false;
            }

            TopologyDescription that = (TopologyDescription) o;
            return subtopologies.Equals(that.subtopologies)
                && globalStores.Equals(that.globalStores);
        }

        @Override
        public int GetHashCode()() {
            return Objects.hash(subtopologies, globalStores);
        }

    }

    private static string nodeNames(Set<TopologyDescription.Node> nodes) {
        StringBuilder sb = new StringBuilder();
        if (!nodes.isEmpty()) {
            for (TopologyDescription.Node n : nodes) {
                sb.append(n.name());
                sb.append(", ");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.deleteCharAt(sb.length() - 1);
        } else {
            return "none";
        }
        return sb.toString();
    }

    /**
     * Used to capture subscribed topic via Patterns discovered during the
     * partition assignment process.
     */
    public static class SubscriptionUpdates {

        private Set<string> updatedTopicSubscriptions = new HashSet<>();

        private void updateTopics(Collection<string> topicNames) {
            updatedTopicSubscriptions.clear();
            updatedTopicSubscriptions.addAll(topicNames);
        }

        public Collection<string> getUpdates() {
            return Collections.unmodifiableSet(updatedTopicSubscriptions);
        }

        bool hasUpdates() {
            return !updatedTopicSubscriptions.isEmpty();
        }

        @Override
        public string toString() {
            return string.format("SubscriptionUpdates{updatedTopicSubscriptions=%s}", updatedTopicSubscriptions);
        }
    }

    void updateSubscribedTopics(Set<string> topics,
                                string logPrefix) {
        SubscriptionUpdates subscriptionUpdates = new SubscriptionUpdates();
        log.debug("{}found {} topics possibly matching regex", logPrefix, topics);
        // update the topic groups with the returned subscription set for regex pattern subscriptions
        subscriptionUpdates.updateTopics(topics);
        updateSubscriptions(subscriptionUpdates, logPrefix);
    }


    // following functions are for test only

    public synchronized Set<string> getSourceTopicNames() {
        return sourceTopicNames;
    }

    public synchronized Dictionary<string, StateStoreFactory> getStateStores() {
        return stateFactories;
    }
}
