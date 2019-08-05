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
using Kafka.Common.serialization.Deserializer;
using Kafka.Common.serialization.Serializer;
using Kafka.Common.Utils.Utils;
using Kafka.Streams.Processor.Interfaces;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Kafka.Streams.Processor.Internals
{
    public class InternalTopologyBuilder
    {
        private static Logger log = new LoggerFactory().CreateLogger < InternalTopologyBuilder);
        private static Pattern EMPTY_ZERO_LENGTH_PATTERN = Pattern.compile("");
        private static string[] NO_PREDECESSORS = { };

        // node factories in a topological order
        private Dictionary<string, NodeFactory> nodeFactories = new LinkedHashMap<>();

        // state factories
        private Dictionary<string, StateStoreFactory> stateFactories = new HashMap<>();

        // built global state stores
        private Dictionary<string, StoreBuilder> globalStateBuilders = new LinkedHashMap<>();

        // built global state stores
        private Dictionary<string, IStateStore> globalStateStores = new LinkedHashMap<>();

        // all topics subscribed from source processors (without application-id prefix for internal topics)
        private HashSet<string> sourceTopicNames = new HashSet<>();

        // all internal topics auto-created by the topology builder and used in source / sink processors
        private HashSet<string> internalTopicNames = new HashSet<>();

        // groups of source processors that need to be copartitioned
        private List<Set<string>> copartitionSourceGroups = new List<>();

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
        private Dictionary<string, HashSet<string>> stateStoreNameToSourceTopics = new HashMap<>();

        // map from state store names to all the regex subscribed topics from source processors that
        // are connected to these state stores
        private Dictionary<string, HashSet<Pattern>> stateStoreNameToSourceRegex = new HashMap<>();

        // map from state store names to this state store's corresponding changelog topic if possible
        private Dictionary<string, string> storeToChangelogTopic = new HashMap<>();

        // all global topics
        private HashSet<string> globalTopics = new HashSet<>();

        private HashSet<string> earliestResetTopics = new HashSet<>();

        private HashSet<string> latestResetTopics = new HashSet<>();

        private HashSet<Pattern> earliestResetPatterns = new HashSet<>();

        private HashSet<Pattern> latestResetPatterns = new HashSet<>();

        private QuickUnion<string> nodeGrouper = new QuickUnion<>();

        private SubscriptionUpdates subscriptionUpdates = new SubscriptionUpdates();

        private string applicationId = null;

        private Pattern topicPattern = null;

        private Dictionary<int, HashSet<string>> nodeGroups = null;

        public static class StateStoreFactory
        {

            private StoreBuilder builder;
            private HashSet<string> users = new HashSet<>();

            private StateStoreFactory(StoreBuilder<?> builder)
            {
                this.builder = builder;
            }

            public IStateStore build()
            {
                return builder.build();
            }

            long retentionPeriod()
            {
                if (builder is WindowStoreBuilder)
                {
                    return ((WindowStoreBuilder)builder).retentionPeriod();
                }
                else if (builder is TimestampedWindowStoreBuilder)
                {
                    return ((TimestampedWindowStoreBuilder)builder).retentionPeriod();
                }
                else if (builder is SessionStoreBuilder)
                {
                    return ((SessionStoreBuilder)builder).retentionPeriod();
                }
                else
                {

                    throw new InvalidOperationException("retentionPeriod is not supported when not a window store");
                }
            }

            private HashSet<string> users()
            {
                return users;
            }

            public bool loggingEnabled()
            {
                return builder.loggingEnabled();
            }

            private string name()
            {
                return builder.name();
            }

            private bool isWindowStore()
            {
                return builder is WindowStoreBuilder
                    || builder is TimestampedWindowStoreBuilder
                    || builder is SessionStoreBuilder;
            }

            // Apparently Java strips the generics from this method because we're using the raw type for builder,
            // even though this method doesn't use builder's (missing) type parameter. Our usage seems obviously
            // correct, though, hence the suppression.

            private Dictionary<string, string> logConfig()
            {
                return builder.logConfig();
            }
        }

        private static abstract class NodeFactory
        {

            string name;
            string[] predecessors;

            NodeFactory(string name,
                        string[] predecessors)
            {
                this.name = name;
                this.predecessors = predecessors;
            }

            public abstract ProcessorNode build();

            abstract AbstractNode describe();
        }

        private static class ProcessorNodeFactory : NodeFactory
        {

            private ProcessorSupplier<?, ?> supplier;
            private HashSet<string> stateStoreNames = new HashSet<>();

            ProcessorNodeFactory(string name,
                                 string[] predecessors,
                                 ProcessorSupplier<?, ?> supplier)
            {
                base(name, predecessors.clone());
                this.supplier = supplier;
            }

            public void addStateStore(string stateStoreName)
            {
                stateStoreNames.Add(stateStoreName);
            }


            public ProcessorNode build()
            {
                return new ProcessorNode<>(name, supplier(), stateStoreNames);
            }


            Processor describe()
            {
                return new Processor(name, new HashSet<>(stateStoreNames));
            }
        }

        private class SourceNodeFactory : NodeFactory
        {

            private List<string> topics;
            private Pattern pattern;
            private IDeserializer<?> keyDeserializer;
            private IDeserializer<?> valDeserializer;
            private TimestampExtractor timestampExtractor;

            private SourceNodeFactory(string name,
                                      string[] topics,
                                      Pattern pattern,
                                      TimestampExtractor timestampExtractor,
                                      IDeserializer<?> keyDeserializer,
                                      IDeserializer<?> valDeserializer)
                : base(name, NO_PREDECESSORS)
            {
                this.topics = topics != null ? Arrays.asList(topics) : new List<>();
                this.pattern = pattern;
                this.keyDeserializer = keyDeserializer;
                this.valDeserializer = valDeserializer;
                this.timestampExtractor = timestampExtractor;
            }

            List<string> getTopics(Collection<string> subscribedTopics)
            {
                // if it is subscribed via patterns, it is possible that the topic metadata has not been updated
                // yet and hence the map from source node to topics is stale, in this case we put the pattern as a place holder;
                // this should only happen for debugging since during runtime this function should always be called after the metadata has updated.
                if (subscribedTopics.isEmpty())
                {
                    return Collections.singletonList(string.valueOf(pattern));
                }

                List<string> matchedTopics = new List<>();
                foreach (string update in subscribedTopics)
                {
                    if (pattern == topicToPatterns[update))
                    {
                        matchedTopics.Add(update);
                    }
                    else if (topicToPatterns.ContainsKey(update) && isMatch(update))
                    {
                        // the same topic cannot be matched to more than one pattern
                        // TODO: we should lift this requirement in the future
                        throw new TopologyException("Topic " + update +
                            " is already matched for another regex pattern " + topicToPatterns[update] +
                            " and hence cannot be matched to this regex pattern " + pattern + " any more.");
                    }
                    else if (isMatch(update))
                    {
                        topicToPatterns.Add(update, pattern);
                        matchedTopics.Add(update);
                    }
                }
                return matchedTopics;
            }


            public ProcessorNode build()
            {
                List<string> sourceTopics = nodeToSourceTopics[name];

                // if it is subscribed via patterns, it is possible that the topic metadata has not been updated
                // yet and hence the map from source node to topics is stale, in this case we put the pattern as a place holder;
                // this should only happen for debugging since during runtime this function should always be called after the metadata has updated.
                if (sourceTopics == null)
                {
                    return new SourceNode<>(name, Collections.singletonList(string.valueOf(pattern)), timestampExtractor, keyDeserializer, valDeserializer);
                }
                else
                {

                    return new SourceNode<>(name, maybeDecorateInternalSourceTopics(sourceTopics), timestampExtractor, keyDeserializer, valDeserializer);
                }
            }

            private bool isMatch(string topic)
            {
                return pattern.matcher(topic).matches();
            }


            Source describe()
            {
                return new Source(name, topics.size() == 0 ? null : new HashSet<>(topics), pattern);
            }
        }

        private class SinkNodeFactory<K, V> : NodeFactory
        {

            private ISerializer<K> keySerializer;
            private Serializer<V> valSerializer;
            private StreamPartitioner<K, V> partitioner;
            private TopicNameExtractor<K, V> topicExtractor;

            private SinkNodeFactory(string name,
                                    string[] predecessors,
                                    TopicNameExtractor<K, V> topicExtractor,
                                    ISerializer<K> keySerializer,
                                    ISerializer<V> valSerializer,
                                    StreamPartitioner<K, V> partitioner)
                : base(name, predecessors.clone())
            {
                this.topicExtractor = topicExtractor;
                this.keySerializer = keySerializer;
                this.valSerializer = valSerializer;
                this.partitioner = partitioner;
            }


            public ProcessorNode build()
            {
                if (topicExtractor is StaticTopicNameExtractor)
                {
                    string topic = ((StaticTopicNameExtractor)topicExtractor).topicName;
                    if (internalTopicNames.contains(topic))
                    {
                        // prefix the internal topic name with the application id
                        return new SinkNode<>(name, new StaticTopicNameExtractor<>(decorateTopic(topic)), keySerializer, valSerializer, partitioner);
                    }
                    else
                    {

                        return new SinkNode<>(name, topicExtractor, keySerializer, valSerializer, partitioner);
                    }
                }
                else
                {

                    return new SinkNode<>(name, topicExtractor, keySerializer, valSerializer, partitioner);
                }
            }


            Sink describe()
            {
                return new Sink(name, topicExtractor);
            }
        }

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
                foreach (StateStoreFactory storeFactory in stateFactories.values())
                {
                    storeFactory.builder.withCachingDisabled();
                }

                foreach (StoreBuilder storeBuilder in globalStateBuilders.values())
                {
                    storeBuilder.withCachingDisabled();
                }
            }

            // build global state stores
            foreach (StoreBuilder storeBuilder in globalStateBuilders.values())
            {
                globalStateStores.Add(storeBuilder.name(), storeBuilder.build());
            }

            return this;
        }

        public void addSource(Topology.AutoOffsetReset offsetReset,
                                    string name,
                                    TimestampExtractor timestampExtractor,
                                    Deserializer keyDeserializer,
                                    Deserializer valDeserializer,
                                    string[] topics)
        {
            if (topics.Length == 0)
            {
                throw new TopologyException("You must provide at least one topic");
            }
            name = name ?? throw new System.ArgumentNullException("name must not be null", nameof(name));
            if (nodeFactories.ContainsKey(name))
            {
                throw new TopologyException("Processor " + name + " is already.Added.");
            }

            foreach (string topic in topics)
            {
                topic = topic ?? throw new System.ArgumentNullException("topic names cannot be null", nameof(topic));
                validateTopicNotAlreadyRegistered(topic);
                maybeAddToResetList(earliestResetTopics, latestResetTopics, offsetReset, topic);
                sourceTopicNames.Add(topic);
            }

            nodeFactories.Add(name, new SourceNodeFactory(name, topics, null, timestampExtractor, keyDeserializer, valDeserializer));
            nodeToSourceTopics.Add(name, Arrays.asList(topics));
            nodeGrouper.Add(name);
            nodeGroups = null;
        }

        public void addSource(Topology.AutoOffsetReset offsetReset,
                                    string name,
                                    TimestampExtractor timestampExtractor,
                                    Deserializer keyDeserializer,
                                    Deserializer valDeserializer,
                                    Pattern topicPattern)
        {
            topicPattern = topicPattern ?? throw new System.ArgumentNullException("topicPattern can't be null", nameof(topicPattern));
            name = name ?? throw new System.ArgumentNullException("name can't be null", nameof(name));

            if (nodeFactories.ContainsKey(name))
            {
                throw new TopologyException("Processor " + name + " is already.Added.");
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
                if (topicPattern.pattern().contains(otherPattern.pattern()) || otherPattern.pattern().contains(topicPattern.pattern()))
                {
                    throw new TopologyException("Pattern " + topicPattern + " will overlap with another pattern " + otherPattern + " already been registered by another source");
                }
            }

            foreach (Pattern otherPattern in latestResetPatterns)
            {
                if (topicPattern.pattern().contains(otherPattern.pattern()) || otherPattern.pattern().contains(topicPattern.pattern()))
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

        public void addSink(string name,
                                         string topic,
                                         Serializer<K> keySerializer,
                                         Serializer<V> valSerializer,
                                         StreamPartitioner<K, V> partitioner,
                                         string[] predecessorNames)
        {
            name = name ?? throw new System.ArgumentNullException("name must not be null", nameof(name));
            topic = topic ?? throw new System.ArgumentNullException("topic must not be null", nameof(topic));
            predecessorNames = predecessorNames ?? throw new System.ArgumentNullException("predecessor names must not be null", nameof(predecessorNames));
            if (predecessorNames.Length == 0)
            {
                throw new TopologyException("Sink " + name + " must have at least one parent");
            }

            addSink(name, new StaticTopicNameExtractor<>(topic), keySerializer, valSerializer, partitioner, predecessorNames);
            nodeToSinkTopic.Add(name, topic);
            nodeGroups = null;
        }

        public void addSink(string name,
                                         TopicNameExtractor<K, V> topicExtractor,
                                         Serializer<K> keySerializer,
                                         Serializer<V> valSerializer,
                                         StreamPartitioner<K, V> partitioner,
                                         string[] predecessorNames)
        {
            name = name ?? throw new System.ArgumentNullException("name must not be null", nameof(name));
            topicExtractor = topicExtractor ?? throw new System.ArgumentNullException("topic extractor must not be null", nameof(topicExtractor));
            predecessorNames = predecessorNames ?? throw new System.ArgumentNullException("predecessor names must not be null", nameof(predecessorNames));
            if (nodeFactories.ContainsKey(name))
            {
                throw new TopologyException("Processor " + name + " is already.Added.");
            }
            if (predecessorNames.Length == 0)
            {
                throw new TopologyException("Sink " + name + " must have at least one parent");
            }

            foreach (string predecessor in predecessorNames)
            {
                predecessor = predecessor ?? throw new System.ArgumentNullException("predecessor name can't be null", nameof(predecessor));
                if (predecessor.Equals(name))
                {
                    throw new TopologyException("Processor " + name + " cannot be a predecessor of itself.");
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

            nodeFactories.Add(name, new SinkNodeFactory<>(name, predecessorNames, topicExtractor, keySerializer, valSerializer, partitioner));
            nodeGrouper.Add(name);
            nodeGrouper.unite(name, predecessorNames);
            nodeGroups = null;
        }

        public void addProcessor(string name,
                                       ProcessorSupplier supplier,
                                       string[] predecessorNames)
        {
            name = name ?? throw new System.ArgumentNullException("name must not be null", nameof(name));
            supplier = supplier ?? throw new System.ArgumentNullException("supplier must not be null", nameof(supplier));
            predecessorNames = predecessorNames ?? throw new System.ArgumentNullException("predecessor names must not be null", nameof(predecessorNames));
            if (nodeFactories.ContainsKey(name))
            {
                throw new TopologyException("Processor " + name + " is already.Added.");
            }
            if (predecessorNames.Length == 0)
            {
                throw new TopologyException("Processor " + name + " must have at least one parent");
            }

            foreach (string predecessor in predecessorNames)
            {
                predecessor = predecessor ?? throw new System.ArgumentNullException("predecessor name must not be null", nameof(predecessor));
                if (predecessor.Equals(name))
                {
                    throw new TopologyException("Processor " + name + " cannot be a predecessor of itself.");
                }
                if (!nodeFactories.ContainsKey(predecessor))
                {
                    throw new TopologyException("Predecessor processor " + predecessor + " is not.Added yet for " + name);
                }
            }

            nodeFactories.Add(name, new ProcessorNodeFactory(name, predecessorNames, supplier));
            nodeGrouper.Add(name);
            nodeGrouper.unite(name, predecessorNames);
            nodeGroups = null;
        }

        public void addStateStore(StoreBuilder<?> storeBuilder,
                                        string[] processorNames)
        {
            addStateStore(storeBuilder, false, processorNames);
        }

        public void addStateStore(StoreBuilder<?> storeBuilder,
                                        bool allowOverride,
                                        string[] processorNames)
        {
            storeBuilder = storeBuilder ?? throw new System.ArgumentNullException("storeBuilder can't be null", nameof(storeBuilder));
            if (!allowOverride && stateFactories.ContainsKey(storeBuilder.name()))
            {
                throw new TopologyException("IStateStore " + storeBuilder.name() + " is already.Added.");
            }

            stateFactories.Add(storeBuilder.name(), new StateStoreFactory(storeBuilder));

            if (processorNames != null)
            {
                foreach (string processorName in processorNames)
                {
                    processorName = processorName ?? throw new System.ArgumentNullException("processor name must not be null", nameof(processorName));
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
                                         ProcessorSupplier stateUpdateSupplier)
        {
            storeBuilder = storeBuilder ?? throw new System.ArgumentNullException("store builder must not be null", nameof(storeBuilder));
            validateGlobalStoreArguments(sourceName,
                                         topic,
                                         processorName,
                                         stateUpdateSupplier,
                                         storeBuilder.name(),
                                         storeBuilder.loggingEnabled());
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
            nodeGrouper.Add(sourceName);
            nodeFactory.AddStateStore(storeBuilder.name());
            nodeFactories.Add(processorName, nodeFactory);
            nodeGrouper.Add(processorName);
            nodeGrouper.unite(processorName, predecessors);
            globalStateBuilders.Add(storeBuilder.name(), storeBuilder);
            connectSourceStoreAndTopic(storeBuilder.name(), topic);
            nodeGroups = null;
        }

        private void validateTopicNotAlreadyRegistered(string topic)
        {
            if (sourceTopicNames.contains(topic) || globalTopics.contains(topic))
            {
                throw new TopologyException("Topic " + topic + " has already been registered by another source.");
            }

            foreach (Pattern pattern in nodeToSourcePatterns.values())
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

        public void connectSourceStoreAndTopic(string sourceStoreName,
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

        public void copartitionSources(Collection<string> sourceNodes)
        {
            copartitionSourceGroups.Add(Collections.unmodifiableSet(new HashSet<>(sourceNodes)));
        }

        private void validateGlobalStoreArguments(string sourceName,
                                                  string topic,
                                                  string processorName,
                                                  ProcessorSupplier stateUpdateSupplier,
                                                  string storeName,
                                                  bool loggingEnabled)
        {
            sourceName = sourceName ?? throw new System.ArgumentNullException("sourceName must not be null", nameof(sourceName));
            topic = topic ?? throw new System.ArgumentNullException("topic must not be null", nameof(topic));
            stateUpdateSupplier = stateUpdateSupplier ?? throw new System.ArgumentNullException("supplier must not be null", nameof(stateUpdateSupplier));
            processorName = processorName ?? throw new System.ArgumentNullException("processorName must not be null", nameof(processorName));
            if (nodeFactories.ContainsKey(sourceName))
            {
                throw new TopologyException("Processor " + sourceName + " is already.Added.");
            }
            if (nodeFactories.ContainsKey(processorName))
            {
                throw new TopologyException("Processor " + processorName + " is already.Added.");
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

        private void connectProcessorAndStateStore(string processorName,
                                                   string stateStoreName)
        {
            if (globalStateBuilders.ContainsKey(stateStoreName))
            {
                throw new TopologyException("Global IStateStore " + stateStoreName +
                        " can be used by a Processor without being specified; it should not be explicitly passed.");
            }
            if (!stateFactories.ContainsKey(stateStoreName))
            {
                throw new TopologyException("IStateStore " + stateStoreName + " is not.Added yet.");
            }
            if (!nodeFactories.ContainsKey(processorName))
            {
                throw new TopologyException("Processor " + processorName + " is not.Added yet.");
            }

            StateStoreFactory stateStoreFactory = stateFactories[stateStoreName];
            Iterator<string> iter = stateStoreFactory.users().iterator();
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
                processorNodeFactory.AddStateStore(stateStoreName);
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

            foreach (SourceNodeFactory sourceNodeFactory in sourceNodesForPredecessor)
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

            if (!sourceTopics.isEmpty())
            {
                stateStoreNameToSourceTopics.Add(stateStoreName,
                        Collections.unmodifiableSet(sourceTopics));
            }

            if (!sourcePatterns.isEmpty())
            {
                stateStoreNameToSourceRegex.Add(stateStoreName,
                        Collections.unmodifiableSet(sourcePatterns));
            }

        }

        private void maybeAddToResetList(Collection<T> earliestResets,
                                             Collection<T> latestResets,
                                             Topology.AutoOffsetReset offsetReset,
                                             T item)
        {
            if (offsetReset != null)
            {
                switch (offsetReset)
                {
                    case EARLIEST:
                        earliestResets.Add(item);
                        break;
                    case LATEST:
                        latestResets.Add(item);
                        break;
                    default:
                        throw new TopologyException(string.Format("Unrecognized reset format %s", offsetReset));
                }
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Dictionary<int, HashSet<string>> nodeGroups()
        {
            if (nodeGroups == null)
            {
                nodeGroups = makeNodeGroups();
            }
            return nodeGroups;
        }

        private Dictionary<int, HashSet<string>> makeNodeGroups()
        {
            Dictionary<int, HashSet<string>> nodeGroups = new LinkedHashMap<>();
            Dictionary<string, HashSet<string>> rootToNodeGroup = new HashMap<>();

            int nodeGroupId = 0;

            // Go through source nodes first. This makes the group id assignment easy to predict in tests
            HashSet<string> allSourceNodes = new HashSet<>(nodeToSourceTopics.keySet());
            allSourceNodes.AddAll(nodeToSourcePatterns.keySet());

            foreach (string nodeName in Utils.sorted(allSourceNodes))
            {
                nodeGroupId = putNodeGroupName(nodeName, nodeGroupId, nodeGroups, rootToNodeGroup);
            }

            // Go through non-source nodes
            foreach (string nodeName in Utils.sorted(nodeFactories.keySet()))
            {
                if (!nodeToSourceTopics.ContainsKey(nodeName))
                {
                    nodeGroupId = putNodeGroupName(nodeName, nodeGroupId, nodeGroups, rootToNodeGroup);
                }
            }

            return nodeGroups;
        }

        private int putNodeGroupName(string nodeName,
                                     int nodeGroupId,
                                     Dictionary<int, HashSet<string>> nodeGroups,
                                     Dictionary<string, HashSet<string>> rootToNodeGroup)
        {
            int newNodeGroupId = nodeGroupId;
            string root = nodeGrouper.root(nodeName);
            HashSet<string> nodeGroup = rootToNodeGroup[root];
            if (nodeGroup == null)
            {
                nodeGroup = new HashSet<>();
                rootToNodeGroup.Add(root, nodeGroup);
                nodeGroups.Add(newNodeGroupId++, nodeGroup);
            }
            nodeGroup.Add(nodeName);
            return newNodeGroupId;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public ProcessorTopology build()
        {
            return build((int)null);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public ProcessorTopology build(int topicGroupId)
        {
            HashSet<string> nodeGroup;
            if (topicGroupId != null)
            {
                nodeGroup = nodeGroups()[topicGroupId);
            }
            else
            {

                // when topicGroupId is null, we build the full topology minus the global groups
                HashSet<string> globalNodeGroups = globalNodeGroups();
                Collection<Set<string>> values = nodeGroups().values();
                nodeGroup = new HashSet<>();
                foreach (Set<string> value in values)
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
            if (globalGroups.isEmpty())
            {
                return null;
            }
            return build(globalGroups);
        }

        private HashSet<string> globalNodeGroups()
        {
            HashSet<string> globalGroups = new HashSet<>();
            foreach (Map.Entry<int, HashSet<string>> nodeGroup in nodeGroups().entrySet())
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

        private ProcessorTopology build(Set<string> nodeGroup)
        {
            applicationId = applicationId ?? throw new System.ArgumentNullException("topology has not completed optimization", nameof(applicationId));

            Dictionary<string, ProcessorNode> processorMap = new LinkedHashMap<>();
            Dictionary<string, SourceNode> topicSourceMap = new HashMap<>();
            Dictionary<string, SinkNode> topicSinkMap = new HashMap<>();
            Dictionary<string, IStateStore> stateStoreMap = new LinkedHashMap<>();
            HashSet<string> repartitionTopics = new HashSet<>();

            // create processor nodes in a topological order ("nodeFactories" is already topologically sorted)
            // also make sure the state store map values following the insertion ordering
            foreach (NodeFactory factory in nodeFactories.values())
            {
                if (nodeGroup == null || nodeGroup.contains(factory.name))
                {
                    ProcessorNode node = factory.build();
                    processorMap.Add(node.name(), node);

                    if (factory is ProcessorNodeFactory)
                    {
                        buildProcessorNode(processorMap,
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

            return new ProcessorTopology(new List<>(processorMap.values()),
                                         topicSourceMap,
                                         topicSinkMap,
                                         new List<>(stateStoreMap.values()),
                                         new List<>(globalStateStores.values()),
                                         storeToChangelogTopic,
                                         repartitionTopics);
        }


        private void buildSinkNode(Dictionary<string, ProcessorNode> processorMap,
                                   Dictionary<string, SinkNode> topicSinkMap,
                                   HashSet<string> repartitionTopics,
                                   SinkNodeFactory sinkNodeFactory,
                                   SinkNode node)
        {

            foreach (string predecessor in sinkNodeFactory.predecessors)
            {
                processorMap[predecessor).AddChild(node);
                if (sinkNodeFactory.topicExtractor is StaticTopicNameExtractor)
                {
                    string topic = ((StaticTopicNameExtractor)sinkNodeFactory.topicExtractor).topicName;

                    if (internalTopicNames.contains(topic))
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

        private void buildSourceNode(Dictionary<string, SourceNode> topicSourceMap,
                                     HashSet<string> repartitionTopics,
                                     SourceNodeFactory sourceNodeFactory,
                                     SourceNode node)
        {

            List<string> topics = (sourceNodeFactory.pattern != null) ?
                                        sourceNodeFactory.getTopics(subscriptionUpdates.getUpdates()) :
                                        sourceNodeFactory.topics;

            foreach (string topic in topics)
            {
                if (internalTopicNames.contains(topic))
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

        private void buildProcessorNode(Dictionary<string, ProcessorNode> processorMap,
                                        Dictionary<string, IStateStore> stateStoreMap,
                                        ProcessorNodeFactory factory,
                                        ProcessorNode node)
        {

            foreach (string predecessor in factory.predecessors)
            {
                ProcessorNode <?, ?> predecessorNode = processorMap[predecessor];
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
                        if (stateStoreFactory.loggingEnabled() && !storeToChangelogTopic.ContainsKey(stateStoreName))
                        {
                            string changelogTopic = ProcessorStateManager.storeChangelogTopic(applicationId, stateStoreName);
                            storeToChangelogTopic.Add(stateStoreName, changelogTopic);
                        }
                        stateStoreMap.Add(stateStoreName, stateStoreFactory.build());
                    }
                    else
                    {

                        stateStoreMap.Add(stateStoreName, globalStateStores[stateStoreName));
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

            return Collections.unmodifiableMap(globalStateStores);
        }

        public HashSet<string> allStateStoreName()
        {
            applicationId = applicationId ?? throw new System.ArgumentNullException("topology has not completed optimization", nameof(applicationId));

            HashSet<string> allNames = new HashSet<>(stateFactories.keySet());
            allNames.AddAll(globalStateStores.keySet());
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

            foreach (Map.Entry<int, HashSet<string>> entry in nodeGroups.entrySet())
            {
                HashSet<string> sinkTopics = new HashSet<>();
                HashSet<string> sourceTopics = new HashSet<>();
                Dictionary<string, InternalTopicConfig> repartitionTopics = new HashMap<>();
                Dictionary<string, InternalTopicConfig> stateChangelogTopics = new HashMap<>();
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
                            if (globalTopics.contains(topic))
                            {
                                continue;
                            }
                            if (internalTopicNames.contains(topic))
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
                        if (internalTopicNames.contains(topic))
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
                    foreach (StateStoreFactory stateFactory in stateFactories.values())
                    {
                        if (stateFactory.loggingEnabled() && stateFactory.users().contains(node))
                        {
                            string topicName = storeToChangelogTopic.ContainsKey(stateFactory.name()) ?
                                    storeToChangelogTopic[stateFactory.name()] :
                                    ProcessorStateManager.storeChangelogTopic(applicationId, stateFactory.name());
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
                foreach (Map.Entry<string, Pattern> stringPatternEntry in nodeToSourcePatterns.entrySet())
                {
                    SourceNodeFactory sourceNode =
                        (SourceNodeFactory)nodeFactories[stringPatternEntry.Key);
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
                foreach (Map.Entry<string, HashSet<Pattern>> storePattern in stateStoreNameToSourceRegex.entrySet())
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
                        Collection<string> storeTopics = stateStoreNameToSourceTopics[storePattern.Key);
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

        private Pattern resetTopicsPattern(Set<string> resetTopics,
                                           HashSet<Pattern> resetPatterns)
        {
            List<string> topics = maybeDecorateInternalSourceTopics(resetTopics);

            return buildPatternForOffsetResetTopics(topics, resetPatterns);
        }

        private static Pattern buildPatternForOffsetResetTopics(Collection<string> sourceTopics,
                                                                Collection<Pattern> sourcePatterns)
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
            Dictionary<string, List<string>> results = new HashMap<>();
            foreach (Map.Entry<string, HashSet<string>> entry in stateStoreNameToSourceTopics.entrySet())
            {
                results.Add(entry.Key, maybeDecorateInternalSourceTopics(entry.Value));
            }
            return results;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Collection<Set<string>> copartitionGroups()
        {
            List<Set<string>> list = new List<>(copartitionSourceGroups.size());
            foreach (Set<string> nodeNames in copartitionSourceGroups)
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

        private List<string> maybeDecorateInternalSourceTopics(Collection<string> sourceTopics)
        {
            List<string> decoratedTopics = new List<>();
            foreach (string topic in sourceTopics)
            {
                if (internalTopicNames.contains(topic))
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
                    foreach (List<string> topics in nodeToSourceTopics.values())
                    {
                        allSourceTopics.AddAll(maybeDecorateInternalSourceTopics(topics));
                    }
                }
                Collections.sort(allSourceTopics);

                topicPattern = buildPatternForOffsetResetTopics(allSourceTopics, nodeToSourcePatterns.values());
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
                return topics != null && topics.size() == 1 && globalTopics.contains(topics[0));
            }
            return false;
        }

        public TopologyDescription describe()
        {
            TopologyDescription description = new TopologyDescription();

            foreach (Map.Entry<int, HashSet<string>> nodeGroup in makeNodeGroups().entrySet())
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
            Iterator<string> it = nodes.iterator();
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
                        ((ProcessorNodeFactory)nodeFactories[processorNode)).stateStoreNames.iterator().next(),
                        nodeToSourceTopics[node)[0],
                        id
                    ));
                    break;
                }
            }
        }

        private bool nodeGroupContainsGlobalSourceNode(Set<string> allNodesOfGroups)
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

        private static class NodeComparator : Comparator<TopologyDescription.Node>, Serializable
        {



            public int compare(TopologyDescription.Node node1,
                               TopologyDescription.Node node2)
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

                    return node1.name().compareTo(node2.name());
                }
            }
        }

        private static NodeComparator NODE_COMPARATOR = new NodeComparator();

        private static void updateSize(AbstractNode node,
                                       int delta)
        {
            node.size += delta;

            foreach (TopologyDescription.Node predecessor in node.predecessors())
            {
                updateSize((AbstractNode)predecessor, delta);
            }
        }

        private void describeSubtopology(TopologyDescription description,
                                         int subtopologyId,
                                         HashSet<string> nodeNames)
        {

            Dictionary<string, AbstractNode> nodesByName = new HashMap<>();

            //.Add all nodes
            foreach (string nodeName in nodeNames)
            {
                nodesByName.Add(nodeName, nodeFactories[nodeName).describe());
            }

            // connect each node to its predecessors and successors
            foreach (AbstractNode node in nodesByName.values())
            {
                foreach (string predecessorName in nodeFactories[node.name()).predecessors)
                {
                    AbstractNode predecessor = nodesByName[predecessorName];
                    node.AddPredecessor(predecessor);
                    predecessor.AddSuccessor(node);
                    updateSize(predecessor, node.size);
                }
            }

            description.AddSubtopology(new Subtopology(
                    subtopologyId,
                    new HashSet<>(nodesByName.values())));
        }

        public static class GlobalStore : TopologyDescription.GlobalStore
        {

            private Source source;
            private Processor processor;
            private int id;

            public GlobalStore(string sourceName,
                               string processorName,
                               string storeName,
                               string topicName,
                               int id)
            {
                source = new Source(sourceName, Collections.singleton(topicName), null);
                processor = new Processor(processorName, Collections.singleton(storeName));
                source.successors.Add(processor);
                processor.predecessors.Add(source);
                this.id = id;
            }


            public int id()
            {
                return id;
            }


            public TopologyDescription.Source source()
            {
                return source;
            }


            public TopologyDescription.Processor processor()
            {
                return processor;
            }


            public string ToString()
            {
                return "Sub-topology: " + id + " for global store (will not generate tasks)\n"
                        + "    " + source.ToString() + "\n"
                        + "    " + processor.ToString() + "\n";
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

                GlobalStore that = (GlobalStore)o;
                return source.Equals(that.source)
                    && processor.Equals(that.processor);
            }


            public int GetHashCode()
            {
                return Objects.hash(source, processor);
            }
        }

        public abstract static class AbstractNode : TopologyDescription.Node
        {

            string name;
            HashSet<TopologyDescription.Node> predecessors = new TreeSet<>(NODE_COMPARATOR);
            HashSet<TopologyDescription.Node> successors = new TreeSet<>(NODE_COMPARATOR);

            // size of the sub-topology rooted at this node, including the node itself
            int size;

            AbstractNode(string name)
            {
                name = name ?? throw new System.ArgumentNullException("name cannot be null", nameof(name));
                this.name = name;
                this.size = 1;
            }


            public string name()
            {
                return name;
            }


            public HashSet<TopologyDescription.Node> predecessors()
            {
                return Collections.unmodifiableSet(predecessors);
            }


            public HashSet<TopologyDescription.Node> successors()
            {
                return Collections.unmodifiableSet(successors);
            }

            public void addPredecessor(TopologyDescription.Node predecessor)
            {
                predecessors.Add(predecessor);
            }

            public void addSuccessor(TopologyDescription.Node successor)
            {
                successors.Add(successor);
            }
        }

        public static class Source : AbstractNode, TopologyDescription.Source
        {

            private HashSet<string> topics;
            private Pattern topicPattern;

            public Source(string name,
                          HashSet<string> topics,
                          Pattern pattern)
                : base(name)
            {
                if (topics == null && pattern == null)
                {
                    throw new System.ArgumentException("Either topics or pattern must be not-null, but both are null.");
                }
                if (topics != null && pattern != null)
                {
                    throw new System.ArgumentException("Either topics or pattern must be null, but both are not null.");
                }

                this.topics = topics;
                this.topicPattern = pattern;
            }

            [System.Obsolete]

            public string topics()
            {
                return topics.ToString();
            }


            public HashSet<string> topicSet()
            {
                return topics;
            }


            public Pattern topicPattern()
            {
                return topicPattern;
            }


            public void addPredecessor(TopologyDescription.Node predecessor)
            {
                throw new InvalidOperationException("Sources don't have predecessors.");
            }


            public string ToString()
            {
                string topicsString = topics == null ? topicPattern.ToString() : topics.ToString();

                return "Source: " + name + " (topics: " + topicsString + ")\n      --> " + nodeNames(successors);
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

                Source source = (Source)o;
                // omit successor to avoid infinite loops
                return name.Equals(source.name)
                    && Objects.Equals(topics, source.topics)
                    && (topicPattern == null ? source.topicPattern == null :
                        topicPattern.pattern().Equals(source.topicPattern.pattern()));
            }


            public int GetHashCode()
            {
                // omit successor as it might change and alter the hash code
                return Objects.hash(name, topics, topicPattern);
            }
        }

        public static class Processor : AbstractNode, TopologyDescription.Processor
        {

            private HashSet<string> stores;

            public Processor(string name,
                             HashSet<string> stores)
            {
                base(name);
                this.stores = stores;
            }


            public HashSet<string> stores()
            {
                return Collections.unmodifiableSet(stores);
            }


            public string ToString()
            {
                return "Processor: " + name + " (stores: " + stores + ")\n      --> "
                    + nodeNames(successors) + "\n      <-- " + nodeNames(predecessors);
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

                Processor processor = (Processor)o;
                // omit successor to avoid infinite loops
                return name.Equals(processor.name)
                    && stores.Equals(processor.stores)
                    && predecessors.Equals(processor.predecessors);
            }


            public int GetHashCode()
            {
                // omit successor as it might change and alter the hash code
                return Objects.hash(name, stores);
            }
        }

        public static class Sink : AbstractNode, TopologyDescription.Sink
        {

            private TopicNameExtractor topicNameExtractor;

            public Sink(string name,
                        TopicNameExtractor topicNameExtractor)
                : base(name)
            {
                this.topicNameExtractor = topicNameExtractor;
            }

            public Sink(string name,
                        string topic)
                : base(name)
            {
                this.topicNameExtractor = new StaticTopicNameExtractor(topic);
            }


            public string topic()
            {
                if (topicNameExtractor is StaticTopicNameExtractor)
                {
                    return ((StaticTopicNameExtractor)topicNameExtractor).topicName;
                }
                else
                {

                    return null;
                }
            }


            public TopicNameExtractor topicNameExtractor()
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


            public void addSuccessor(TopologyDescription.Node successor)
            {
                throw new InvalidOperationException("Sinks don't have successors.");
            }


            public string ToString()
            {
                if (topicNameExtractor is StaticTopicNameExtractor)
                {
                    return "Sink: " + name + " (topic: " + topic() + ")\n      <-- " + nodeNames(predecessors);
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


            public int GetHashCode()
            {
                // omit predecessors as it might change and alter the hash code
                return Objects.hash(name, topicNameExtractor);
            }
        }

        public static class Subtopology : TopologyDescription.Subtopology
        {

            private int id;
            private HashSet<TopologyDescription.Node> nodes;

            public Subtopology(int id, HashSet<TopologyDescription.Node> nodes)
            {
                this.id = id;
                this.nodes = new TreeSet<>(NODE_COMPARATOR);
                this.nodes.AddAll(nodes);
            }


            public int id()
            {
                return id;
            }


            public HashSet<TopologyDescription.Node> nodes()
            {
                return Collections.unmodifiableSet(nodes);
            }

            // visible for testing
            Iterator<TopologyDescription.Node> nodesInOrder()
            {
                return nodes.iterator();
            }


            public string ToString()
            {
                return "Sub-topology: " + id + "\n" + nodesAsString() + "\n";
            }

            private string nodesAsString()
            {
                StringBuilder sb = new StringBuilder();
                foreach (TopologyDescription.Node node in nodes)
                {
                    sb.Append("    ");
                    sb.Append(node);
                    sb.Append('\n');
                }
                return sb.ToString();
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

                Subtopology that = (Subtopology)o;
                return id == that.id
                    && nodes.Equals(that.nodes);
            }


            public int GetHashCode()
            {
                return Objects.hash(id, nodes);
            }
        }

        public static class TopicsInfo
        {

            HashSet<string> sinkTopics;
            HashSet<string> sourceTopics;
            public Dictionary<string, InternalTopicConfig> stateChangelogTopics;
            public Dictionary<string, InternalTopicConfig> repartitionSourceTopics;

            TopicsInfo(Set<string> sinkTopics,
                       HashSet<string> sourceTopics,
                       Dictionary<string, InternalTopicConfig> repartitionSourceTopics,
                       Dictionary<string, InternalTopicConfig> stateChangelogTopics)
            {
                this.sinkTopics = sinkTopics;
                this.sourceTopics = sourceTopics;
                this.stateChangelogTopics = stateChangelogTopics;
                this.repartitionSourceTopics = repartitionSourceTopics;
            }


            public bool Equals(object o)
            {
                if (o is TopicsInfo)
                {
                    TopicsInfo other = (TopicsInfo)o;
                    return other.sourceTopics.Equals(sourceTopics) && other.stateChangelogTopics.Equals(stateChangelogTopics);
                }
                else
                {

                    return false;
                }
            }


            public int GetHashCode()
            {
                long n = ((long)sourceTopics.GetHashCode() << 32) | (long)stateChangelogTopics.GetHashCode();
                return (int)(n % 0xFFFFFFFFL);
            }


            public string ToString()
            {
                return "TopicsInfo{" +
                    "sinkTopics=" + sinkTopics +
                    ", sourceTopics=" + sourceTopics +
                    ", repartitionSourceTopics=" + repartitionSourceTopics +
                    ", stateChangelogTopics=" + stateChangelogTopics +
                    '}';
            }
        }

        private static class GlobalStoreComparator : Comparator<TopologyDescription.GlobalStore>, Serializable
        {


            public int compare(TopologyDescription.GlobalStore globalStore1,
                               TopologyDescription.GlobalStore globalStore2)
            {
                if (globalStore1.Equals(globalStore2))
                {
                    return 0;
                }
                return globalStore1.id() - globalStore2.id();
            }
        }

        private static GlobalStoreComparator GLOBALSTORE_COMPARATOR = new GlobalStoreComparator();

        private static class SubtopologyComparator : Comparator<TopologyDescription.Subtopology>, Serializable
        {


            public int compare(TopologyDescription.Subtopology subtopology1,
                               TopologyDescription.Subtopology subtopology2)
            {
                if (subtopology1.Equals(subtopology2))
                {
                    return 0;
                }
                return subtopology1.id() - subtopology2.id();
            }
        }

        private static SubtopologyComparator SUBTOPOLOGY_COMPARATOR = new SubtopologyComparator();

        public static class TopologyDescription : TopologyDescription
        {

            private TreeSet<TopologyDescription.Subtopology> subtopologies = new TreeSet<>(SUBTOPOLOGY_COMPARATOR);
            private TreeSet<TopologyDescription.GlobalStore> globalStores = new TreeSet<>(GLOBALSTORE_COMPARATOR);

            public void addSubtopology(TopologyDescription.Subtopology subtopology)
            {
                subtopologies.Add(subtopology);
            }

            public void addGlobalStore(TopologyDescription.GlobalStore globalStore)
            {
                globalStores.Add(globalStore);
            }


            public HashSet<TopologyDescription.Subtopology> subtopologies()
            {
                return Collections.unmodifiableSet(subtopologies);
            }


            public HashSet<TopologyDescription.GlobalStore> globalStores()
            {
                return Collections.unmodifiableSet(globalStores);
            }


            public string ToString()
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("Topologies:\n ");
                TopologyDescription.Subtopology[] sortedSubtopologies =
                    subtopologies.descendingSet().toArray(new Subtopology[0]);
                TopologyDescription.GlobalStore[] sortedGlobalStores =
                    globalStores.descendingSet().toArray(new GlobalStore[0]);
                int expectedId = 0;
                int subtopologiesIndex = sortedSubtopologies.Length - 1;
                int globalStoresIndex = sortedGlobalStores.Length - 1;
                while (subtopologiesIndex != -1 && globalStoresIndex != -1)
                {
                    sb.Append("  ");
                    TopologyDescription.Subtopology subtopology = sortedSubtopologies[subtopologiesIndex];
                    TopologyDescription.GlobalStore globalStore = sortedGlobalStores[globalStoresIndex];
                    if (subtopology.id() == expectedId)
                    {
                        sb.Append(subtopology);
                        subtopologiesIndex--;
                    }
                    else
                    {

                        sb.Append(globalStore);
                        globalStoresIndex--;
                    }
                    expectedId++;
                }
                while (subtopologiesIndex != -1)
                {
                    TopologyDescription.Subtopology subtopology = sortedSubtopologies[subtopologiesIndex];
                    sb.Append("  ");
                    sb.Append(subtopology);
                    subtopologiesIndex--;
                }
                while (globalStoresIndex != -1)
                {
                    TopologyDescription.GlobalStore globalStore = sortedGlobalStores[globalStoresIndex];
                    sb.Append("  ");
                    sb.Append(globalStore);
                    globalStoresIndex--;
                }
                return sb.ToString();
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

                TopologyDescription that = (TopologyDescription)o;
                return subtopologies.Equals(that.subtopologies)
                    && globalStores.Equals(that.globalStores);
            }


            public int GetHashCode()
            {
                return Objects.hash(subtopologies, globalStores);
            }

        }

        private static string nodeNames(Set<TopologyDescription.Node> nodes)
        {
            StringBuilder sb = new StringBuilder();
            if (!nodes.isEmpty())
            {
                foreach (TopologyDescription.Node n in nodes)
                {
                    sb.Append(n.name());
                    sb.Append(", ");
                }
                sb.deleteCharAt(sb.Length - 1);
                sb.deleteCharAt(sb.Length - 1);
            }
            else
            {

                return "none";
            }
            return sb.ToString();
        }

        /**
         * Used to capture subscribed topic via Patterns discovered during the
         * partition assignment process.
         */
        public static class SubscriptionUpdates
        {


            private HashSet<string> updatedTopicSubscriptions = new HashSet<>();

            private void updateTopics(Collection<string> topicNames)
            {
                updatedTopicSubscriptions.clear();
                updatedTopicSubscriptions.AddAll(topicNames);
            }

            public Collection<string> getUpdates()
            {
                return Collections.unmodifiableSet(updatedTopicSubscriptions);
            }

            bool hasUpdates()
            {
                return !updatedTopicSubscriptions.isEmpty();
            }


            public string ToString()
            {
                return string.Format("SubscriptionUpdates{updatedTopicSubscriptions=%s}", updatedTopicSubscriptions);
            }
        }

        void updateSubscribedTopics(Set<string> topics,
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