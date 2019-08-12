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
using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processor;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals;
using Kafka.Streams.State;
using System.Runtime.CompilerServices;

namespace Kafka.Streams
{
    /**
     * A logical representation of a {@link ProcessorTopology}.
     * A topology is an acyclic graph of sources, processors, and sinks.
     * A {@link SourceNode source} is a node in the graph that consumes one or more Kafka topics and forwards them to its
     * successor nodes.
     * A {@link IProcessor processor} is a node in the graph that receives input records from upstream nodes, processes the
     * records, and optionally forwarding new records to one or all of its downstream nodes.
     * Finally, a {@link SinkNode sink} is a node in the graph that receives records from upstream nodes and writes them to
     * a Kafka topic.
     * A {@code Topology} allows you to construct an acyclic graph of these nodes, and then passed into a new
     * {@link KafkaStreams} instance that will then {@link KafkaStreams#start() begin consuming, processing, and producing
     * records}.
     */
    public class Topology
    {
        public InternalTopologyBuilder internalTopologyBuilder { get; }
            = new InternalTopologyBuilder();

        /**
         * Add a new source that consumes the named topics and forward the records to child processor and/or sink nodes.
         * The source will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
         * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
         * {@link StreamsConfig stream configuration}.
         * The default {@link ITimestampExtractor} as specified in the {@link StreamsConfig config} is used.
         *
         * @param name the unique name of the source used to reference this node when
         * {@link #addProcessor(string, IProcessorSupplier, string[]) adding processor children}.
         * @param topics the name of one or more Kafka topics that this source is to consume
         * @return itself
         * @throws TopologyException if processor is already added or if topics have already been registered by another source
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSource<K, V>(
            string name,
            string[] topics)
        {
            internalTopologyBuilder.addSource<K, V>(AutoOffsetReset.UNKNOWN, name, null, null, null, topics);
            return this;
        }

        /**
         * Add a new source that consumes from topics matching the given pattern
         * and forward the records to child processor and/or sink nodes.
         * The source will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
         * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
         * {@link StreamsConfig stream configuration}.
         * The default {@link ITimestampExtractor} as specified in the {@link StreamsConfig config} is used.
         *
         * @param name the unique name of the source used to reference this node when
         * {@link #addProcessor(string, IProcessorSupplier, string[]) adding processor children}.
         * @param topicPattern regular expression pattern to match Kafka topics that this source is to consume
         * @return itself
         * @throws TopologyException if processor is already added or if topics have already been registered by another source
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSource<K, V>(
            string name,
            Regex topicPattern)
        {
            internalTopologyBuilder.addSource<K, V>(AutoOffsetReset.UNKNOWN, name, null, null, null, topicPattern);
            return this;
        }

        /**
         * Add a new source that consumes the named topics and forward the records to child processor and/or sink nodes.
         * The source will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
         * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
         * {@link StreamsConfig stream configuration}.
         * The default {@link ITimestampExtractor} as specified in the {@link StreamsConfig config} is used.
         *
         * @param offsetReset the auto offset reset policy to use for this source if no committed offsets found; acceptable values earliest or latest
         * @param name the unique name of the source used to reference this node when
         * {@link #addProcessor(string, IProcessorSupplier, string[]) adding processor children}.
         * @param topics the name of one or more Kafka topics that this source is to consume
         * @return itself
         * @throws TopologyException if processor is already added or if topics have already been registered by another source
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSource<K, V>(
            AutoOffsetReset offsetReset,
            string name,
            string[] topics)
        {
            internalTopologyBuilder.addSource<K, V>(offsetReset, name, null, null, null, topics);
            return this;
        }

        /**
         * Add a new source that consumes from topics matching the given pattern
         * and forward the records to child processor and/or sink nodes.
         * The source will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
         * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
         * {@link StreamsConfig stream configuration}.
         * The default {@link ITimestampExtractor} as specified in the {@link StreamsConfig config} is used.
         *
         * @param offsetReset the auto offset reset policy value for this source if no committed offsets found; acceptable values earliest or latest.
         * @param name the unique name of the source used to reference this node when
         * {@link #addProcessor(string, IProcessorSupplier, string[]) adding processor children}.
         * @param topicPattern regular expression pattern to match Kafka topics that this source is to consume
         * @return itself
         * @throws TopologyException if processor is already added or if topics have already been registered by another source
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSource<K, V>(
            AutoOffsetReset offsetReset,
            string name,
            Regex topicPattern)
        {
            internalTopologyBuilder.addSource<K, V>(offsetReset, name, null, null, null, topicPattern);
            return this;
        }

        /**
         * Add a new source that consumes the named topics and forward the records to child processor and/or sink nodes.
         * The source will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
         * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
         * {@link StreamsConfig stream configuration}.
         *
         * @param timestampExtractor the stateless timestamp extractor used for this source,
         *                           if not specified the default extractor defined in the configs will be used
         * @param name               the unique name of the source used to reference this node when
         *                           {@link #addProcessor(string, IProcessorSupplier, string[]) adding processor children}.
         * @param topics             the name of one or more Kafka topics that this source is to consume
         * @return itself
         * @throws TopologyException if processor is already added or if topics have already been registered by another source
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSource<K, V>(
            ITimestampExtractor timestampExtractor,
            string name,
            string[] topics)
        {
            internalTopologyBuilder.addSource<K, V>(AutoOffsetReset.UNKNOWN, name, timestampExtractor, null, null, topics);
            return this;
        }

        /**
         * Add a new source that consumes from topics matching the given pattern
         * and forward the records to child processor and/or sink nodes.
         * The source will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
         * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
         * {@link StreamsConfig stream configuration}.
         *
         * @param timestampExtractor the stateless timestamp extractor used for this source,
         *                           if not specified the default extractor defined in the configs will be used
         * @param name               the unique name of the source used to reference this node when
         *                           {@link #addProcessor(string, IProcessorSupplier, string[]) adding processor children}.
         * @param topicPattern       regular expression pattern to match Kafka topics that this source is to consume
         * @return itself
         * @throws TopologyException if processor is already added or if topics have already been registered by another source
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSource<K, V>(
            ITimestampExtractor timestampExtractor,
            string name,
            Regex topicPattern)
        {
            internalTopologyBuilder.addSource<K, V>(AutoOffsetReset.UNKNOWN, name, timestampExtractor, null, null, topicPattern);
            return this;
        }

        /**
         * Add a new source that consumes the named topics and forward the records to child processor and/or sink nodes.
         * The source will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
         * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
         * {@link StreamsConfig stream configuration}.
         *
         * @param offsetReset        the auto offset reset policy to use for this source if no committed offsets found;
         *                           acceptable values earliest or latest
         * @param timestampExtractor the stateless timestamp extractor used for this source,
         *                           if not specified the default extractor defined in the configs will be used
         * @param name               the unique name of the source used to reference this node when
         *                           {@link #addProcessor(string, IProcessorSupplier, string[]) adding processor children}.
         * @param topics             the name of one or more Kafka topics that this source is to consume
         * @return itself
         * @throws TopologyException if processor is already added or if topics have already been registered by another source
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSource<K, V>(
            AutoOffsetReset offsetReset,
            ITimestampExtractor timestampExtractor,
            string name,
            string[] topics)
        {
            internalTopologyBuilder.addSource<K, V>(offsetReset, name, timestampExtractor, null, null, topics);
            return this;
        }

        /**
         * Add a new source that consumes from topics matching the given pattern and forward the records to child processor
         * and/or sink nodes.
         * The source will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key deserializer} and
         * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value deserializer} specified in the
         * {@link StreamsConfig stream configuration}.
         *
         * @param offsetReset        the auto offset reset policy value for this source if no committed offsets found;
         *                           acceptable values earliest or latest.
         * @param timestampExtractor the stateless timestamp extractor used for this source,
         *                           if not specified the default extractor defined in the configs will be used
         * @param name               the unique name of the source used to reference this node when
         *                           {@link #addProcessor(string, IProcessorSupplier, string[]) adding processor children}.
         * @param topicPattern       regular expression pattern to match Kafka topics that this source is to consume
         * @return itself
         * @throws TopologyException if processor is already added or if topics have already been registered by another source
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSource<K, V>(
            AutoOffsetReset offsetReset,
            ITimestampExtractor timestampExtractor,
            string name,
            Regex topicPattern)
        {
            internalTopologyBuilder.addSource<K, V>(offsetReset, name, timestampExtractor, null, null, topicPattern);
            return this;
        }

        /**
         * Add a new source that consumes the named topics and forwards the records to child processor and/or sink nodes.
         * The source will use the specified key and value deserializers.
         * The default {@link ITimestampExtractor} as specified in the {@link StreamsConfig config} is used.
         *
         * @param name               the unique name of the source used to reference this node when
         *                           {@link #addProcessor(string, IProcessorSupplier, string[]) adding processor children}
         * @param keyDeserializer    key deserializer used to read this source, if not specified the default
         *                           key deserializer defined in the configs will be used
         * @param valueDeserializer  value deserializer used to read this source,
         *                           if not specified the default value deserializer defined in the configs will be used
         * @param topics             the name of one or more Kafka topics that this source is to consume
         * @return itself
         * @throws TopologyException if processor is already added or if topics have already been registered by another source
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSource<K, V>(
            string name,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valueDeserializer,
            string[] topics)
        {
            internalTopologyBuilder.addSource(AutoOffsetReset.UNKNOWN, name, null, keyDeserializer, valueDeserializer, topics);
            return this;
        }

        /**
         * Add a new source that consumes from topics matching the given pattern and forwards the records to child processor
         * and/or sink nodes.
         * The source will use the specified key and value deserializers.
         * The provided de-/serializers will be used for all matched topics, so care should be taken to specify patterns for
         * topics that share the same key-value data string.Format.
         * The default {@link ITimestampExtractor} as specified in the {@link StreamsConfig config} is used.
         *
         * @param name               the unique name of the source used to reference this node when
         *                           {@link #addProcessor(string, IProcessorSupplier, string[]) adding processor children}
         * @param keyDeserializer    key deserializer used to read this source, if not specified the default
         *                           key deserializer defined in the configs will be used
         * @param valueDeserializer  value deserializer used to read this source,
         *                           if not specified the default value deserializer defined in the configs will be used
         * @param topicPattern       regular expression pattern to match Kafka topics that this source is to consume
         * @return itself
         * @throws TopologyException if processor is already added or if topics have already been registered by name
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSource<K, V>(
            string name,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valueDeserializer,
            Regex topicPattern)
        {
            internalTopologyBuilder.addSource(AutoOffsetReset.UNKNOWN, name, null, keyDeserializer, valueDeserializer, topicPattern);
            return this;
        }

        /**
         * Add a new source that consumes from topics matching the given pattern and forwards the records to child processor
         * and/or sink nodes.
         * The source will use the specified key and value deserializers.
         * The provided de-/serializers will be used for all the specified topics, so care should be taken when specifying
         * topics that share the same key-value data string.Format.
         *
         * @param offsetReset        the auto offset reset policy to use for this stream if no committed offsets found;
         *                           acceptable values are earliest or latest
         * @param name               the unique name of the source used to reference this node when
         *                           {@link #addProcessor(string, IProcessorSupplier, string[]) adding processor children}
         * @param keyDeserializer    key deserializer used to read this source, if not specified the default
         *                           key deserializer defined in the configs will be used
         * @param valueDeserializer  value deserializer used to read this source,
         *                           if not specified the default value deserializer defined in the configs will be used
         * @param topics             the name of one or more Kafka topics that this source is to consume
         * @return itself
         * @throws TopologyException if processor is already added or if topics have already been registered by name
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSource<K, V>(
            AutoOffsetReset offsetReset,
            string name,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valueDeserializer,
            string[] topics)
        {
            internalTopologyBuilder.addSource(offsetReset, name, null, keyDeserializer, valueDeserializer, topics);
            return this;
        }

        /**
         * Add a new source that consumes from topics matching the given pattern and forwards the records to child processor
         * and/or sink nodes.
         * The source will use the specified key and value deserializers.
         * The provided de-/serializers will be used for all matched topics, so care should be taken to specify patterns for
         * topics that share the same key-value data string.Format.
         *
         * @param offsetReset        the auto offset reset policy to use for this stream if no committed offsets found;
         *                           acceptable values are earliest or latest
         * @param name               the unique name of the source used to reference this node when
         *                           {@link #addProcessor(string, IProcessorSupplier, string[]) adding processor children}
         * @param keyDeserializer    key deserializer used to read this source, if not specified the default
         *                           key deserializer defined in the configs will be used
         * @param valueDeserializer  value deserializer used to read this source,
         *                           if not specified the default value deserializer defined in the configs will be used
         * @param topicPattern       regular expression pattern to match Kafka topics that this source is to consume
         * @return itself
         * @throws TopologyException if processor is already added or if topics have already been registered by name
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSource<K, V>(
            AutoOffsetReset offsetReset,
            string name,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valueDeserializer,
            Regex topicPattern)
        {
            internalTopologyBuilder.addSource(offsetReset, name, null, keyDeserializer, valueDeserializer, topicPattern);
            return this;
        }

        /**
         * Add a new source that consumes the named topics and forwards the records to child processor and/or sink nodes.
         * The source will use the specified key and value deserializers.
         *
         * @param offsetReset        the auto offset reset policy to use for this stream if no committed offsets found;
         *                           acceptable values are earliest or latest.
         * @param name               the unique name of the source used to reference this node when
         *                           {@link #addProcessor(string, IProcessorSupplier, string[]) adding processor children}.
         * @param timestampExtractor the stateless timestamp extractor used for this source,
         *                           if not specified the default extractor defined in the configs will be used
         * @param keyDeserializer    key deserializer used to read this source, if not specified the default
         *                           key deserializer defined in the configs will be used
         * @param valueDeserializer  value deserializer used to read this source,
         *                           if not specified the default value deserializer defined in the configs will be used
         * @param topics             the name of one or more Kafka topics that this source is to consume
         * @return itself
         * @throws TopologyException if processor is already added or if topics have already been registered by another source
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSource<K, V>(
            AutoOffsetReset offsetReset,
            string name,
            ITimestampExtractor timestampExtractor,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valueDeserializer,
            string[] topics)
        {
            internalTopologyBuilder.addSource(offsetReset, name, timestampExtractor, keyDeserializer, valueDeserializer, topics);
            return this;
        }

        /**
         * Add a new source that consumes from topics matching the given pattern and forwards the records to child processor
         * and/or sink nodes.
         * The source will use the specified key and value deserializers.
         * The provided de-/serializers will be used for all matched topics, so care should be taken to specify patterns for
         * topics that share the same key-value data string.Format.
         *
         * @param offsetReset        the auto offset reset policy to use for this stream if no committed offsets found;
         *                           acceptable values are earliest or latest
         * @param name               the unique name of the source used to reference this node when
         *                           {@link #addProcessor(string, IProcessorSupplier, string[]) adding processor children}.
         * @param timestampExtractor the stateless timestamp extractor used for this source,
         *                           if not specified the default extractor defined in the configs will be used
         * @param keyDeserializer    key deserializer used to read this source, if not specified the default
         *                           key deserializer defined in the configs will be used
         * @param valueDeserializer  value deserializer used to read this source,
         *                           if not specified the default value deserializer defined in the configs will be used
         * @param topicPattern       regular expression pattern to match Kafka topics that this source is to consume
         * @return itself
         * @throws TopologyException if processor is already added or if topics have already been registered by name
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSource<K, V>(
            AutoOffsetReset offsetReset,
            string name,
            ITimestampExtractor timestampExtractor,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valueDeserializer,
            Regex topicPattern)
        {
            internalTopologyBuilder.addSource(offsetReset, name, timestampExtractor, keyDeserializer, valueDeserializer, topicPattern);
            return this;
        }

        /**
         * Add a new sink that forwards records from upstream parent processor and/or source nodes to the named Kafka topic.
         * The sink will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} and
         * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
         * {@link StreamsConfig stream configuration}.
         *
         * @param name the unique name of the sink
         * @param topic the name of the Kafka topic to which this sink should write its records
         * @param parentNames the name of one or more source or processor nodes whose output records this sink should consume
         * and write to its topic
         * @return itself
         * @throws TopologyException if parent processor is not added yet, or if this processor's name is equal to the parent's name
         * @see #addSink(string, string, StreamPartitioner, string[])
         * @see #addSink(string, string, ISerializer, ISerializer, string[])
         * @see #addSink(string, string, ISerializer, ISerializer, StreamPartitioner, string[])
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSink<K, V>(
            string name,
            string topic,
            string[] parentNames)
        {
            internalTopologyBuilder.addSink<K, V>(name, topic, null, null, null, parentNames);
            return this;
        }

        /**
         * Add a new sink that forwards records from upstream parent processor and/or source nodes to the named Kafka topic,
         * using the supplied partitioner.
         * The sink will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} and
         * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
         * {@link StreamsConfig stream configuration}.
         * <p>
         * The sink will also use the specified {@link StreamPartitioner} to determine how records are distributed among
         * the named Kafka topic's partitions.
         * Such control is often useful with topologies that use {@link #addStateStore(IStoreBuilder, string[]) state
         * stores} in its processors.
         * In most other cases, however, a partitioner needs not be specified and Kafka will automatically distribute
         * records among partitions using Kafka's default partitioning logic.
         *
         * @param name the unique name of the sink
         * @param topic the name of the Kafka topic to which this sink should write its records
         * @param partitioner the function that should be used to determine the partition for each record processed by the sink
         * @param parentNames the name of one or more source or processor nodes whose output records this sink should consume
         * and write to its topic
         * @return itself
         * @throws TopologyException if parent processor is not added yet, or if this processor's name is equal to the parent's name
         * @see #addSink(string, string, string[])
         * @see #addSink(string, string, ISerializer, ISerializer, string[])
         * @see #addSink(string, string, ISerializer, ISerializer, StreamPartitioner, string[])
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSink<K, V>(
            string name,
            string topic,
            IStreamPartitioner<K, V> partitioner,
            string[] parentNames)
        {
            internalTopologyBuilder.addSink(name, topic, null, null, partitioner, parentNames);
            return this;
        }

        /**
         * Add a new sink that forwards records from upstream parent processor and/or source nodes to the named Kafka topic.
         * The sink will use the specified key and value serializers.
         *
         * @param name the unique name of the sink
         * @param topic the name of the Kafka topic to which this sink should write its records
         * @param keySerializer the {@link ISerializer key serializer} used when consuming records; may be null if the sink
         * should use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} specified in the
         * {@link StreamsConfig stream configuration}
         * @param valueSerializer the {@link ISerializer value serializer} used when consuming records; may be null if the sink
         * should use the {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
         * {@link StreamsConfig stream configuration}
         * @param parentNames the name of one or more source or processor nodes whose output records this sink should consume
         * and write to its topic
         * @return itself
         * @throws TopologyException if parent processor is not added yet, or if this processor's name is equal to the parent's name
         * @see #addSink(string, string, string[])
         * @see #addSink(string, string, StreamPartitioner, string[])
         * @see #addSink(string, string, ISerializer, ISerializer, StreamPartitioner, string[])
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSink<K, V>(
            string name,
            string topic,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer,
            string[] parentNames)
        {
            internalTopologyBuilder.addSink(name, topic, keySerializer, valueSerializer, null, parentNames);
            return this;
        }

        /**
         * Add a new sink that forwards records from upstream parent processor and/or source nodes to the named Kafka topic.
         * The sink will use the specified key and value serializers, and the supplied partitioner.
         *
         * @param name the unique name of the sink
         * @param topic the name of the Kafka topic to which this sink should write its records
         * @param keySerializer the {@link ISerializer key serializer} used when consuming records; may be null if the sink
         * should use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} specified in the
         * {@link StreamsConfig stream configuration}
         * @param valueSerializer the {@link ISerializer value serializer} used when consuming records; may be null if the sink
         * should use the {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
         * {@link StreamsConfig stream configuration}
         * @param partitioner the function that should be used to determine the partition for each record processed by the sink
         * @param parentNames the name of one or more source or processor nodes whose output records this sink should consume
         * and write to its topic
         * @return itself
         * @throws TopologyException if parent processor is not added yet, or if this processor's name is equal to the parent's name
         * @see #addSink(string, string, string[])
         * @see #addSink(string, string, StreamPartitioner, string[])
         * @see #addSink(string, string, ISerializer, ISerializer, string[])
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSink<K, V>(
            string name,
            string topic,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer,
            IStreamPartitioner<K, V> partitioner,
            string[] parentNames)
        {
            internalTopologyBuilder.addSink(name, topic, keySerializer, valueSerializer, partitioner, parentNames);
            return this;
        }

        /**
         * Add a new sink that forwards records from upstream parent processor and/or source nodes to Kafka topics based on {@code topicExtractor}.
         * The topics that it may ever send to should be pre-created.
         * The sink will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} and
         * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
         * {@link StreamsConfig stream configuration}.
         *
         * @param name              the unique name of the sink
         * @param topicExtractor    the extractor to determine the name of the Kafka topic to which this sink should write for each record
         * @param parentNames       the name of one or more source or processor nodes whose output records this sink should consume
         *                          and dynamically write to topics
         * @return                  itself
         * @throws TopologyException if parent processor is not added yet, or if this processor's name is equal to the parent's name
         * @see #addSink(string, string, StreamPartitioner, string[])
         * @see #addSink(string, string, ISerializer, ISerializer, string[])
         * @see #addSink(string, string, ISerializer, ISerializer, StreamPartitioner, string[])
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSink<K, V>(
            string name,
            ITopicNameExtractor<K, V> topicExtractor,
            string[] parentNames)
        {
            internalTopologyBuilder.addSink(name, topicExtractor, null, null, null, parentNames);
            return this;
        }

        /**
         * Add a new sink that forwards records from upstream parent processor and/or source nodes to Kafka topics based on {@code topicExtractor},
         * using the supplied partitioner.
         * The topics that it may ever send to should be pre-created.
         * The sink will use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} and
         * {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
         * {@link StreamsConfig stream configuration}.
         * <p>
         * The sink will also use the specified {@link StreamPartitioner} to determine how records are distributed among
         * the named Kafka topic's partitions.
         * Such control is often useful with topologies that use {@link #addStateStore(IStoreBuilder, string[]) state
         * stores} in its processors.
         * In most other cases, however, a partitioner needs not be specified and Kafka will automatically distribute
         * records among partitions using Kafka's default partitioning logic.
         *
         * @param name              the unique name of the sink
         * @param topicExtractor    the extractor to determine the name of the Kafka topic to which this sink should write for each record
         * @param partitioner       the function that should be used to determine the partition for each record processed by the sink
         * @param parentNames       the name of one or more source or processor nodes whose output records this sink should consume
         *                          and dynamically write to topics
         * @return                  itself
         * @throws TopologyException if parent processor is not added yet, or if this processor's name is equal to the parent's name
         * @see #addSink(string, string, string[])
         * @see #addSink(string, string, ISerializer, ISerializer, string[])
         * @see #addSink(string, string, ISerializer, ISerializer, StreamPartitioner, string[])
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSink<K, V>(
            string name,
            ITopicNameExtractor<K, V> topicExtractor,
            IStreamPartitioner<K, V> partitioner,
            string[] parentNames)
        {
            internalTopologyBuilder.addSink(name, topicExtractor, null, null, partitioner, parentNames);
            return this;
        }

        /**
         * Add a new sink that forwards records from upstream parent processor and/or source nodes to Kafka topics based on {@code topicExtractor}.
         * The topics that it may ever send to should be pre-created.
         * The sink will use the specified key and value serializers.
         *
         * @param name              the unique name of the sink
         * @param topicExtractor    the extractor to determine the name of the Kafka topic to which this sink should write for each record
         * @param keySerializer     the {@link ISerializer key serializer} used when consuming records; may be null if the sink
         *                          should use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} specified in the
         *                          {@link StreamsConfig stream configuration}
         * @param valueSerializer   the {@link ISerializer value serializer} used when consuming records; may be null if the sink
         *                          should use the {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
         *                          {@link StreamsConfig stream configuration}
         * @param parentNames       the name of one or more source or processor nodes whose output records this sink should consume
         *                          and dynamically write to topics
         * @return                  itself
         * @throws TopologyException if parent processor is not added yet, or if this processor's name is equal to the parent's name
         * @see #addSink(string, string, string[])
         * @see #addSink(string, string, StreamPartitioner, string[])
         * @see #addSink(string, string, ISerializer, ISerializer, StreamPartitioner, string[])
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSink<K, V>(
            string name,
            ITopicNameExtractor<K, V> topicExtractor,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer,
            string[] parentNames)
        {
            internalTopologyBuilder.addSink(name, topicExtractor, keySerializer, valueSerializer, null, parentNames);
            return this;
        }

        /**
         * Add a new sink that forwards records from upstream parent processor and/or source nodes to Kafka topics based on {@code topicExtractor}.
         * The topics that it may ever send to should be pre-created.
         * The sink will use the specified key and value serializers, and the supplied partitioner.
         *
         * @param name              the unique name of the sink
         * @param topicExtractor    the extractor to determine the name of the Kafka topic to which this sink should write for each record
         * @param keySerializer     the {@link ISerializer key serializer} used when consuming records; may be null if the sink
         *                          should use the {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default key serializer} specified in the
         *                          {@link StreamsConfig stream configuration}
         * @param valueSerializer   the {@link ISerializer value serializer} used when consuming records; may be null if the sink
         *                          should use the {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default value serializer} specified in the
         *                          {@link StreamsConfig stream configuration}
         * @param partitioner       the function that should be used to determine the partition for each record processed by the sink
         * @param parentNames       the name of one or more source or processor nodes whose output records this sink should consume
         *                          and dynamically write to topics
         * @return                  itself
         * @throws TopologyException if parent processor is not added yet, or if this processor's name is equal to the parent's name
         * @see #addSink(string, string, string[])
         * @see #addSink(string, string, StreamPartitioner, string[])
         * @see #addSink(string, string, ISerializer, ISerializer, string[])
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addSink<K, V>(
            string name,
            ITopicNameExtractor<K, V> topicExtractor,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer,
            IStreamPartitioner<K, V> partitioner,
            string[] parentNames)
        {
            internalTopologyBuilder.addSink(name, topicExtractor, keySerializer, valueSerializer, partitioner, parentNames);
            return this;
        }

        /**
         * Add a new processor node that receives and processes records output by one or more parent source or processor
         * node.
         * Any new record output by this processor will be forwarded to its child processor or sink nodes.
         *
         * @param name the unique name of the processor node
         * @param supplier the supplier used to obtain this node's {@link IProcessor} instance
         * @param parentNames the name of one or more source or processor nodes whose output records this processor should receive
         * and process
         * @return itself
         * @throws TopologyException if parent processor is not added yet, or if this processor's name is equal to the parent's name
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addProcessor<K, V>(
            string name,
            IProcessorSupplier<K, V> supplier,
            string[] parentNames)
        {
            internalTopologyBuilder.addProcessor(name, supplier, parentNames);
            return this;
        }

        /**
         * Adds a state store.
         *
         * @param storeBuilder the storeBuilder used to obtain this state store {@link StateStore} instance
         * @param processorNames the names of the processors that should be able to access the provided store
         * @return itself
         * @throws TopologyException if state store supplier is already added
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addStateStore<T>(
            IStoreBuilder<T> storeBuilder,
            string[] processorNames)
            where T : IStateStore
        {
            internalTopologyBuilder.addStateStore<T>(
                storeBuilder,
                allowOverride: false,
                processorNames);

            return this;
        }

        /**
         * Adds a global {@link StateStore} to the topology.
         * The {@link StateStore} sources its data from all partitions of the provided input topic.
         * There will be exactly one instance of this {@link StateStore} per Kafka Streams instance.
         * <p>
         * A {@link SourceNode} with the provided sourceName will be added to consume the data arriving from the partitions
         * of the input topic.
         * <p>
         * The provided {@link IProcessorSupplier} will be used to create an {@link ProcessorNode} that will receive all
         * records forwarded from the {@link SourceNode}.
         * This {@link ProcessorNode} should be used to keep the {@link StateStore} up-to-date.
         * The default {@link ITimestampExtractor} as specified in the {@link StreamsConfig config} is used.
         *
         * @param storeBuilder          user defined state store builder
         * @param sourceName            name of the {@link SourceNode} that will be automatically added
         * @param keyDeserializer       the {@link IDeserializer} to deserialize keys with
         * @param valueDeserializer     the {@link IDeserializer} to deserialize values with
         * @param topic                 the topic to source the data from
         * @param processorName         the name of the {@link IProcessorSupplier}
         * @param stateUpdateSupplier   the instance of {@link IProcessorSupplier}
         * @return itself
         * @throws TopologyException if the processor of state is already registered
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addGlobalStore<K, V, T>(
            IStoreBuilder<T> storeBuilder,
            string sourceName,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valueDeserializer,
            string topic,
            string processorName,
            IProcessorSupplier<K, V> stateUpdateSupplier)
            where T : IStateStore
        {
            internalTopologyBuilder.addGlobalStore(storeBuilder, sourceName, null, keyDeserializer,
                valueDeserializer, topic, processorName, stateUpdateSupplier);

            return this;
        }

        /**
         * Adds a global {@link StateStore} to the topology.
         * The {@link StateStore} sources its data from all partitions of the provided input topic.
         * There will be exactly one instance of this {@link StateStore} per Kafka Streams instance.
         * <p>
         * A {@link SourceNode} with the provided sourceName will be added to consume the data arriving from the partitions
         * of the input topic.
         * <p>
         * The provided {@link IProcessorSupplier} will be used to create an {@link ProcessorNode} that will receive all
         * records forwarded from the {@link SourceNode}.
         * This {@link ProcessorNode} should be used to keep the {@link StateStore} up-to-date.
         *
         * @param storeBuilder          user defined key value store builder
         * @param sourceName            name of the {@link SourceNode} that will be automatically added
         * @param timestampExtractor    the stateless timestamp extractor used for this source,
         *                              if not specified the default extractor defined in the configs will be used
         * @param keyDeserializer       the {@link IDeserializer} to deserialize keys with
         * @param valueDeserializer     the {@link IDeserializer} to deserialize values with
         * @param topic                 the topic to source the data from
         * @param processorName         the name of the {@link IProcessorSupplier}
         * @param stateUpdateSupplier   the instance of {@link IProcessorSupplier}
         * @return itself
         * @throws TopologyException if the processor of state is already registered
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology addGlobalStore<K, V, T>(
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
            internalTopologyBuilder.addGlobalStore(storeBuilder, sourceName, timestampExtractor, keyDeserializer,
                valueDeserializer, topic, processorName, stateUpdateSupplier);

            return this;
        }

        /**
         * Connects the processor and the state stores.
         *
         * @param processorName the name of the processor
         * @param stateStoreNames the names of state stores that the processor uses
         * @return itself
         * @throws TopologyException if the processor or a state store is unknown
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Topology connectProcessorAndStateStores(
            string processorName,
            string[] stateStoreNames)
        {
            internalTopologyBuilder.connectProcessorAndStateStores(processorName, stateStoreNames);
            return this;
        }

        /**
         * Returns a description of the specified {@code Topology}.
         *
         * @return a description of the topology.
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public TopologyDescription describe()
        {
            return internalTopologyBuilder.describe();
        }
    }
}
