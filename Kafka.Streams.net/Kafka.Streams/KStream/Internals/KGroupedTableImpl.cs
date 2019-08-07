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
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.State.Internals;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{





















    /**
     * The implementation of {@link KGroupedTable}.
     *
     * @param the key type
     * @param the value type
     */
    public class KGroupedTableImpl<K, V> : AbstractStream<K, V>, KGroupedTable<K, V>
    {

        private static string AGGREGATE_NAME = "KTABLE-AGGREGATE-";

        private static string REDUCE_NAME = "KTABLE-REDUCE-";

        private string userProvidedRepartitionTopicName;

        private Initializer<long> countInitializer = ()-> 0L;

        private Aggregator<K, V, long> countAdder = (aggKey, value, aggregate)->aggregate + 1L;

        private Aggregator<K, V, long> countSubtractor = (aggKey, value, aggregate)->aggregate - 1L;

        private StreamsGraphNode repartitionGraphNode;

        KGroupedTableImpl(InternalStreamsBuilder builder,
                           string name,
                           HashSet<string> sourceNodes,
                           GroupedInternal<K, V> groupedInternal,
                           StreamsGraphNode streamsGraphNode)
            : base(name, groupedInternal.keySerde(), groupedInternal.valueSerde(), sourceNodes, streamsGraphNode, builder)
        {

            this.userProvidedRepartitionTopicName = groupedInternal.name();
        }

        private IKTable<K, T> doAggregate(ProcessorSupplier<K, Change<V>> aggregateSupplier,
                                              string functionName,
                                              MaterializedInternal<K, T, IKeyValueStore<Bytes, byte[]>> materialized)
        {

            string sinkName = builder.newProcessorName(KStreamImpl.SINK_NAME);
            string sourceName = builder.newProcessorName(KStreamImpl.SOURCE_NAME);
            string funcName = builder.newProcessorName(functionName);
            string repartitionTopic = (userProvidedRepartitionTopicName != null ? userProvidedRepartitionTopicName : materialized.storeName())
               + KStreamImpl.REPARTITION_TOPIC_SUFFIX;

            if (repartitionGraphNode == null || userProvidedRepartitionTopicName == null)
            {
                repartitionGraphNode = createRepartitionNode(sinkName, sourceName, repartitionTopic);
            }


            // the passed in StreamsGraphNode must be the parent of the repartition node
            builder.addGraphNode(this.streamsGraphNode, repartitionGraphNode);

            StatefulProcessorNode statefulProcessorNode = new StatefulProcessorNode<>(
               funcName,
               new ProcessorParameters<>(aggregateSupplier, funcName),
               new TimestampedKeyValueStoreMaterializer<>(materialized).materialize()
           );

            // now the repartition node must be the parent of the StateProcessorNode
            builder.addGraphNode(repartitionGraphNode, statefulProcessorNode);

            // return the KTable representation with the intermediate topic as the sources
            return new KTableImpl<>(funcName,
                                    materialized.keySerde(),
                                    materialized.valueSerde(),
                                    Collections.singleton(sourceName),
                                    materialized.queryableStoreName(),
                                    aggregateSupplier,
                                    statefulProcessorNode,
                                    builder);
        }

        private GroupedTableOperationRepartitionNode<K, V> createRepartitionNode(
            string sinkName,
            string sourceName,
            string topic)
        {
            //return GroupedTableOperationRepartitionNode.< K, V > groupedTableOperationNodeBuilder()
            //     .withRepartitionTopic(topic)
            //     .withSinkName(sinkName)
            //     .withSourceName(sourceName)
            //     .withKeySerde(keySerde)
            //     .withValueSerde(valSerde)
            //     .withNodeName(sourceName).build();
        }


        public IKTable<K, V> reduce(Reducer<V> adder,
                                    Reducer<V> subtractor,
                                    Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            Objects.requireNonNull(adder, "adder can't be null");
            subtractor = subtractor ?? throw new System.ArgumentNullException("subtractor can't be null", nameof(subtractor));
            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));
            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal =
               new MaterializedInternal<K, V>(materialized, builder, AGGREGATE_NAME);

            if (materializedInternal.keySerde() == null)
            {
                materializedInternal.withKeySerde(keySerde);
            }
            if (materializedInternal.valueSerde() == null)
            {
                materializedInternal.withValueSerde(valSerde);
            }
            ProcessorSupplier<K, Change<V>> aggregateSupplier = new KTableReduce<>(materializedInternal.storeName(),

                                                                                       .Adder,
                                                                                        subtractor);
            return doAggregate(aggregateSupplier, REDUCE_NAME, materializedInternal);
        }


        public IKTable<K, V> reduce(Reducer<V> adder,
                                    Reducer<V> subtractor)
        {
            return reduce(adder, subtractor, Materialized.with(keySerde, valSerde));
        }


        public IKTable<K, long> count(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            MaterializedInternal<K, long, IKeyValueStore<Bytes, byte[]>> materializedInternal =
               new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);

            if (materializedInternal.keySerde() == null)
            {
                materializedInternal.withKeySerde(keySerde);
            }
            if (materializedInternal.valueSerde() == null)
            {
                materializedInternal.withValueSerde(Serdes.long());
            }

            ProcessorSupplier<K, Change<V>> aggregateSupplier = new KTableAggregate<>(materializedInternal.storeName(),
                                                                                           countInitializer,
                                                                                           countAdder,
                                                                                           countSubtractor);

            return doAggregate(aggregateSupplier, AGGREGATE_NAME, materializedInternal);
        }


        public IKTable<K, long> count()
        {
            return count(Materialized.with(keySerde, Serdes.long()));
        }


        public IKTable<K, VR> aggregate<VR>(Initializer<VR> initializer,
                                             Aggregator<K, V, VR> adder,
                                             Aggregator<K, V, VR> subtractor,
                                             Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            initializer = initializer ?? throw new System.ArgumentNullException("initializer can't be null", nameof(initializer));
            //Objects.requireNonNull(adder, "adder can't be null");
            subtractor = subtractor ?? throw new System.ArgumentNullException("subtractor can't be null", nameof(subtractor));
            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));

            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal =
               new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);

            if (materializedInternal.keySerde() == null)
            {
                materializedInternal.withKeySerde(keySerde);
            }
            ProcessorSupplier<K, Change<V>> aggregateSupplier = new KTableAggregate<>(materializedInternal.storeName(),
                                                                                           initializer,

                                                                                          .Adder,
                                                                                           subtractor);
            return doAggregate(aggregateSupplier, AGGREGATE_NAME, materializedInternal);
        }


        public IKTable<K, T> aggregate(Initializer<T> initializer,
                                           Aggregator<K, V, T>.Adder,
                                           Aggregator<K, V, T> subtractor)
        {
            return aggregate(initializer,.Adder, subtractor, Materialized.with(keySerde, null));
        }

    }
}