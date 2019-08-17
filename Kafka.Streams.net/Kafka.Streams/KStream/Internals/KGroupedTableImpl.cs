///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements. See the NOTICE file distributed with
// * this work for.Additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//using Kafka.Common;
//using Kafka.Common.Utils;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Internals.Graph;
//using Kafka.Streams.Processor;
//using Kafka.Streams.State.Internals;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{





















//    /**
//     * The implementation of {@link KGroupedTable}.
//     *
//     * @param the key type
//     * @param the value type
//     */
//    public class KGroupedTableImpl<K, V> : AbstractStream<K, V>, IKGroupedTable<K, V>
//    {
//        private static string AGGREGATE_NAME = "KTABLE-AGGREGATE-";

//        private static string REDUCE_NAME = "KTABLE-REDUCE-";

//        private string userProvidedRepartitionTopicName;
//        private IInitializer<long> countInitializer = null;// ()=> 0L;
//        private IAggregator<K, V, long> countAdder = null;// (aggKey, value, aggregate)=>aggregate + 1L;
//        private IAggregator<K, V, long> countSubtractor = null; // (aggKey, value, aggregate)=>aggregate - 1L;

//        private StreamsGraphNode repartitionGraphNode;

//        public KGroupedTableImpl(InternalStreamsBuilder builder,
//                           string name,
//                           HashSet<string> sourceNodes,
//                           GroupedInternal<K, V> groupedInternal,
//                           StreamsGraphNode streamsGraphNode)
//            : base(name, groupedInternal.keySerde, groupedInternal.valueSerde, sourceNodes, streamsGraphNode, builder)
//        {

//            this.userProvidedRepartitionTopicName = groupedInternal.name;
//        }

//        private IKTable<K, V> doAggregate(
//            IProcessorSupplier<K, Change<V>> aggregateSupplier,
//            string functionName,
//            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
//        {
//            return null;

//            string sinkName = builder.NewProcessorName(KStream.SINK_NAME);
//            string sourceName = builder.NewProcessorName(KStream.SourceName);
//            string funcName = builder.NewProcessorName(functionName);
//            string repartitionTopic = (userProvidedRepartitionTopicName != null ? userProvidedRepartitionTopicName : materialized.storeName())
//               + KStream.RepartitionTopicSuffix;

//            if (repartitionGraphNode == null || userProvidedRepartitionTopicName == null)
//            {
//                repartitionGraphNode = createRepartitionNode(sinkName, sourceName, repartitionTopic);
//            }


//            // the passed in StreamsGraphNode must be the parent of the repartition node
//            builder.addGraphNode(this.streamsGraphNode, repartitionGraphNode);

//           // var statefulProcessorNode = new StatefulProcessorNode<K, V>(
//           //    funcName,
//           //    new ProcessorParameters<K, V>(aggregateSupplier, funcName),
//           //    new TimestampedKeyValueStoreMaterializer<K, V>(materialized).materialize()
//           //);

//           // // now the repartition node must be the parent of the StateProcessorNode
//           // builder.addGraphNode(repartitionGraphNode, statefulProcessorNode);

//           // // return the KTable representation with the intermediate topic as the sources
//           // return new KTable<>(funcName,
//           //                         materialized.keySerde,
//           //                         materialized.valueSerde,
//           //                         Collections.singleton(sourceName),
//           //                         materialized.queryableStoreName(),
//           //                         aggregateSupplier,
//           //                         statefulProcessorNode,
//           //                         builder);
//        }

//        private GroupedTableOperationRepartitionNode<K, V> createRepartitionNode(
//            string sinkName,
//            string sourceName,
//            string topic)
//        {
//            return null;
//            //return GroupedTableOperationRepartitionNode.< K, V > groupedTableOperationNodeBuilder()
//            //     .withRepartitionTopic(topic)
//            //     .withSinkName(sinkName)
//            //     .withSourceName(sourceName)
//            //     .withKeySerde(keySerde)
//            //     .withValueSerde(valSerde)
//            //     .withNodeName(sourceName).build();
//        }


//        public IKTable<K, V> reduce(IReducer<V> adder,
//                                    IReducer<V> subtractor,
//                                    Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
//        {
//            Objects.requireNonNull(adder, "adder can't be null");
//            subtractor = subtractor ?? throw new System.ArgumentNullException("subtractor can't be null", nameof(subtractor));
//            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));
//            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materializedInternal =
//               new MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>(materialized, builder, AGGREGATE_NAME);

//            if (materializedInternal.keySerde == null)
//            {
//                materializedInternal.withKeySerde(keySerde);
//            }
//            if (materializedInternal.valueSerde == null)
//            {
//                materializedInternal.withValueSerde(valSerde);
//            }

//            IProcessorSupplier<K, Change<V>> aggregateSupplier = 
//                new KTableReduce<K, V>(
//                materializedInternal.storeName(),
//                adder,
//                subtractor);

//            return doAggregate(aggregateSupplier, REDUCE_NAME, materializedInternal);
//        }


//        public IKTable<K, V> reduce(IReducer<V> adder,
//                                    IReducer<V> subtractor)
//        {
//            return null;
//            //return reduce(adder, subtractor, Materialized.with(keySerde, valSerde));
//        }


//        public IKTable<K, long> count(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized)
//        {
//            return null;
//            //MaterializedInternal<K, long, IKeyValueStore<Bytes, byte[]>> materializedInternal =
//            //   new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);

//            //if (materializedInternal.keySerde == null)
//            //{
//            //    materializedInternal.withKeySerde(keySerde);
//            //}
//            //if (materializedInternal.valueSerde == null)
//            //{
//            //    materializedInternal.withValueSerde(Serdes.Long());
//            //}

//            //IProcessorSupplier<K, Change<V>> aggregateSupplier = new KTableAggregate<>(materializedInternal.storeName(),
//            //                                                                               countInitializer,
//            //                                                                               countAdder,
//            //                                                                               countSubtractor);

//            //return doAggregate(aggregateSupplier, AGGREGATE_NAME, materializedInternal);
//        }


//        public IKTable<K, long> count()
//        {
//            return null;
//            //return count(Materialized<K, long>.with(keySerde, Serdes.Long()));
//        }


//        public IKTable<K, VR> aggregate<VR>(
//            IInitializer<VR> initializer,
//            IAggregator<K, V, VR> adder,
//            IAggregator<K, V, VR> subtractor,
//            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized)
//        {
//            initializer = initializer ?? throw new System.ArgumentNullException("initializer can't be null", nameof(initializer));
//            //Objects.requireNonNull(adder, "adder can't be null");
//            subtractor = subtractor ?? throw new System.ArgumentNullException("subtractor can't be null", nameof(subtractor));
//            materialized = materialized ?? throw new System.ArgumentNullException("materialized can't be null", nameof(materialized));

//            MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal =
//               new MaterializedInternal<K, VR, IKeyValueStore<Bytes, byte[]>>(materialized, builder, AGGREGATE_NAME);

//            if (materializedInternal.keySerde == null)
//            {
//                materializedInternal.withKeySerde(keySerde);
//            }
//            //            IProcessorSupplier<K, Change<V>> aggregateSupplier = new KTableAggregate<>(materializedInternal.storeName(),
//            //initializer,
//            //adder,
//            //subtractor);

//            return null;
//            //return doAggregate(aggregateSupplier, AGGREGATE_NAME, materializedInternal);
//        }


//        public IKTable<K, T> aggregate<T>(
//            IInitializer<T> initializer,
//            IAggregator<K, V, T> adder,
//            IAggregator<K, V, T> subtractor)
//        {
//            return null;
//           // return aggregate<K, V>(initializer, adder, subtractor, Materialized<K, V, T>.with(keySerde, null));
//        }

//        //public IKTable<K, VR> aggregate<VR>(
//        //    IInitializer<VR> initializer,
//        //    IAggregator<K, V, VR> adder,
//        //    IAggregator<K, V, VR> subtractor)
//        //{
//        //    throw new System.NotImplementedException();
//        //}
//    }
//}