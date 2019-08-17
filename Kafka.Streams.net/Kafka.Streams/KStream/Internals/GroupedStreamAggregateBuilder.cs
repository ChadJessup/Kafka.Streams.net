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
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Internals.Graph;
//using Kafka.Streams.Processor.Interfaces;
//using Kafka.Streams.State;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class GroupedStreamAggregateBuilder<K, V>
//    {
//        private InternalStreamsBuilder builder;
//        private ISerde<K> keySerde;
//        private ISerde<V> valueSerde;
//        private bool repartitionRequired;
//        private string userProvidedRepartitionTopicName;
//        private HashSet<string> sourceNodes;
//        private string name;
//        private StreamsGraphNode streamsGraphNode;
//        private StreamsGraphNode repartitionNode;

//        public IInitializer<long> countInitializer;// = () => 0L;
//        public IAggregator<K, V, long> countAggregator;// = (aggKey, value, aggregate) => aggregate + 1;
//        public IInitializer<V> reduceInitializer;// = () => null;

//        public GroupedStreamAggregateBuilder(
//            InternalStreamsBuilder builder,
//            GroupedInternal<K, V> groupedInternal,
//            bool repartitionRequired,
//            HashSet<string> sourceNodes,
//            string name,
//            StreamsGraphNode streamsGraphNode)
//        {
//            this.builder = builder;
//            this.keySerde = groupedInternal.keySerde;
//            this.valueSerde = groupedInternal.valueSerde;
//            this.repartitionRequired = repartitionRequired;
//            this.sourceNodes = sourceNodes;
//            this.name = name;
//            this.streamsGraphNode = streamsGraphNode;
//            this.userProvidedRepartitionTopicName = groupedInternal.name;
//        }

//        public IKTable<KR, VR> build<KR, VR>(
//            string functionName,
//            IStoreBuilder<IStateStore> storeBuilder,
//            IKStreamAggProcessorSupplier<K, KR, V, VR> aggregateSupplier,
//            string queryableStoreName,
//            ISerde<KR> keySerde,
//            ISerde<VR> valSerde)
//        {
//            if(queryableStoreName == null || queryableStoreName.Equals(storeBuilder.name))
//            {

//            }

//            string aggFunctionName = builder.NewProcessorName(functionName);

//            string sourceName = this.name;
//            StreamsGraphNode parentNode = streamsGraphNode;

//            if (repartitionRequired)
//            {
//                OptimizableRepartitionNodeBuilder<K, V> repartitionNodeBuilder = optimizableRepartitionNodeBuilder();
//                string repartitionTopicPrefix = userProvidedRepartitionTopicName != null ? userProvidedRepartitionTopicName : storeBuilder.name;
//                sourceName = createRepartitionSource(repartitionTopicPrefix, repartitionNodeBuilder);

//                // First time through we need to create a repartition node.
//                // Any subsequent calls to GroupedStreamAggregateBuilder#build we check if
//                // the user has provided a name for the repartition topic, is so we re-use
//                // the existing repartition node, otherwise we create a new one.
//                if (repartitionNode == null || userProvidedRepartitionTopicName == null)
//                {
//                    repartitionNode = repartitionNodeBuilder.build();
//                }

//                builder.addGraphNode(parentNode, repartitionNode);
//                parentNode = repartitionNode;
//            }

//            StatefulProcessorNode<K, V> statefulProcessorNode =
//               new StatefulProcessorNode<K, V>(
//                   aggFunctionName,
//                   new ProcessorParameters<K, V>(aggregateSupplier, aggFunctionName),
//                   storeBuilder);

//            builder.addGraphNode(parentNode, statefulProcessorNode);

//            return new KTable<>(aggFunctionName,
//                                    keySerde,
//                                    valSerde,
//                                    sourceName.Equals(this.name) ? sourceNodes : Collections.singleton(sourceName),
//                                    queryableStoreName,
//                                    aggregateSupplier,
//                                    statefulProcessorNode,
//                                    builder);

//        }

//        /**
//         * @return the new sourceName of the repartitioned source
//         */
//        private string createRepartitionSource(string repartitionTopicNamePrefix,
//                                                OptimizableRepartitionNodeBuilder<K, V> optimizableRepartitionNodeBuilder)
//        {

//            return KStream<K, V>.createRepartitionedSource(
//                builder,
//                keySerde,
//                valueSerde,
//                repartitionTopicNamePrefix,
//                optimizableRepartitionNodeBuilder);

//        }
//    }
//}
