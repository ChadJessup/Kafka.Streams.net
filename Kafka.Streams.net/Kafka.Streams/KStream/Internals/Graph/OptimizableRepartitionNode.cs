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

//using Confluent.Kafka;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Processor;
//using Kafka.Streams.Processor.Internals;

//namespace Kafka.Streams.KStream.Internals.Graph
//{
//    public class OptimizableRepartitionNode
//    {
//        public static OptimizableRepartitionNodeBuilder<K, V> optimizableRepartitionNodeBuilder<K, V>()
//        {
//            return new OptimizableRepartitionNodeBuilder<K, V>();
//        }
//    }

//    public class OptimizableRepartitionNode<K, V> : BaseRepartitionNode<K, V>
//    {
//        public OptimizableRepartitionNode(
//            string nodeName,
//            string sourceName,
//            ProcessorParameters<K, V> processorParameters,
//            ISerde<K> keySerde,
//            ISerde<V> valueSerde,
//            string sinkName,
//            string repartitionTopic)
//            : base(
//                nodeName,
//                sourceName,
//                processorParameters,
//                keySerde,
//                valueSerde,
//                sinkName,
//                repartitionTopic)
//        {

//        }

//        public override ISerializer<V> getValueSerializer()
//        {
//            return valueSerde != null
//                ? valueSerde.Serializer
//                : null;
//        }

//        public override IDeserializer<V> getValueDeserializer()
//        {
//            return valueSerde != null
//                ? valueSerde.Deserializer
//                : null;
//        }

//        public override string ToString()
//        {
//            return $"OptimizableRepartitionNode{{{base.ToString()}}}";
//        }


//        public override void writeToTopology(InternalTopologyBuilder topologyBuilder)
//        {
//            ISerializer<K> keySerializer = keySerde != null
//                ? keySerde.Serializer
//                : null;

//            IDeserializer<K> keyDeserializer = keySerde != null
//                ? keySerde.Deserializer
//                : null;

//            topologyBuilder.addInternalTopic(repartitionTopic);

//            topologyBuilder.addProcessor(
//                processorParameters.processorName,
//                processorParameters.IProcessorSupplier,
//                parentNodeNames());

//            topologyBuilder.addSink(
//                sinkName,
//                repartitionTopic,
//                keySerializer,
//                getValueSerializer(),
//                null,
//                new[] { processorParameters.processorName });

//            topologyBuilder.addSource(
//                AutoOffsetReset.Error,
//                sourceName,
//                new FailOnInvalidTimestamp(),
//                keyDeserializer,
//                getValueDeserializer(),
//                new[] { repartitionTopic });
//        }
//    }
//}