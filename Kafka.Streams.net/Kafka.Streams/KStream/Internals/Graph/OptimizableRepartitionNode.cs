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
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processor;
using Kafka.Streams.Processor.Internals;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class OptimizableRepartitionNode<K, V> : BaseRepartitionNode<K, V>
    {

        public OptimizableRepartitionNode(
            string nodeName,
            string sourceName,
            ProcessorParameters processorParameters,
            ISerde<K> keySerde,
            ISerde<V> valueSerde,
            string sinkName,
            string repartitionTopic)
            : base(
                nodeName,
                sourceName,
                processorParameters,
                keySerde,
                valueSerde,
                sinkName,
                repartitionTopic)
        {

        }

        public ISerde<K> keySerde
        {
            return keySerde;
        }

        public ISerde<V> valueSerde
        {
            return valueSerde;
        }

        public string repartitionTopic()
        {
            return repartitionTopic;
        }


        ISerializer<V> getValueSerializer()
        {
            return valueSerde != null ? valueSerde.Serializer() : null;
        }


        IDeserializer<V> getValueDeserializer()
        {
            return valueSerde != null ? valueSerde.Deserializer() : null;
        }


        public string ToString()
        {
            return "OptimizableRepartitionNode{ " + base.ToString() + " }";
        }


        public void writeToTopology(InternalTopologyBuilder topologyBuilder)
        {
            ISerializer<K> keySerializer = keySerde != null ? keySerde.Serializer() : null;
            IDeserializer<K> keyDeserializer = keySerde != null ? keySerde.Deserializer() : null;

            topologyBuilder.AddInternalTopic(repartitionTopic);

            topologyBuilder.AddProcessor(
                processorParameters.processorName(),
                processorParameters.processorSupplier(),
                parentNodeNames()
            );

            topologyBuilder.AddSink(
                sinkName,
                repartitionTopic,
                keySerializer,
                getValueSerializer(),
                null,
                processorParameters.processorName()
            );

            topologyBuilder.addSource(
                null,
                sourceName,
                new FailOnInvalidTimestamp(),
                keyDeserializer,
                getValueDeserializer(),
                repartitionTopic
            );

        }

        public static OptimizableRepartitionNodeBuilder<K, V> optimizableRepartitionNodeBuilder()
        {
            return new OptimizableRepartitionNodeBuilder<>();
        }
    }
}