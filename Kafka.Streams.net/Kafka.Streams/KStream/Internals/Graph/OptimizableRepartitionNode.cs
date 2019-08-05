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

namespace Kafka.Streams.KStream.Internals.Graph
{









    public class OptimizableRepartitionNode<K, V> : BaseRepartitionNode<K, V>
    {

        OptimizableRepartitionNode(string nodeName,
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

        public ISerde<K> keySerde()
        {
            return keySerde;
        }

        public ISerde<V> valueSerde()
        {
            return valueSerde;
        }

        public string repartitionTopic()
        {
            return repartitionTopic;
        }


        ISerializer<V> getValueSerializer()
        {
            return valueSerde != null ? valueSerde.serializer() : null;
        }


        IDeserializer<V> getValueDeserializer()
        {
            return valueSerde != null ? valueSerde.deserializer() : null;
        }


        public string ToString()
        {
            return "OptimizableRepartitionNode{ " + base.ToString() + " }";
        }


        public void writeToTopology(InternalTopologyBuilder topologyBuilder)
        {
            ISerializer<K> keySerializer = keySerde != null ? keySerde.serializer() : null;
            IDeserializer<K> keyDeserializer = keySerde != null ? keySerde.deserializer() : null;

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

            topologyBuilder.AddSource(
                null,
                sourceName,
                new FailOnInvalidTimestamp(),
                keyDeserializer,
                getValueDeserializer(),
                repartitionTopic
            );

        }

        public staticOptimizableRepartitionNodeBuilder<K, V> optimizableRepartitionNodeBuilder()
        {
            return new OptimizableRepartitionNodeBuilder<>();
        }


        public static OptimizableRepartitionNodeBuilder<K, V> {

        private string nodeName;
        private ProcessorParameters processorParameters;
        private ISerde<K> keySerde;
        private ISerde<V> valueSerde;
        private string sinkName;
        private string sourceName;
        private string repartitionTopic;

        private OptimizableRepartitionNodeBuilder()
        {
        }

        public OptimizableRepartitionNodeBuilder<K, V> withProcessorParameters(ProcessorParameters processorParameters)
        {
            this.processorParameters = processorParameters;
            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> withKeySerde(ISerde<K> keySerde)
        {
            this.keySerde = keySerde;
            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> withValueSerde(ISerde<V> valueSerde)
        {
            this.valueSerde = valueSerde;
            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> withSinkName(string sinkName)
        {
            this.sinkName = sinkName;
            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> withSourceName(string sourceName)
        {
            this.sourceName = sourceName;
            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> withRepartitionTopic(string repartitionTopic)
        {
            this.repartitionTopic = repartitionTopic;
            return this;
        }


        public OptimizableRepartitionNodeBuilder<K, V> withNodeName(string nodeName)
        {
            this.nodeName = nodeName;
            return this;
        }

        public OptimizableRepartitionNode<K, V> build()
        {

            return new OptimizableRepartitionNode<>(
                nodeName,
                sourceName,
                processorParameters,
                keySerde,
                valueSerde,
                sinkName,
                repartitionTopic
            );

        }
    }
}
