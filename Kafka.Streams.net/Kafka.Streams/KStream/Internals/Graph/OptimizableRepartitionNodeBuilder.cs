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

using Kafka.Streams.Interfaces;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class OptimizableRepartitionNodeBuilder<K, V>
    {
        private string nodeName;
        private ProcessorParameters<K, V> processorParameters;
        private ISerde<K> keySerde;
        private ISerde<V> valueSerde;
        private string sinkName;
        private string sourceName;
        private string repartitionTopic;

        public OptimizableRepartitionNodeBuilder<K, V> WithProcessorParameters(
            ProcessorParameters<K, V> processorParameters)
        {
            this.processorParameters = processorParameters;

            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> WithKeySerde(ISerde<K> keySerde)
        {
            this.keySerde = keySerde;

            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> WithValueSerde(ISerde<V> valueSerde)
        {
            this.valueSerde = valueSerde;

            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> WithSinkName(string sinkName)
        {
            this.sinkName = sinkName;

            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> WithSourceName(string sourceName)
        {
            this.sourceName = sourceName;

            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> WithRepartitionTopic(string repartitionTopic)
        {
            this.repartitionTopic = repartitionTopic;

            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> WithNodeName(string nodeName)
        {
            this.nodeName = nodeName;

            return this;
        }

        public OptimizableRepartitionNode<K, V> Build()
            => new OptimizableRepartitionNode<K, V>(
                nodeName,
                sourceName,
                processorParameters,
                keySerde,
                valueSerde,
                sinkName,
                repartitionTopic);
    }
}
