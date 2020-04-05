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

namespace Kafka.Streams.KStream.Internals.Graph
{
    public abstract class BaseRepartitionNode<K, V> : StreamsGraphNode
    {
        public ISerde<K>? KeySerde { get; protected set; }
        public ISerde<V>? ValueSerde { get; protected set; }

        protected string SinkName { get; }
        protected string SourceName { get; }
        protected string RepartitionTopic { get; }
        protected ProcessorParameters<K, V> ProcessorParameters { get; }

        public BaseRepartitionNode(
            string nodeName,
            string sourceName,
            ProcessorParameters<K, V> processorParameters,
            ISerde<K>? keySerde,
            ISerde<V>? valueSerde,
            string sinkName,
            string repartitionTopic)
            : base(nodeName)
        {
            this.KeySerde = keySerde;
            this.ValueSerde = valueSerde;
            this.SinkName = sinkName;
            this.SourceName = sourceName;
            this.RepartitionTopic = repartitionTopic;
            this.ProcessorParameters = processorParameters;
        }

        public abstract ISerializer<V>? GetValueSerializer();

        public abstract IDeserializer<V>? GetValueDeserializer();

        public override string ToString()
            => "BaseRepartitionNode{" +
                    $"keySerde={KeySerde}" +
                    $", valueSerde={ValueSerde}" +
                    $", sinkName='{SinkName}'" +
                    $", sourceName='{SourceName}'" +
                    $", repartitionTopic='{RepartitionTopic}'" +
                    $", processorParameters={ProcessorParameters}" +
                    $"}} {base.ToString()}";
    }
}
