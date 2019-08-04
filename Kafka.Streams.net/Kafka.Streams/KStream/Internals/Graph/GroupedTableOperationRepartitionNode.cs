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








public class GroupedTableOperationRepartitionNode<K, V> : BaseRepartitionNode<K, V>
{
    private GroupedTableOperationRepartitionNode( string nodeName,
                                                  ISerde<K> keySerde,
                                                  ISerde<V> valueSerde,
                                                  string sinkName,
                                                  string sourceName,
                                                  string repartitionTopic,
                                                  ProcessorParameters processorParameters)
{

        base(
            nodeName,
            sourceName,
            processorParameters,
            keySerde,
            valueSerde,
            sinkName,
            repartitionTopic
        );
    }


    Serializer<V> getValueSerializer()
{
         Serializer<V> valueSerializer = valueSerde == null ? null : valueSerde.serializer();
        return unsafeCastChangedToValueSerializer(valueSerializer);
    }


    private Serializer<V> unsafeCastChangedToValueSerializer( Serializer<V> valueSerializer)
{
        return (Serializer<V>) new ChangedSerializer<>(valueSerializer);
    }


    Deserializer<V> getValueDeserializer()
{
         Deserializer<V> valueDeserializer = valueSerde == null ? null : valueSerde.deserializer();
        return unsafeCastChangedToValueDeserializer(valueDeserializer);
    }


    private Deserializer<V> unsafeCastChangedToValueDeserializer( Deserializer<V> valueDeserializer)
{
        return (Deserializer<V>) new ChangedDeserializer<>(valueDeserializer);
    }


    public string ToString()
{
        return "GroupedTableOperationRepartitionNode{} " + base.ToString();
    }


    public void writeToTopology( InternalTopologyBuilder topologyBuilder)
{
         Serializer<K> keySerializer = keySerde != null ? keySerde.serializer() : null;
         Deserializer<K> keyDeserializer = keySerde != null ? keySerde.deserializer() : null;


        topologyBuilder.AddInternalTopic(repartitionTopic);

        topologyBuilder.AddSink(
            sinkName,
            repartitionTopic,
            keySerializer,
            getValueSerializer(),
            null,
            parentNodeNames()
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

    public static GroupedTableOperationRepartitionNodeBuilder<K1, V1> groupedTableOperationNodeBuilder()
{
        return new GroupedTableOperationRepartitionNodeBuilder<>();
    }

    public static  GroupedTableOperationRepartitionNodeBuilder<K, V> {

        private ISerde<K> keySerde;
        private ISerde<V> valueSerde;
        private string sinkName;
        private string nodeName;
        private string sourceName;
        private string repartitionTopic;
        private ProcessorParameters processorParameters;

        private GroupedTableOperationRepartitionNodeBuilder()
{
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> withKeySerde( ISerde<K> keySerde)
{
            this.keySerde = keySerde;
            return this;
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> withValueSerde( ISerde<V> valueSerde)
{
            this.valueSerde = valueSerde;
            return this;
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> withSinkName( string sinkName)
{
            this.sinkName = sinkName;
            return this;
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> withNodeName( string nodeName)
{
            this.nodeName = nodeName;
            return this;
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> withSourceName( string sourceName)
{
            this.sourceName = sourceName;
            return this;
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> withRepartitionTopic( string repartitionTopic)
{
            this.repartitionTopic = repartitionTopic;
            return this;
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> withProcessorParameters( ProcessorParameters processorParameters)
{
            this.processorParameters = processorParameters;
            return this;
        }

        public GroupedTableOperationRepartitionNode<K, V> build()
{
            return new GroupedTableOperationRepartitionNode<>(
                nodeName,
                keySerde,
                valueSerde,
                sinkName,
                sourceName,
                repartitionTopic,
                processorParameters
            );
        }
    }
}
}