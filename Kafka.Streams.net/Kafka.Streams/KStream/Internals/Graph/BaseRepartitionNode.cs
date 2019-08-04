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

namespace Kafka.streams.kstream.internals.graph;





public abstract class BaseRepartitionNode<K, V> : StreamsGraphNode {

    protected  ISerde<K> keySerde;
    protected  ISerde<V> valueSerde;
    protected  string sinkName;
    protected  string sourceName;
    protected  string repartitionTopic;
    protected  ProcessorParameters processorParameters;


    BaseRepartitionNode( string nodeName,
                         string sourceName,
                         ProcessorParameters processorParameters,
                         ISerde<K> keySerde,
                         ISerde<V> valueSerde,
                         string sinkName,
                         string repartitionTopic)
{

        super(nodeName);

        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.sinkName = sinkName;
        this.sourceName = sourceName;
        this.repartitionTopic = repartitionTopic;
        this.processorParameters = processorParameters;
    }

    abstract Serializer<V> getValueSerializer();

    abstract Deserializer<V> getValueDeserializer();

    
    public string ToString()
{
        return "BaseRepartitionNode{" +
               "keySerde=" + keySerde +
               ", valueSerde=" + valueSerde +
               ", sinkName='" + sinkName + '\'' +
               ", sourceName='" + sourceName + '\'' +
               ", repartitionTopic='" + repartitionTopic + '\'' +
               ", processorParameters=" + processorParameters +
               "} " + super.ToString();
    }
}
