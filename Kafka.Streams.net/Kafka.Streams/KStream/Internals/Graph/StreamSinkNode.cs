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










public StreamSinkNode<K, V> : StreamsGraphNode
{


    private  TopicNameExtractor<K, V> topicNameExtractor;
    private  ProducedInternal<K, V> producedInternal;

    public StreamSinkNode( string nodeName,
                           TopicNameExtractor<K, V> topicNameExtractor,
                           ProducedInternal<K, V> producedInternal)
{

        base(nodeName);

        this.topicNameExtractor = topicNameExtractor;
        this.producedInternal = producedInternal;
    }


    
    public string ToString()
{
        return "StreamSinkNode{" +
               "topicNameExtractor=" + topicNameExtractor +
               ", producedInternal=" + producedInternal +
               "} " + base.ToString();
    }

    
    public void writeToTopology( InternalTopologyBuilder topologyBuilder)
{
         Serializer<K> keySerializer = producedInternal.keySerde() == null ? null : producedInternal.keySerde().serializer();
         Serializer<V> valSerializer = producedInternal.valueSerde() == null ? null : producedInternal.valueSerde().serializer();
         StreamPartitioner<K, V> partitioner = producedInternal.streamPartitioner();
         string[] parentNames = parentNodeNames();

        if (partitioner == null && keySerializer is WindowedSerializer)
{
            
             StreamPartitioner<K, V> windowedPartitioner = (StreamPartitioner<K, V>) new WindowedStreamPartitioner<object, V>((WindowedSerializer) keySerializer);
            topologyBuilder.AddSink(nodeName(), topicNameExtractor, keySerializer, valSerializer, windowedPartitioner, parentNames);
        } else
{

            topologyBuilder.AddSink(nodeName(), topicNameExtractor, keySerializer, valSerializer, partitioner,  parentNames);
        }
    }

}
