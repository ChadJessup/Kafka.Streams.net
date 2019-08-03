/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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

package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.internals.ProducedInternal;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.kstream.internals.WindowedStreamPartitioner;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

public class StreamSinkNode<K, V> : StreamsGraphNode {

    private  TopicNameExtractor<K, V> topicNameExtractor;
    private  ProducedInternal<K, V> producedInternal;

    public StreamSinkNode( string nodeName,
                           TopicNameExtractor<K, V> topicNameExtractor,
                           ProducedInternal<K, V> producedInternal) {

        super(nodeName);

        this.topicNameExtractor = topicNameExtractor;
        this.producedInternal = producedInternal;
    }


    @Override
    public string toString() {
        return "StreamSinkNode{" +
               "topicNameExtractor=" + topicNameExtractor +
               ", producedInternal=" + producedInternal +
               "} " + super.toString();
    }

    @Override
    public void writeToTopology( InternalTopologyBuilder topologyBuilder) {
         Serializer<K> keySerializer = producedInternal.keySerde() == null ? null : producedInternal.keySerde().serializer();
         Serializer<V> valSerializer = producedInternal.valueSerde() == null ? null : producedInternal.valueSerde().serializer();
         StreamPartitioner<? super K, ? super V> partitioner = producedInternal.streamPartitioner();
         string[] parentNames = parentNodeNames();

        if (partitioner == null && keySerializer is WindowedSerializer) {
            @SuppressWarnings("unchecked")
             StreamPartitioner<K, V> windowedPartitioner = (StreamPartitioner<K, V>) new WindowedStreamPartitioner<Object, V>((WindowedSerializer) keySerializer);
            topologyBuilder.addSink(nodeName(), topicNameExtractor, keySerializer, valSerializer, windowedPartitioner, parentNames);
        } else {
            topologyBuilder.addSink(nodeName(), topicNameExtractor, keySerializer, valSerializer, partitioner,  parentNames);
        }
    }

}
