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

namespace Kafka.Streams.Processor.Internals
{
    private class SinkNodeFactory<K, V> : NodeFactory
    {

        private ISerializer<K> keySerializer;
        private ISerializer<V> valSerializer;
        private IStreamPartitioner<K, V> partitioner;
        private ITopicNameExtractor<K, V> topicExtractor;

        private SinkNodeFactory(string name,
                                string[] predecessors,
                                ITopicNameExtractor<K, V> topicExtractor,
                                ISerializer<K> keySerializer,
                                ISerializer<V> valSerializer,
                                IStreamPartitioner<K, V> partitioner)
            : base(name, predecessors.clone())
        {
            this.topicExtractor = topicExtractor;
            this.keySerializer = keySerializer;
            this.valSerializer = valSerializer;
            this.partitioner = partitioner;
        }


        public ProcessorNode build()
        {
            if (topicExtractor is StaticTopicNameExtractor)
            {
                string topic = ((StaticTopicNameExtractor)topicExtractor).topicName;
                if (internalTopicNames.contains(topic))
                {
                    // prefix the internal topic name with the application id
                    return new SinkNode<>(name, new StaticTopicNameExtractor<>(decorateTopic(topic)), keySerializer, valSerializer, partitioner);
                }
                else
                {

                    return new SinkNode<>(name, topicExtractor, keySerializer, valSerializer, partitioner);
                }
            }
            else
            {

                return new SinkNode<>(name, topicExtractor, keySerializer, valSerializer, partitioner);
            }
        }


        Sink describe()
        {
            return new Sink(name, topicExtractor);
        }
    }
}
