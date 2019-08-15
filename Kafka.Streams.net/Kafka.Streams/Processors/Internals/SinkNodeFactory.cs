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
//using Kafka.Streams.Processor.Interfaces;
//using System.Linq;

//namespace Kafka.Streams.Processor.Internals
//{
//    public class SinkNodeFactory<K, V> : NodeFactory<K, V>
//    {
//        private ISerializer<K> keySerializer;
//        private ISerializer<V> valSerializer;
//        private IStreamPartitioner<K, V> partitioner;
//        public ITopicNameExtractor topicExtractor { get; }

//        public SinkNodeFactory(
//            string name,
//            string[] predecessors,
//            ITopicNameExtractor topicExtractor,
//            ISerializer<K> keySerializer,
//            ISerializer<V> valSerializer,
//            IStreamPartitioner<K, V> partitioner)
//            : base(name, predecessors.ToArray())
//        {
//            this.topicExtractor = topicExtractor;
//            this.keySerializer = keySerializer;
//            this.valSerializer = valSerializer;
//            this.partitioner = partitioner;
//        }

//        public override ProcessorNode<K, V> build()
//        {
//            if (topicExtractor is StaticTopicNameExtractor<K, V>)
//            {
//                string topic = ((StaticTopicNameExtractor<K, V>)topicExtractor).topicName;
//                if (internalTopicNames.Contains(topic))
//                {
//                    // prefix the internal topic name with the application id
//                    return new SinkNode<K, V>(
//                        name,
//                        new StaticTopicNameExtractor<K, V>(decorateTopic(topic)),
//                        keySerializer,
//                        valSerializer,
//                        partitioner);
//                }
//                else
//                {

//                    return new SinkNode<>(name, topicExtractor, keySerializer, valSerializer, partitioner);
//                }
//            }
//            else
//            {

//                return new SinkNode<>(name, topicExtractor, keySerializer, valSerializer, partitioner);
//            }
//        }


//        Sink describe()
//        {
//            return new Sink(name, topicExtractor);
//        }
//    }
//}
