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










    public class StreamSourceNode<K, V> : StreamsGraphNode
    {


        private Collection<string> topicNames;
        private Pattern topicPattern;
        private ConsumedInternal<K, V> consumedInternal;


        public StreamSourceNode(string nodeName,
                                 Collection<string> topicNames,
                                 ConsumedInternal<K, V> consumedInternal)
            : base(nodeName)
        {

            this.topicNames = topicNames;
            this.consumedInternal = consumedInternal;
        }

        public StreamSourceNode(string nodeName,
                                 Pattern topicPattern,
                                 ConsumedInternal<K, V> consumedInternal)
            : base(nodeName)
        {


            this.topicPattern = topicPattern;
            this.consumedInternal = consumedInternal;
        }

        public Collection<string> getTopicNames()
        {
            return new List<>(topicNames);
        }

        public Pattern topicPattern()
        {
            return topicPattern;
        }

        public ConsumedInternal<K, V> consumedInternal()
        {
            return consumedInternal;
        }

        public ISerde<K> keySerde()
        {
            return consumedInternal.keySerde();
        }

        public ISerde<V> valueSerde()
        {
            return consumedInternal.valueSerde();
        }


        public string ToString()
        {
            return "StreamSourceNode{" +
                   "topicNames=" + topicNames +
                   ", topicPattern=" + topicPattern +
                   ", consumedInternal=" + consumedInternal +
                   "} " + base.ToString();
        }


        public void writeToTopology(InternalTopologyBuilder topologyBuilder)
        {

            if (topicPattern != null)
            {
                topologyBuilder.AddSource(consumedInternal.offsetResetPolicy(),
                                          nodeName(),
                                          consumedInternal.timestampExtractor(),
                                          consumedInternal.keyDeserializer(),
                                          consumedInternal.valueDeserializer(),
                                          topicPattern);
            }
            else
            {

                topologyBuilder.AddSource(consumedInternal.offsetResetPolicy(),
                                          nodeName(),
                                          consumedInternal.timestampExtractor(),
                                          consumedInternal.keyDeserializer(),
                                          consumedInternal.valueDeserializer(),
                                          topicNames.toArray(new string[topicNames.size()]));

            }
        }

    }
