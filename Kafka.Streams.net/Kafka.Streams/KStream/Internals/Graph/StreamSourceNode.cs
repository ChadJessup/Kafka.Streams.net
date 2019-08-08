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

using Kafka.Common;
using Kafka.Streams.Processor.Internals;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class StreamSourceNode<K, V> : StreamsGraphNode
    {
        private List<string> topicNames;
        private Pattern topicPattern;
        protected ConsumedInternal<K, V> consumedInternal;

        public StreamSourceNode(string nodeName,
                                 List<string> topicNames,
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

        public List<string> getTopicNames()
        {
            return new List<string>(topicNames);
        }

        public override string ToString()
        {
            return "StreamSourceNode{" +
                   "topicNames=" + topicNames +
                   ", topicPattern=" + topicPattern +
                   ", consumedInternal=" + consumedInternal +
                   "} " + base.ToString();
        }


        public override void writeToTopology(InternalTopologyBuilder topologyBuilder)
        {

            if (topicPattern != null)
            {
                topologyBuilder.addSource(consumedInternal.offsetResetPolicy(),
                                          nodeName,
                                          consumedInternal.timestampExtractor(),
                                          consumedInternal.keyDeserializer(),
                                          consumedInternal.valueDeserializer(),
                                          topicPattern);
            }
            else
            {

                topologyBuilder.addSource(
                    consumedInternal.offsetResetPolicy(),
                    nodeName,
                    consumedInternal.timestampExtractor,
                    consumedInternal.keyDeserializer(),
                    consumedInternal.valueDeserializer(),
                    topicNames.ToArray());

            }
        }
    }
}
