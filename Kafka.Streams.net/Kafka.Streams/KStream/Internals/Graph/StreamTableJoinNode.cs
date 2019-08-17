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

using Kafka.Streams.Processor;
using Kafka.Streams.Processor.Internals;
using Kafka.Streams.Topologies;

namespace Kafka.Streams.KStream.Internals.Graph
{
    /**
     * Represents a join between a KStream and a KTable or GlobalKTable
     */
    public class StreamTableJoinNode<K, V> : StreamsGraphNode
    {
        private string[] storeNames;
        private ProcessorParameters<K, V> processorParameters;
        private string otherJoinSideNodeName;

        public StreamTableJoinNode(
            string nodeName,
            ProcessorParameters<K, V> processorParameters,
            string[] storeNames,
            string otherJoinSideNodeName)
            : base(nodeName)
        {

            // in the case of Stream-Table join the state stores associated with the KTable
            this.storeNames = storeNames;
            this.processorParameters = processorParameters;
            this.otherJoinSideNodeName = otherJoinSideNodeName;
        }

        public override string ToString()
        {
            return "StreamTableJoinNode{" +
                   "storeNames=" + Arrays.ToString(storeNames) +
                   ", processorParameters=" + processorParameters +
                   ", otherJoinSideNodeName='" + otherJoinSideNodeName + '\'' +
                   "} " + base.ToString();
        }


        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            string processorName = processorParameters.processorName;
            IProcessorSupplier<K, V> IProcessorSupplier = processorParameters.ProcessorSupplier;

            // Stream - Table join (Global or KTable)
            topologyBuilder.addProcessor(processorName, IProcessorSupplier, ParentNodeNames());

            // Steam - KTable join only
            if (otherJoinSideNodeName != null)
            {
                topologyBuilder.connectProcessorAndStateStores(processorName, storeNames);
            }

        }
    }
}
