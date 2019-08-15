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
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals;
using Kafka.Streams.State;
using Kafka.Streams.Topologies;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class StatefulProcessorNode<K, V> : ProcessorGraphNode<K, V>
    {

        private string[] storeNames;
        private IStoreBuilder<IStateStore> storeBuilder;


        /**
         * Create a node representing a stateful processor, where the named store has already been registered.
         */
        public StatefulProcessorNode(string nodeName,
                                      ProcessorParameters<K, V> processorParameters,
                                      string[] storeNames)
            : base(nodeName, processorParameters)
        {

            this.storeNames = storeNames;
            this.storeBuilder = null;
        }


        /**
         * Create a node representing a stateful processor,
         * where the store needs to be built and registered as part of building this node.
         */
        public StatefulProcessorNode(
            string nodeName,
            ProcessorParameters<K, V> processorParameters,
            IStoreBuilder<IStateStore> materializedKTableStoreBuilder)
            : base(nodeName, processorParameters)
        {

            this.storeNames = null;
            this.storeBuilder = materializedKTableStoreBuilder;
        }


        public string ToString()
        {
            return "StatefulProcessorNode{" +
                "storeNames=" + Arrays.ToString(storeNames) +
                ", storeBuilder=" + storeBuilder +
                "} " + base.ToString();
        }


        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            string processorName = processorParameters.processorName;
            var IProcessorSupplier = processorParameters.IProcessorSupplier;

            topologyBuilder.addProcessor(processorName, IProcessorSupplier, ParentNodeNames());

            if (storeNames != null && storeNames.Length > 0)
            {
                topologyBuilder.connectProcessorAndStateStores(processorName, storeNames);
            }

            if (storeBuilder != null)
            {
                //topologyBuilder.addStateStore(storeBuilder, processorName);
            }
        }
    }
}
