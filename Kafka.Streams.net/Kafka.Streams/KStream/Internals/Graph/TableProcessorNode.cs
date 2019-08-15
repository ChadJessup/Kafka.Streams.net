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

using Kafka.Streams.Processor.Internals;
using Kafka.Streams.State;
using Kafka.Streams.Topologies;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class TableProcessorNode<K, V> : StreamsGraphNode
    {
        private ProcessorParameters<K, V> processorParameters;
        //private IStoreBuilder<ITimestampedKeyValueStore<K, V>> storeBuilder;
        private string[] storeNames;

        //public TableProcessorNode(
        //    string nodeName,
        //    ProcessorParameters<K, V> processorParameters)//,
        //    IStoreBuilder<ITimestampedKeyValueStore<K, V>> storeBuilder)
        //    : this(nodeName, processorParameters, storeBuilder, null)
        //{
        //}

        public TableProcessorNode(string nodeName,
                                   ProcessorParameters<K, V> processorParameters,
//                                   IStoreBuilder<ITimestampedKeyValueStore<K, V>> storeBuilder,
                                   string[] storeNames)
            : base(nodeName)
        {
            this.processorParameters = processorParameters;
//            this.storeBuilder = storeBuilder;
            this.storeNames = storeNames != null ? storeNames : new string[] { };
        }


        //public string ToString()
        //{
        //    return "TableProcessorNode{" +
        //        ", processorParameters=" + processorParameters +
        //        ", storeBuilder=" + (storeBuilder == null ? "null" : storeBuilder.name) +
        //        ", storeNames=" + Arrays.ToString(storeNames) +
        //        "} " + base.ToString();
        //}

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            string processorName = processorParameters.processorName;
            topologyBuilder.addProcessor(processorName, processorParameters.IProcessorSupplier, ParentNodeNames());

            if (storeNames.Length > 0)
            {
                topologyBuilder.connectProcessorAndStateStores(processorName, storeNames);
            }

            // TODO: we are enforcing this as a keyvalue store, but it should go beyond any type of stores
            //if (storeBuilder != null)
            //{
            //    topologyBuilder.addStateStore<K, V, T>(storeBuilder, processorName);
            //}
        }
    }
}
