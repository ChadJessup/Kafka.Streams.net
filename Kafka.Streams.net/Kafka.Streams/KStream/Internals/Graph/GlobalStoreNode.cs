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
using Kafka.Streams.Topologies;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class GlobalStoreNode<K, V, T> : StateStoreNode<T>
        where T : IStateStore
    {
        private readonly string sourceName;
        private readonly string topic;
        private readonly ConsumedInternal<K, V> consumed;
        private readonly string processorName;
        private readonly IProcessorSupplier<K, V> stateUpdateSupplier;


        public GlobalStoreNode(
            T storeBuilder,
            string sourceName,
            string topic,
            ConsumedInternal<K, V> consumed,
            string processorName,
            IProcessorSupplier<K, V> stateUpdateSupplier)
            : base(storeBuilder)
        {

            this.sourceName = sourceName;
            this.topic = topic;
            this.consumed = consumed;
            this.processorName = processorName;
            this.stateUpdateSupplier = stateUpdateSupplier;
        }

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            //storeBuilder.withLoggingDisabled();
            //topologyBuilder.addGlobalStore(storeBuilder,
            //                               sourceName,
            //                               consumed.timestampExtractor,
            //                               consumed.keyDeserializer(),
            //                               consumed.valueDeserializer(),
            //                               topic,
            //                               processorName,
            //                               stateUpdateSupplier);
        }

        public override string ToString()
        {
            return "GlobalStoreNode{" +
                   "sourceName='" + sourceName + '\'' +
                   ", topic='" + topic + '\'' +
                   ", processorName='" + processorName + '\'' +
                   "} ";
        }
    }
}
