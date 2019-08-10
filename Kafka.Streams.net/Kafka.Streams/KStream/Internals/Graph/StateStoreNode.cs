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
using Kafka.Streams.IProcessor.Interfaces;
using Kafka.Streams.IProcessor.Internals;
using Kafka.Streams.State;
using System;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class StateStoreNode<T> : StreamsGraphNode
        where T : IStateStore
    {
        protected IStoreBuilder<T> storeBuilder;

        public StateStoreNode(IStoreBuilder<T> storeBuilder)
            : base(storeBuilder.name)
        {
            this.storeBuilder = storeBuilder;
        }

        public override void writeToTopology(InternalTopologyBuilder topologyBuilder)
        {
            topologyBuilder.addStateStore<T>(storeBuilder, false, Array.Empty<string>());
        }

        public override string ToString()
        {
            return "StateStoreNode{" +
                   " name='" + storeBuilder.name + '\'' +
                   ", logConfig=" + storeBuilder.logConfig +
                   ", loggingEnabled='" + storeBuilder.loggingEnabled + '\'' +
                   "} ";
        }
    }
}
