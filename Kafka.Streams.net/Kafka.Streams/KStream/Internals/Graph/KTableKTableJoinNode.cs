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

using Kafka.Streams.Interfaces;
using Kafka.Streams.Processor.Internals;
using Kafka.Streams.State;

namespace Kafka.Streams.KStream.Internals.Graph
{
    /**
     * Too much specific information to generalize so the KTable-KTable join requires a specific node.
     */
    public class KTableKTableJoinNode<K, V1, V2, VR> : BaseJoinProcessorNode<K, Change<V1>, Change<V2>, Change<VR>>
    {
        private ISerde<K> keySerde;
        private ISerde<VR> valueSerde;
        private string[] joinThisStoreNames;
        private string[] joinOtherStoreNames;
        private readonly IStoreBuilder<ITimestampedKeyValueStore<K, VR>> storeBuilder;

        public KTableKTableJoinNode(
            string nodeName,
            ProcessorParameters<K, Change<V1>> joinThisProcessorParameters,
            ProcessorParameters<K, Change<V2>> joinOtherProcessorParameters,
            ProcessorParameters<K, Change<VR>> joinMergeProcessorParameters,
            string thisJoinSide,
            string otherJoinSide,
            ISerde<K> keySerde,
            ISerde<VR> valueSerde,
            string[] joinThisStoreNames,
            string[] joinOtherStoreNames,
            IStoreBuilder<ITimestampedKeyValueStore<K, VR>> storeBuilder)
            : base(nodeName,
                null,
                joinThisProcessorParameters,
                joinOtherProcessorParameters,
                joinMergeProcessorParameters,
                thisJoinSide,
                otherJoinSide)
        {
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
            this.joinThisStoreNames = joinThisStoreNames;
            this.joinOtherStoreNames = joinOtherStoreNames;
            this.storeBuilder = storeBuilder;
        }

        public string queryableStoreName()
        {
            return ((KTableKTableJoinMerger<K, V1>)mergeProcessorParameters().processorSupplier).getQueryableName();
        }

        /**
         * The supplier which provides processor with KTable-KTable join merge functionality.
         */
        public KTableKTableJoinMerger<K, VR> joinMerger()
        {
            return (KTableKTableJoinMerger<K, VR>)mergeProcessorParameters().processorSupplier;
        }

        public override void writeToTopology(InternalTopologyBuilder topologyBuilder)
        {
            string thisProcessorName = thisProcessorParameters().processorName;
            string otherProcessorName = otherProcessorParameters().processorName;
            string mergeProcessorName = mergeProcessorParameters().processorName;

            topologyBuilder.addProcessor(
                thisProcessorName,
                thisProcessorParameters().processorSupplier,
                thisJoinSideNodeName);

            topologyBuilder.addProcessor(
                otherProcessorName,
                otherProcessorParameters().processorSupplier,
                otherJoinSideNodeName);

            topologyBuilder.addProcessor(
                mergeProcessorName,
                mergeProcessorParameters().processorSupplier,
                thisProcessorName,
                otherProcessorName);

            topologyBuilder.connectProcessorAndStateStores(thisProcessorName, joinOtherStoreNames);
            topologyBuilder.connectProcessorAndStateStores(otherProcessorName, joinThisStoreNames);

            if (storeBuilder != null)
            {
                topologyBuilder.addStateStore<K, V1, ITimestampedKeyValueStore<K, VR>>(storeBuilder, mergeProcessorName);
            }
        }


        public string ToString()
        {
            return "KTableKTableJoinNode{" +
                "joinThisStoreNames=" + Arrays.ToString(joinThisStoreNames) +
                ", joinOtherStoreNames=" + Arrays.ToString(joinOtherStoreNames) +
                "} " + base.ToString();
        }

        public static KTableKTableJoinNodeBuilder<K, V1, V2, VR> kTableKTableJoinNodeBuilder()
        {
            return new KTableKTableJoinNodeBuilder<K, V1, V2, VR>();
        }
    }
}
