/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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

package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableKTableJoinMerger;
import org.apache.kafka.streams.kstream.internals.KTableProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;

import java.util.Arrays;

/**
 * Too much specific information to generalize so the KTable-KTable join requires a specific node.
 */
public class KTableKTableJoinNode<K, V1, V2, VR> : BaseJoinProcessorNode<K, Change<V1>, Change<V2>, Change<VR>> {

    private  ISerde<K> keySerde;
    private  ISerde<VR> valueSerde;
    private  string[] joinThisStoreNames;
    private  string[] joinOtherStoreNames;
    private  StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder;

    KTableKTableJoinNode( string nodeName,
                          ProcessorParameters<K, Change<V1>> joinThisProcessorParameters,
                          ProcessorParameters<K, Change<V2>> joinOtherProcessorParameters,
                          ProcessorParameters<K, Change<VR>> joinMergeProcessorParameters,
                          string thisJoinSide,
                          string otherJoinSide,
                          ISerde<K> keySerde,
                          ISerde<VR> valueSerde,
                          string[] joinThisStoreNames,
                          string[] joinOtherStoreNames,
                          StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder) {

        super(nodeName,
            null,
            joinThisProcessorParameters,
            joinOtherProcessorParameters,
            joinMergeProcessorParameters,
            thisJoinSide,
            otherJoinSide);

        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.joinThisStoreNames = joinThisStoreNames;
        this.joinOtherStoreNames = joinOtherStoreNames;
        this.storeBuilder = storeBuilder;
    }

    public ISerde<K> keySerde() {
        return keySerde;
    }

    public ISerde<VR> valueSerde() {
        return valueSerde;
    }

    public string[] joinThisStoreNames() {
        return joinThisStoreNames;
    }

    public string[] joinOtherStoreNames() {
        return joinOtherStoreNames;
    }

    public string queryableStoreName() {
        return ((KTableKTableJoinMerger) mergeProcessorParameters().processorSupplier()).getQueryableName();
    }

    /**
     * The supplier which provides processor with KTable-KTable join merge functionality.
     */
    public KTableKTableJoinMerger<K, VR> joinMerger() {
        return (KTableKTableJoinMerger<K, VR>) mergeProcessorParameters().processorSupplier();
    }

    @Override
    public void writeToTopology( InternalTopologyBuilder topologyBuilder) {
         string thisProcessorName = thisProcessorParameters().processorName();
         string otherProcessorName = otherProcessorParameters().processorName();
         string mergeProcessorName = mergeProcessorParameters().processorName();

        topologyBuilder.addProcessor(
            thisProcessorName,
            thisProcessorParameters().processorSupplier(),
            thisJoinSideNodeName());

        topologyBuilder.addProcessor(
            otherProcessorName,
            otherProcessorParameters().processorSupplier(),
            otherJoinSideNodeName());

        topologyBuilder.addProcessor(
            mergeProcessorName,
            mergeProcessorParameters().processorSupplier(),
            thisProcessorName,
            otherProcessorName);

        topologyBuilder.connectProcessorAndStateStores(thisProcessorName, joinOtherStoreNames);
        topologyBuilder.connectProcessorAndStateStores(otherProcessorName, joinThisStoreNames);

        if (storeBuilder != null) {
            topologyBuilder.addStateStore(storeBuilder, mergeProcessorName);
        }
    }

    @Override
    public string toString() {
        return "KTableKTableJoinNode{" +
            "joinThisStoreNames=" + Arrays.toString(joinThisStoreNames()) +
            ", joinOtherStoreNames=" + Arrays.toString(joinOtherStoreNames()) +
            "} " + super.toString();
    }

    public static <K, V1, V2, VR> KTableKTableJoinNodeBuilder<K, V1, V2, VR> kTableKTableJoinNodeBuilder() {
        return new KTableKTableJoinNodeBuilder<>();
    }

    public static  class KTableKTableJoinNodeBuilder<K, V1, V2, VR> {
        private string nodeName;
        private ProcessorParameters<K, Change<V1>> joinThisProcessorParameters;
        private ProcessorParameters<K, Change<V2>> joinOtherProcessorParameters;
        private string thisJoinSide;
        private string otherJoinSide;
        private ISerde<K> keySerde;
        private ISerde<VR> valueSerde;
        private string[] joinThisStoreNames;
        private string[] joinOtherStoreNames;
        private string queryableStoreName;
        private StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder;

        private KTableKTableJoinNodeBuilder() {
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withNodeName( string nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinThisProcessorParameters( ProcessorParameters<K, Change<V1>> joinThisProcessorParameters) {
            this.joinThisProcessorParameters = joinThisProcessorParameters;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinOtherProcessorParameters( ProcessorParameters<K, Change<V2>> joinOtherProcessorParameters) {
            this.joinOtherProcessorParameters = joinOtherProcessorParameters;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withThisJoinSideNodeName( string thisJoinSide) {
            this.thisJoinSide = thisJoinSide;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withOtherJoinSideNodeName( string otherJoinSide) {
            this.otherJoinSide = otherJoinSide;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withKeySerde( ISerde<K> keySerde) {
            this.keySerde = keySerde;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withValueSerde( ISerde<VR> valueSerde) {
            this.valueSerde = valueSerde;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinThisStoreNames( string[] joinThisStoreNames) {
            this.joinThisStoreNames = joinThisStoreNames;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinOtherStoreNames( string[] joinOtherStoreNames) {
            this.joinOtherStoreNames = joinOtherStoreNames;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withQueryableStoreName( string queryableStoreName) {
            this.queryableStoreName = queryableStoreName;
            return this;
        }

        public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withStoreBuilder( StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder) {
            this.storeBuilder = storeBuilder;
            return this;
        }

        @SuppressWarnings("unchecked")
        public KTableKTableJoinNode<K, V1, V2, VR> build() {
            return new KTableKTableJoinNode<>(nodeName,
                joinThisProcessorParameters,
                joinOtherProcessorParameters,
                new ProcessorParameters<>(
                    KTableKTableJoinMerger.of(
                        (KTableProcessorSupplier<K, V1, VR>) (joinThisProcessorParameters.processorSupplier()),
                        (KTableProcessorSupplier<K, V2, VR>) (joinOtherProcessorParameters.processorSupplier()),
                        queryableStoreName),
                    nodeName),
                thisJoinSide,
                otherJoinSide,
                keySerde,
                valueSerde,
                joinThisStoreNames,
                joinOtherStoreNames,
                storeBuilder);
        }
    }
}
