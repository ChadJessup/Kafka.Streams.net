﻿

//using Kafka.Streams.Interfaces;
//using Kafka.Streams.State;

//namespace Kafka.Streams.KStream.Internals.Graph
//{
//   public class KTableKTableJoinNodeBuilder<K, V1, V2, VR>
//        {
//            private string nodeName;
//            private ProcessorParameters<K, Change<V1>> joinThisProcessorParameters;
//            private ProcessorParameters<K, Change<V2>> joinOtherProcessorParameters;
//            private string thisJoinSide;
//            private string otherJoinSide;
//            private ISerde<K> keySerde;
//            private ISerde<VR> valueSerde;
//            private string[] joinThisStoreNames;
//            private string[] joinOtherStoreNames;
//            private string queryableStoreName;
//            private IStoreBuilder<ITimestampedKeyValueStore<K, VR>> storeBuilder;

//            public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withNodeName(string nodeName)
//            {
//                this.nodeName = nodeName;
//                return this;
//            }

//            public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinThisProcessorParameters(ProcessorParameters<K, Change<V1>> joinThisProcessorParameters)
//            {
//                this.joinThisProcessorParameters = joinThisProcessorParameters;
//                return this;
//            }

//            public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinOtherProcessorParameters(ProcessorParameters<K, Change<V2>> joinOtherProcessorParameters)
//            {
//                this.joinOtherProcessorParameters = joinOtherProcessorParameters;
//                return this;
//            }

//            public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withThisJoinSideNodeName(string thisJoinSide)
//            {
//                this.thisJoinSide = thisJoinSide;
//                return this;
//            }

//            public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withOtherJoinSideNodeName(string otherJoinSide)
//            {
//                this.otherJoinSide = otherJoinSide;
//                return this;
//            }

//            public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withKeySerde(ISerde<K> keySerde)
//            {
//                this.keySerde = keySerde;
//                return this;
//            }

//            public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withValueSerde(ISerde<VR> valueSerde)
//            {
//                this.valueSerde = valueSerde;
//                return this;
//            }

//            public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinThisStoreNames(string[] joinThisStoreNames)
//            {
//                this.joinThisStoreNames = joinThisStoreNames;
//                return this;
//            }

//            public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withJoinOtherStoreNames(string[] joinOtherStoreNames)
//            {
//                this.joinOtherStoreNames = joinOtherStoreNames;
//                return this;
//            }

//            public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withQueryableStoreName(string queryableStoreName)
//            {
//                this.queryableStoreName = queryableStoreName;
//                return this;
//            }

//            public KTableKTableJoinNodeBuilder<K, V1, V2, VR> withStoreBuilder(IStoreBuilder<ITimestampedKeyValueStore<K, VR>> storeBuilder)
//            {
//                this.storeBuilder = storeBuilder;
//                return this;
//            }


//            public KTableKTableJoinNode<K, V1, V2, VR> build()
//            {
//                return new KTableKTableJoinNode<K, V1, V2, VR>(
//                    nodeName,
//                    joinThisProcessorParameters,
//                    joinOtherProcessorParameters,
//                    new ProcessorParameters<K, V>(
//                        KTableKTableJoinMerger.of(
//                            (IKTableProcessorSupplier<K, V1, VR>)(joinThisProcessorParameters.IProcessorSupplier),
//                            (IKTableProcessorSupplier<K, V2, VR>)(joinOtherProcessorParameters.IProcessorSupplier),
//                            queryableStoreName),
//                        nodeName),
//                    thisJoinSide,
//                    otherJoinSide,
//                    keySerde,
//                    valueSerde,
//                    joinThisStoreNames,
//                    joinOtherStoreNames,
//                    storeBuilder);
//            }
//        }
//    }
