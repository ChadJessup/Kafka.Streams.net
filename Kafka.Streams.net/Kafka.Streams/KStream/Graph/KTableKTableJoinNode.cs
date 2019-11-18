
//        public KTableKTableJoinMerger<K, VR> joinMerger()
//        {
//            return (KTableKTableJoinMerger<K, VR>)mergeProcessorParameters().IProcessorSupplier;
//        }

//        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
//        {
//            string thisProcessorName = thisProcessorParameters().processorName;
//            string otherProcessorName = otherProcessorParameters().processorName;
//            string mergeProcessorName = mergeProcessorParameters().processorName;

//            topologyBuilder.addProcessor(
//                thisProcessorName,
//                thisProcessorParameters().IProcessorSupplier,
//                thisJoinSideNodeName);

//            topologyBuilder.addProcessor(
//                otherProcessorName,
//                otherProcessorParameters().IProcessorSupplier,
//                otherJoinSideNodeName);

//            topologyBuilder.addProcessor(
//                mergeProcessorName,
//                mergeProcessorParameters().IProcessorSupplier,
//                thisProcessorName,
//                otherProcessorName);

//            topologyBuilder.connectProcessorAndStateStores(thisProcessorName, joinOtherStoreNames);
//            topologyBuilder.connectProcessorAndStateStores(otherProcessorName, joinThisStoreNames);

//            if (storeBuilder != null)
//            {
//                topologyBuilder.addStateStore<K, V1, ITimestampedKeyValueStore<K, VR>>(storeBuilder, mergeProcessorName);
//            }
//        }


//        public string ToString()
//        {
//            return "KTableKTableJoinNode{" +
//                "joinThisStoreNames=" + Arrays.ToString(joinThisStoreNames) +
//                ", joinOtherStoreNames=" + Arrays.ToString(joinOtherStoreNames) +
//                "} " + base.ToString();
//        }

//        public static KTableKTableJoinNodeBuilder<K, V1, V2, VR> kTableKTableJoinNodeBuilder()
//        {
//            return new KTableKTableJoinNodeBuilder<K, V1, V2, VR>();
//        }
//    }
//}
