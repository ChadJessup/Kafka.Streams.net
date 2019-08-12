//using Kafka.Streams.KStream.Interfaces;

//namespace Kafka.Streams.KStream.Internals
//{

//    public class KStreamImplJoin
//    {


//        private bool leftOuter;
//        private bool rightOuter;


//        public KStreamImplJoin(bool leftOuter,
//                         bool rightOuter)
//        {
//            this.leftOuter = leftOuter;
//            this.rightOuter = rightOuter;
//        }

//        public IKStream<K1, R> join<K1, V1, V2, R>(
//            IKStream<K1, V1> lhs,
//            IKStream<K1, V2> other,
//            IValueJoiner<V1, V2, R> joiner,
//            JoinWindows windows,
//            Joined<K1, V1, V2> joined)
//        {
//            // JoinedInternal<K1, V1, V2> joinedInternal = new JoinedInternal<K1, V1, V2>(joined);
//            // NamedInternal renamed = new NamedInternal(joinedInternal.name);

//            // string thisWindowStreamName = renamed.suffixWithOrElseGet(
//            //        "-this-windowed", builder, WINDOWED_NAME);
//            // string otherWindowStreamName = renamed.suffixWithOrElseGet(
//            //        "-other-windowed", builder, WINDOWED_NAME);

//            // string joinThisName = rightOuter ?
//            //        renamed.suffixWithOrElseGet("-outer-this-join", builder, OUTERTHIS_NAME)
//            //        : renamed.suffixWithOrElseGet("-this-join", builder, JOINTHIS_NAME);
//            // string joinOtherName = leftOuter ?
//            //        renamed.suffixWithOrElseGet("-outer-other-join", builder, OUTEROTHER_NAME)
//            //        : renamed.suffixWithOrElseGet("-other-join", builder, JOINOTHER_NAME);
//            // string joinMergeName = renamed.suffixWithOrElseGet(
//            //        "-merge", builder, MERGE_NAME);
//            // StreamsGraphNode thisStreamsGraphNode = ((AbstractStream)lhs).streamsGraphNode;
//            // StreamsGraphNode otherStreamsGraphNode = ((AbstractStream)other).streamsGraphNode;


//            // IStoreBuilder<IWindowStore<K1, V1>> thisWindowStore =
//            //    joinWindowStoreBuilder(joinThisName, windows, joined.keySerde, joined.valueSerde);
//            // IStoreBuilder<IWindowStore<K1, V2>> otherWindowStore =
//            //    joinWindowStoreBuilder(joinOtherName, windows, joined.keySerde, joined.otherValueSerde());

//            // KStreamJoinWindow<K1, V1> thisWindowedStream = new KStreamJoinWindow<K1, V1>(thisWindowStore.name);

//            // ProcessorParameters<K1, V1> thisWindowStreamProcessorParams = new ProcessorParameters<>(thisWindowedStream, thisWindowStreamName);
//            // ProcessorGraphNode<K1, V1> thisWindowedStreamsNode = new ProcessorGraphNode<>(thisWindowStreamName, thisWindowStreamProcessorParams);
//            // builder.addGraphNode(thisStreamsGraphNode, thisWindowedStreamsNode);

//            // KStreamJoinWindow<K1, V2> otherWindowedStream = new KStreamJoinWindow<>(otherWindowStore.name);

//            // ProcessorParameters<K1, V2> otherWindowStreamProcessorParams = new ProcessorParameters<>(otherWindowedStream, otherWindowStreamName);
//            // ProcessorGraphNode<K1, V2> otherWindowedStreamsNode = new ProcessorGraphNode<>(otherWindowStreamName, otherWindowStreamProcessorParams);
//            // builder.addGraphNode(otherStreamsGraphNode, otherWindowedStreamsNode);

//            // KStreamKStreamJoin<K1, R, V1, V2> joinThis = new KStreamKStreamJoin<>(
//            //    otherWindowStore.name,
//            //    windows.beforeMs,
//            //    windows.afterMs,
//            //    joiner,
//            //    leftOuter
//            //);

//            // KStreamKStreamJoin<K1, R, V2, V1> joinOther = new KStreamKStreamJoin<>(
//            //    thisWindowStore.name,
//            //    windows.afterMs,
//            //    windows.beforeMs,
//            //    reverseJoiner(joiner),
//            //    rightOuter
//            //);

//            // KStreamPassThrough<K1, R> joinMerge = new KStreamPassThrough<K1, R>();

//            // //                StreamStreamJoinNode.StreamStreamJoinNodeBuilder<K1, V1, V2, R> joinBuilder = StreamStreamJoinNode.streamStreamJoinNodeBuilder();

//            // ProcessorParameters<K1, V1> joinThisProcessorParams = new ProcessorParameters<>(joinThis, joinThisName);
//            // ProcessorParameters<K1, V2> joinOtherProcessorParams = new ProcessorParameters<>(joinOther, joinOtherName);
//            // ProcessorParameters<K1, R> joinMergeProcessorParams = new ProcessorParameters<>(joinMerge, joinMergeName);

//            // joinBuilder.withJoinMergeProcessorParameters(joinMergeProcessorParams)
//            //            .withJoinThisProcessorParameters(joinThisProcessorParams)
//            //            .withJoinOtherProcessorParameters(joinOtherProcessorParams)
//            //            .withThisWindowStoreBuilder(thisWindowStore)
//            //            .withOtherWindowStoreBuilder(otherWindowStore)
//            //            .withThisWindowedStreamProcessorParameters(thisWindowStreamProcessorParams)
//            //            .withOtherWindowedStreamProcessorParameters(otherWindowStreamProcessorParams)
//            //            .withValueJoiner(joiner)
//            //            .withNodeName(joinMergeName);

//            //StreamsGraphNode joinGraphNode = joinBuilder.build();

//            //builder.addGraphNode(Arrays.asList(thisStreamsGraphNode, otherStreamsGraphNode), joinGraphNode);

//            //HashSet<string> allSourceNodes = new HashSet<>(((KStreamImpl<K1, V1>)lhs).sourceNodes);
//            //allSourceNodes.addAll(((KStreamImpl<K1, V2>)other).sourceNodes);

//            // do not have serde for joined result;
//            // also for key serde we do not inherit from either since we cannot tell if these two serdes are different
//            return null; // new KStreamImpl<>(joinMergeName, joined.keySerde, null, allSourceNodes, false, joinGraphNode, builder);
//        }
//    }
//}
