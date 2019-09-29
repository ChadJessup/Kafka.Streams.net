using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.Processor.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamJoin
    {
        private readonly bool leftOuter;
        private readonly bool rightOuter;
        private readonly InternalStreamsBuilder builder;

        public KStreamJoin(
            InternalStreamsBuilder builder,
            bool leftOuter,
            bool rightOuter)
        {
            this.builder = builder;
            this.leftOuter = leftOuter;
            this.rightOuter = rightOuter;
        }

        public IKStream<K1, R> join<K1, V1, V2, R>(
            IKStream<K1, V1> lhs,
            IKStream<K1, V2> other,
            IValueJoiner<V1, V2, R> joiner,
            JoinWindows windows,
            Joined<K1, V1, V2> joined)
        {
            JoinedInternal<K1, V1, V2> joinedInternal = new JoinedInternal<K1, V1, V2>(joined);
            NamedInternal renamed = new NamedInternal(joinedInternal.name);

            string thisWindowStreamName = renamed.suffixWithOrElseGet(
                   "-this-windowed", builder, KStream.WINDOWED_NAME);

            string otherWindowStreamName = renamed.suffixWithOrElseGet(
                   "-other-windowed", builder, KStream.WINDOWED_NAME);

            string joinThisName = rightOuter ?
                   renamed.suffixWithOrElseGet("-outer-this-join", builder, KStream.OUTERTHIS_NAME)
                   : renamed.suffixWithOrElseGet("-this-join", builder, KStream.JOINTHIS_NAME);

            string joinOtherName = leftOuter ?
                   renamed.suffixWithOrElseGet("-outer-other-join", builder, KStream.OUTEROTHER_NAME)
                   : renamed.suffixWithOrElseGet("-other-join", builder, KStream.JOINOTHER_NAME);

            string joinMergeName = renamed.suffixWithOrElseGet(
                   "-merge", builder, KStream.MERGE_NAME);

            StreamsGraphNode thisStreamsGraphNode = ((AbstractStream<K1, V1>)lhs).streamsGraphNode;
            StreamsGraphNode otherStreamsGraphNode = ((AbstractStream<K1, V2>)other).streamsGraphNode;

            IStoreBuilder<IWindowStore<K1, V1>> thisWindowStore =
               KStream.joinWindowStoreBuilder(joinThisName, windows, joined.keySerde, joined.valueSerde);

            IStoreBuilder<IWindowStore<K1, V2>> otherWindowStore =
               KStream.joinWindowStoreBuilder(joinOtherName, windows, joined.keySerde, joined.otherValueSerde);

            KStreamJoinWindow<K1, V1> thisWindowedStream = new KStreamJoinWindow<K1, V1>(thisWindowStore.name);

            ProcessorParameters<K1, V1> thisWindowStreamProcessorParams = new ProcessorParameters<K1, V1>(thisWindowedStream, thisWindowStreamName);
            ProcessorGraphNode<K1, V1> thisWindowedStreamsNode = new ProcessorGraphNode<K1, V1>(thisWindowStreamName, thisWindowStreamProcessorParams);
            builder.AddGraphNode<K1, V1>(thisStreamsGraphNode, thisWindowedStreamsNode);

            KStreamJoinWindow<K1, V2> otherWindowedStream = new KStreamJoinWindow<K1, V2>(otherWindowStore.name);

            ProcessorParameters<K1, V2> otherWindowStreamProcessorParams = new ProcessorParameters<K1, V2>(otherWindowedStream, otherWindowStreamName);
            ProcessorGraphNode<K1, V2> otherWindowedStreamsNode = new ProcessorGraphNode<K1, V2>(otherWindowStreamName, otherWindowStreamProcessorParams);

            builder.AddGraphNode<K1, V1>(otherStreamsGraphNode, otherWindowedStreamsNode);

            KStreamKStreamJoin<K1, R, V1, V2> joinThis = new KStreamKStreamJoin<K1, R, V1, V2>(
               otherWindowStore.name,
               windows.beforeMs,
               windows.afterMs,
               joiner,
               leftOuter);

            KStreamKStreamJoin<K1, R, V2, V1> joinOther = new KStreamKStreamJoin<K1, R, V2, V1>(
               thisWindowStore.name,
               windows.afterMs,
               windows.beforeMs,
               AbstractStream<K1, V1>.reverseJoiner(joiner),
               rightOuter);

            KStreamPassThrough<K1, R> joinMerge = new KStreamPassThrough<K1, R>();

            StreamStreamJoinNodeBuilder<K1, V1, V2, R> joinBuilder = StreamStreamJoinNode.streamStreamJoinNodeBuilder<K1, V1, V2, R>();

            ProcessorParameters<K1, V1> joinThisProcessorParams = new ProcessorParameters<K1, V1>(joinThis, joinThisName);
            ProcessorParameters<K1, V2> joinOtherProcessorParams = new ProcessorParameters<K1, V2>(joinOther, joinOtherName);
            ProcessorParameters<K1, R> joinMergeProcessorParams = new ProcessorParameters<K1, R>(joinMerge, joinMergeName);

            joinBuilder.withJoinMergeProcessorParameters(joinMergeProcessorParams)
                       .withJoinThisProcessorParameters(joinThisProcessorParams)
                       .withJoinOtherProcessorParameters(joinOtherProcessorParams)
                       .withThisWindowStoreBuilder(thisWindowStore)
                       .withOtherWindowStoreBuilder(otherWindowStore)
                       .withThisWindowedStreamProcessorParameters(thisWindowStreamProcessorParams)
                       .withOtherWindowedStreamProcessorParameters(otherWindowStreamProcessorParams)
                       .withValueJoiner(joiner)
                       .withNodeName(joinMergeName);

            StreamsGraphNode joinGraphNode = joinBuilder.build();

            builder.AddGraphNode<K1, V1>(new HashSet<StreamsGraphNode> { thisStreamsGraphNode, otherStreamsGraphNode }, joinGraphNode);

            HashSet<string> allSourceNodes = new HashSet<string>(((KStream<K1, V1>)lhs).sourceNodes);

            allSourceNodes.UnionWith(((KStream<K1, V2>)other)
                .sourceNodes);

            // do not have serde for joined result;
            // also for key serde we do not inherit from either since we cannot tell if these two serdes are different
            return new KStream<K1, R>(
                joinMergeName,
                joined.keySerde,
                null,
                allSourceNodes,
                false,
                joinGraphNode,
                builder);
        }
    }
}
