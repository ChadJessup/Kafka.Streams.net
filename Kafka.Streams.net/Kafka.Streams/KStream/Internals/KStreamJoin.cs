using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.State;
using Kafka.Streams.State.Window;
using NodaTime;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamJoin
    {
        private readonly bool leftOuter;
        private readonly bool rightOuter;
        private readonly IClock clock;
        private readonly InternalStreamsBuilder builder;

        public KStreamJoin(
            IClock clock,
            InternalStreamsBuilder builder,
            bool leftOuter,
            bool rightOuter)
        {
            this.clock = clock;
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
            var joinedInternal = new JoinedInternal<K1, V1, V2>(joined);
            var renamed = new NamedInternal(joinedInternal.name);

            var thisWindowStreamName = renamed.SuffixWithOrElseGet(
                   "-this-windowed", builder, KStream.WINDOWED_NAME);

            var otherWindowStreamName = renamed.SuffixWithOrElseGet(
                   "-other-windowed", builder, KStream.WINDOWED_NAME);

            var joinThisName = rightOuter ?
                   renamed.SuffixWithOrElseGet("-outer-this-join", builder, KStream.OUTERTHIS_NAME)
                   : renamed.SuffixWithOrElseGet("-this-join", builder, KStream.JOINTHIS_NAME);

            var joinOtherName = leftOuter ?
                   renamed.SuffixWithOrElseGet("-outer-other-join", builder, KStream.OUTEROTHER_NAME)
                   : renamed.SuffixWithOrElseGet("-other-join", builder, KStream.JOINOTHER_NAME);

            var joinMergeName = renamed.SuffixWithOrElseGet(
                   "-merge", builder, KStream.MERGE_NAME);

            StreamsGraphNode thisStreamsGraphNode = ((AbstractStream<K1, V1>)lhs).streamsGraphNode;
            StreamsGraphNode otherStreamsGraphNode = ((AbstractStream<K1, V2>)other).streamsGraphNode;

            IStoreBuilder<IWindowStore<K1, V1>> thisWindowStore =
               KStream.joinWindowStoreBuilder(this.clock, joinThisName, windows, joined.keySerde, joined.valueSerde);

            IStoreBuilder<IWindowStore<K1, V2>> otherWindowStore =
               KStream.joinWindowStoreBuilder(this.clock, joinOtherName, windows, joined.keySerde, joined.otherValueSerde);

            var thisWindowedStream = new KStreamJoinWindow<K1, V1>(thisWindowStore.name);

            var thisWindowStreamProcessorParams = new ProcessorParameters<K1, V1>(thisWindowedStream, thisWindowStreamName);
            var thisWindowedStreamsNode = new ProcessorGraphNode<K1, V1>(thisWindowStreamName, thisWindowStreamProcessorParams);
            builder.AddGraphNode<K1, V1>(thisStreamsGraphNode, thisWindowedStreamsNode);

            var otherWindowedStream = new KStreamJoinWindow<K1, V2>(otherWindowStore.name);

            var otherWindowStreamProcessorParams = new ProcessorParameters<K1, V2>(otherWindowedStream, otherWindowStreamName);
            var otherWindowedStreamsNode = new ProcessorGraphNode<K1, V2>(otherWindowStreamName, otherWindowStreamProcessorParams);

            builder.AddGraphNode<K1, V1>(otherStreamsGraphNode, otherWindowedStreamsNode);

            var joinThis = new KStreamKStreamJoin<K1, R, V1, V2>(
               otherWindowStore.name,
               windows.beforeMs,
               windows.afterMs,
               joiner,
               leftOuter);

            var joinOther = new KStreamKStreamJoin<K1, R, V2, V1>(
               thisWindowStore.name,
               windows.afterMs,
               windows.beforeMs,
               AbstractStream<K1, V1>.reverseJoiner(joiner),
               rightOuter);

            var joinMerge = new KStreamPassThrough<K1, R>();

            StreamStreamJoinNodeBuilder<K1, V1, V2, R> joinBuilder = StreamStreamJoinNode.streamStreamJoinNodeBuilder<K1, V1, V2, R>();

            var joinThisProcessorParams = new ProcessorParameters<K1, V1>(joinThis, joinThisName);
            var joinOtherProcessorParams = new ProcessorParameters<K1, V2>(joinOther, joinOtherName);
            var joinMergeProcessorParams = new ProcessorParameters<K1, R>(joinMerge, joinMergeName);

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

            var allSourceNodes = new HashSet<string>(((KStream<K1, V1>)lhs).sourceNodes);

            allSourceNodes.UnionWith(((KStream<K1, V2>)other)
                .sourceNodes);

            // do not have serde for joined result;
            // also for key serde we do not inherit from either since we cannot tell if these two serdes are different
            return new KStream<K1, R>(
                this.clock,
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
