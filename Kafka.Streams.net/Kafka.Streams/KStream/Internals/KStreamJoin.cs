using Kafka.Common;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.State;
using Kafka.Streams.State.Windowed;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamJoin
    {
        private readonly bool leftOuter;
        private readonly bool rightOuter;
        private readonly KafkaStreamsContext context;
        private readonly InternalStreamsBuilder builder;

        public KStreamJoin(
            KafkaStreamsContext context,
            InternalStreamsBuilder builder,
            bool leftOuter,
            bool rightOuter)
        {
            this.context = context;
            this.builder = builder;
            this.leftOuter = leftOuter;
            this.rightOuter = rightOuter;
        }

        public IKStream<K1, R> Join<K1, V1, V2, R>(
            IKStream<K1, V1> lhs,
            IKStream<K1, V2> other,
            IValueJoiner<V1, V2, R> joiner,
            JoinWindows windows,
            Joined<K1, V1, V2> joined)
        {
            var joinedInternal = new JoinedInternal<K1, V1, V2>(joined);
            var renamed = new NamedInternal(joinedInternal.Name);

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
               KStream.JoinWindowStoreBuilder(this.context, joinThisName, windows, joined.KeySerde, joined.ValueSerde);

            IStoreBuilder<IWindowStore<K1, V2>> otherWindowStore =
               KStream.JoinWindowStoreBuilder(this.context, joinOtherName, windows, joined.KeySerde, joined.OtherValueSerde);

            var thisWindowedStream = new KStreamJoinWindow<K1, V1>(thisWindowStore.name);

            var thisWindowStreamProcessorParams = new ProcessorParameters<K1, V1>(thisWindowedStream, thisWindowStreamName);
            var thisWindowedStreamsNode = new ProcessorGraphNode<K1, V1>(thisWindowStreamName, thisWindowStreamProcessorParams);
            builder.AddGraphNode<K1, V1>(thisStreamsGraphNode, thisWindowedStreamsNode);

            var otherWindowedStream = new KStreamJoinWindow<K1, V2>(otherWindowStore.name);

            var otherWindowStreamProcessorParams = new ProcessorParameters<K1, V2>(otherWindowedStream, otherWindowStreamName);
            var otherWindowedStreamsNode = new ProcessorGraphNode<K1, V2>(otherWindowStreamName, otherWindowStreamProcessorParams);

            builder.AddGraphNode<K1, V1>(otherStreamsGraphNode, otherWindowedStreamsNode);

            var joinThis = new KStreamKStreamJoin<K1, R, V1, V2>(
                this.context,
                otherWindowStore.name,
                windows.beforeMs,
                windows.afterMs,
                joiner,
                leftOuter);

            var joinOther = new KStreamKStreamJoin<K1, R, V2, V1>(
                this.context,
                thisWindowStore.name,
                windows.afterMs,
                windows.beforeMs,
                AbstractStream<K1, V1>.ReverseJoiner(joiner),
                rightOuter);

            var joinMerge = new KStreamPassThrough<K1, R>();

            StreamStreamJoinNodeBuilder<K1, V1, V2, R> joinBuilder = StreamStreamJoinNode.StreamStreamJoinNodeBuilder<K1, V1, V2, R>();

            var joinThisProcessorParams = new ProcessorParameters<K1, V1>(joinThis, joinThisName);
            var joinOtherProcessorParams = new ProcessorParameters<K1, V2>(joinOther, joinOtherName);
            var joinMergeProcessorParams = new ProcessorParameters<K1, R>(joinMerge, joinMergeName);

            joinBuilder.WithJoinMergeProcessorParameters(joinMergeProcessorParams)
                       .WithJoinThisProcessorParameters(joinThisProcessorParams)
                       .WithJoinOtherProcessorParameters(joinOtherProcessorParams)
                       .WithThisWindowStoreBuilder(thisWindowStore)
                       .WithOtherWindowStoreBuilder(otherWindowStore)
                       .WithThisWindowedStreamProcessorParameters(thisWindowStreamProcessorParams)
                       .WithOtherWindowedStreamProcessorParameters(otherWindowStreamProcessorParams)
                       .WithValueJoiner(joiner)
                       .WithNodeName(joinMergeName);

            StreamsGraphNode joinGraphNode = joinBuilder.Build();

            builder.AddGraphNode<K1, V1>(new HashSet<StreamsGraphNode> { thisStreamsGraphNode, otherStreamsGraphNode }, joinGraphNode);

            var allSourceNodes = new HashSet<string>(((KStream<K1, V1>)lhs).sourceNodes);

            allSourceNodes.UnionWith(((KStream<K1, V2>)other)
                .sourceNodes);

            // do not have serde for joined result;
            // also for key serde we do not inherit from either since we cannot tell if these two serdes are different
            return new KStream<K1, R>(
                this.context,
                joinMergeName,
                joined.KeySerde,
                null,
                allSourceNodes,
                false,
                joinGraphNode,
                builder);
        }
    }
}
