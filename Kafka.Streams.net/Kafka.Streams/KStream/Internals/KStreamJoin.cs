using Kafka.Common;
using Kafka.Streams.Interfaces;
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
            ValueJoiner<V1, V2, R> joiner,
            JoinWindows windows,
            Joined<K1, V1, V2> joined)
        {
            var joinedInternal = new JoinedInternal<K1, V1, V2>(joined);
            var renamed = new NamedInternal(joinedInternal.Name);

            var thisWindowStreamName = renamed.SuffixWithOrElseGet(
                   "-this-windowed", this.builder, KStream.WINDOWED_NAME);

            var otherWindowStreamName = renamed.SuffixWithOrElseGet(
                   "-other-windowed", this.builder, KStream.WINDOWED_NAME);

            var joinThisName = this.rightOuter ?
                   renamed.SuffixWithOrElseGet("-outer-this-join", this.builder, KStream.OUTERTHIS_NAME)
                   : renamed.SuffixWithOrElseGet("-this-join", this.builder, KStream.JOINTHIS_NAME);

            var joinOtherName = this.leftOuter ?
                   renamed.SuffixWithOrElseGet("-outer-other-join", this.builder, KStream.OUTEROTHER_NAME)
                   : renamed.SuffixWithOrElseGet("-other-join", this.builder, KStream.JOINOTHER_NAME);

            var joinMergeName = renamed.SuffixWithOrElseGet(
                   "-merge", this.builder, KStream.MERGE_NAME);

            StreamsGraphNode thisStreamsGraphNode = ((AbstractStream<K1, V1>)lhs).StreamsGraphNode;
            StreamsGraphNode otherStreamsGraphNode = ((AbstractStream<K1, V2>)other).StreamsGraphNode;

            IStoreBuilder<IWindowStore<K1, V1>> thisWindowStore =
               KStream.JoinWindowStoreBuilder(this.context, joinThisName, windows, joined.KeySerde, joined.ValueSerde);

            IStoreBuilder<IWindowStore<K1, V2>> otherWindowStore =
               KStream.JoinWindowStoreBuilder(this.context, joinOtherName, windows, joined.KeySerde, joined.OtherValueSerde);

            var thisWindowedStream = new KStreamJoinWindow<K1, V1>(thisWindowStore.Name);

            var thisWindowStreamProcessorParams = new ProcessorParameters<K1, V1>(thisWindowedStream, thisWindowStreamName);
            var thisWindowedStreamsNode = new ProcessorGraphNode<K1, V1>(thisWindowStreamName, thisWindowStreamProcessorParams);
            this.builder.AddGraphNode<K1, V1>(thisStreamsGraphNode, thisWindowedStreamsNode);

            var otherWindowedStream = new KStreamJoinWindow<K1, V2>(otherWindowStore.Name);

            var otherWindowStreamProcessorParams = new ProcessorParameters<K1, V2>(otherWindowedStream, otherWindowStreamName);
            var otherWindowedStreamsNode = new ProcessorGraphNode<K1, V2>(otherWindowStreamName, otherWindowStreamProcessorParams);

            this.builder.AddGraphNode<K1, V1>(otherStreamsGraphNode, otherWindowedStreamsNode);

            var joinThis = new KStreamKStreamJoin<K1, R, V1, V2>(
                this.context,
                otherWindowStore.Name,
                windows.before,
                windows.after,
                joiner,
                this.leftOuter);

            var joinOther = new KStreamKStreamJoin<K1, R, V2, V1>(
                this.context,
                thisWindowStore.Name,
                windows.after,
                windows.before,
                AbstractStream<K1, V1>.ReverseJoiner(joiner),
                this.rightOuter);

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

            this.builder.AddGraphNode<K1, V1>(new HashSet<StreamsGraphNode> { thisStreamsGraphNode, otherStreamsGraphNode }, joinGraphNode);

            var allSourceNodes = new HashSet<string>(((KStream<K1, V1>)lhs).SourceNodes);

            allSourceNodes.UnionWith(((KStream<K1, V2>)other)
                .SourceNodes);

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
                this.builder);
        }
    }
}
