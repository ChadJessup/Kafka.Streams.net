using Kafka.Streams.State;
using Kafka.Streams.State.Window;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class StreamStreamJoinNodeBuilder<K, V1, V2, VR>
    {
        private string nodeName;
        private IValueJoiner<V1, V2, VR> valueJoiner;
        private ProcessorParameters<K, V1> joinThisProcessorParameters;
        private ProcessorParameters<K, V2> joinOtherProcessorParameters;
        private ProcessorParameters<K, VR> joinMergeProcessorParameters;
        private ProcessorParameters<K, V1> thisWindowedStreamProcessorParameters;
        private ProcessorParameters<K, V2> otherWindowedStreamProcessorParameters;
        private IStoreBuilder<IWindowStore<K, V1>> thisWindowStoreBuilder;
        private IStoreBuilder<IWindowStore<K, V2>> otherWindowStoreBuilder;
        private Joined<K, V1, V2> joined;

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> WithValueJoiner(IValueJoiner<V1, V2, VR> valueJoiner)
        {
            this.valueJoiner = valueJoiner;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> WithJoinThisProcessorParameters(ProcessorParameters<K, V1> joinThisProcessorParameters)
        {
            this.joinThisProcessorParameters = joinThisProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> WithNodeName(string nodeName)
        {
            this.nodeName = nodeName;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> WithJoinOtherProcessorParameters(ProcessorParameters<K, V2> joinOtherProcessParameters)
        {
            this.joinOtherProcessorParameters = joinOtherProcessParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> WithJoinMergeProcessorParameters(ProcessorParameters<K, VR> joinMergeProcessorParameters)
        {
            this.joinMergeProcessorParameters = joinMergeProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> WithThisWindowedStreamProcessorParameters(ProcessorParameters<K, V1> thisWindowedStreamProcessorParameters)
        {
            this.thisWindowedStreamProcessorParameters = thisWindowedStreamProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> WithOtherWindowedStreamProcessorParameters(
             ProcessorParameters<K, V2> otherWindowedStreamProcessorParameters)
        {
            this.otherWindowedStreamProcessorParameters = otherWindowedStreamProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> WithThisWindowStoreBuilder(IStoreBuilder<IWindowStore<K, V1>> thisWindowStoreBuilder)
        {
            this.thisWindowStoreBuilder = thisWindowStoreBuilder;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> WithOtherWindowStoreBuilder(IStoreBuilder<IWindowStore<K, V2>> otherWindowStoreBuilder)
        {
            this.otherWindowStoreBuilder = otherWindowStoreBuilder;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> WithJoined(Joined<K, V1, V2> joined)
        {
            this.joined = joined;
            return this;
        }

        public StreamStreamJoinNode<K, V1, V2, VR> Build()
        {

            return new StreamStreamJoinNode<K, V1, V2, VR>(
                nodeName,
                valueJoiner,
                joinThisProcessorParameters,
                joinOtherProcessorParameters,
                joinMergeProcessorParameters,
                thisWindowedStreamProcessorParameters,
                otherWindowedStreamProcessorParameters,
                thisWindowStoreBuilder,
                otherWindowStoreBuilder,
                joined);
        }
    }
}
