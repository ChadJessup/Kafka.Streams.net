using Kafka.Streams.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Windowed;
using Kafka.Streams.Topologies;
using System;

namespace Kafka.Streams.KStream.Internals.Graph
{
    /**
     * Too much information to generalize, so Stream-Stream joins are represented by a specific node.
     */
    public static class StreamStreamJoinNode
    {
        public static StreamStreamJoinNodeBuilder<K, V1, V2, VR> StreamStreamJoinNodeBuilder<K, V1, V2, VR>()
        {
            return new StreamStreamJoinNodeBuilder<K, V1, V2, VR>();
        }
    }

    public class StreamStreamJoinNode<K, V1, V2, VR> : BaseJoinProcessorNode<K, V1, V2, VR>
    {
        private readonly ProcessorParameters<K, V1> thisWindowedStreamProcessorParameters;
        private readonly ProcessorParameters<K, V2> otherWindowedStreamProcessorParameters;
        private readonly IStoreBuilder<IWindowStore<K, V1>> thisWindowStoreBuilder;
        private readonly IStoreBuilder<IWindowStore<K, V2>> otherWindowStoreBuilder;
        private readonly Joined<K, V1, V2> joined;

        public StreamStreamJoinNode(
            string nodeName,
            ValueJoiner<V1, V2, VR> valueJoiner,
            ProcessorParameters<K, V1> joinThisProcessorParameters,
            ProcessorParameters<K, V2> joinOtherProcessParameters,
            ProcessorParameters<K, VR> joinMergeProcessorParameters,
            ProcessorParameters<K, V1> thisWindowedStreamProcessorParameters,
            ProcessorParameters<K, V2> otherWindowedStreamProcessorParameters,
            IStoreBuilder<IWindowStore<K, V1>> thisWindowStoreBuilder,
            IStoreBuilder<IWindowStore<K, V2>> otherWindowStoreBuilder,
            Joined<K, V1, V2> joined)
            : base(nodeName,
                  valueJoiner,
                  joinThisProcessorParameters,
                  joinOtherProcessParameters,
                  joinMergeProcessorParameters,
                  null,
                  null)
        {
            this.thisWindowStoreBuilder = thisWindowStoreBuilder;
            this.otherWindowStoreBuilder = otherWindowStoreBuilder;
            this.joined = joined;
            this.thisWindowedStreamProcessorParameters = thisWindowedStreamProcessorParameters;
            this.otherWindowedStreamProcessorParameters = otherWindowedStreamProcessorParameters;
        }

        public override string ToString()
        {
            return "StreamStreamJoinNode{" +
                   "thisWindowedStreamProcessorParameters=" + this.thisWindowedStreamProcessorParameters +
                   ", otherWindowedStreamProcessorParameters=" + this.otherWindowedStreamProcessorParameters +
                   ", thisWindowStoreBuilder=" + this.thisWindowStoreBuilder +
                   ", otherWindowStoreBuilder=" + this.otherWindowStoreBuilder +
                   ", joined=" + this.joined +
                   "} " + base.ToString();
        }

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            topologyBuilder = topologyBuilder ?? throw new ArgumentNullException(nameof(topologyBuilder));

            var thisProcessorName = this.ThisProcessorParameters().ProcessorName;
            var otherProcessorName = this.OtherProcessorParameters().ProcessorName;
            var thisWindowedStreamProcessorName = this.thisWindowedStreamProcessorParameters.ProcessorName;
            var otherWindowedStreamProcessorName = this.otherWindowedStreamProcessorParameters.ProcessorName;

            topologyBuilder.AddProcessor<K, V1>(thisProcessorName, this.ThisProcessorParameters().ProcessorSupplier, thisWindowedStreamProcessorName);
            topologyBuilder.AddProcessor<K, V1>(otherProcessorName, this.OtherProcessorParameters().ProcessorSupplier, otherWindowedStreamProcessorName);

            topologyBuilder.AddProcessor<K, V1>(
                this.MergeProcessorParameters().ProcessorName,
                this.MergeProcessorParameters().ProcessorSupplier,
                thisProcessorName, otherProcessorName);

            topologyBuilder.AddStateStore<K, V1, IWindowStore<K, V1>>(
                this.thisWindowStoreBuilder,
                new[] { otherProcessorName });

            topologyBuilder.AddStateStore<K, V2, IWindowStore<K, V2>>(
                this.otherWindowStoreBuilder,
                new[] { thisProcessorName });
        }
    }
}
