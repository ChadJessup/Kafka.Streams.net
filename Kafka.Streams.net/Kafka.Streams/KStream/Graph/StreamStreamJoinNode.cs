using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.Topologies;
using System;

namespace Kafka.Streams.KStream.Internals.Graph
{
    /**
     * Too much information to generalize, so Stream-Stream joins are represented by a specific node.
     */
     public static class StreamStreamJoinNode
    {
        public static StreamStreamJoinNodeBuilder<K, V1, V2, VR> streamStreamJoinNodeBuilder<K, V1, V2, VR>()
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
            IValueJoiner<V1, V2, VR> valueJoiner,
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
                   "thisWindowedStreamProcessorParameters=" + thisWindowedStreamProcessorParameters +
                   ", otherWindowedStreamProcessorParameters=" + otherWindowedStreamProcessorParameters +
                   ", thisWindowStoreBuilder=" + thisWindowStoreBuilder +
                   ", otherWindowStoreBuilder=" + otherWindowStoreBuilder +
                   ", joined=" + joined +
                   "} " + base.ToString();
        }

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            topologyBuilder = topologyBuilder ?? throw new ArgumentNullException(nameof(topologyBuilder));

            string thisProcessorName = thisProcessorParameters().ProcessorName;
            string otherProcessorName = otherProcessorParameters().ProcessorName;
            string thisWindowedStreamProcessorName = thisWindowedStreamProcessorParameters.ProcessorName;
            string otherWindowedStreamProcessorName = otherWindowedStreamProcessorParameters.ProcessorName;

            topologyBuilder.AddProcessor(thisProcessorName, thisProcessorParameters().ProcessorSupplier, thisWindowedStreamProcessorName);
            topologyBuilder.AddProcessor(otherProcessorName, otherProcessorParameters().ProcessorSupplier, otherWindowedStreamProcessorName);

            topologyBuilder.AddProcessor(
                mergeProcessorParameters().ProcessorName,
                mergeProcessorParameters().ProcessorSupplier,
                thisProcessorName, otherProcessorName);

            //topologyBuilder.addStateStore(
            //    thisWindowStoreBuilder,
            //    thisWindowedStreamProcessorName,
            //    otherProcessorName);

            //topologyBuilder.addStateStore(
            //    otherWindowStoreBuilder,
            //    otherWindowedStreamProcessorName,
            //    thisProcessorName);
        }
    }
}
