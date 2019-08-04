namespace Kafka.Streams.KStream.Internals.Graph
{
    /**
     * Too much information to generalize, so Stream-Stream joins are represented by a specific node.
     */
    public class StreamStreamJoinNode<K, V1, V2, VR> : BaseJoinProcessorNode<K, V1, V2, VR>
    {
        private ProcessorParameters<K, V1> thisWindowedStreamProcessorParameters;
        private ProcessorParameters<K, V2> otherWindowedStreamProcessorParameters;
        private StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder;
        private StoreBuilder<WindowStore<K, V2>> otherWindowStoreBuilder;
        private Joined<K, V1, V2> joined;

        private StreamStreamJoinNode(
            string nodeName,
            ValueJoiner<V1, V2, VR> valueJoiner,
            ProcessorParameters<K, V1> joinThisProcessorParameters,
            ProcessorParameters<K, V2> joinOtherProcessParameters,
            ProcessorParameters<K, VR> joinMergeProcessorParameters,
            ProcessorParameters<K, V1> thisWindowedStreamProcessorParameters,
            ProcessorParameters<K, V2> otherWindowedStreamProcessorParameters,
            StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder,
            StoreBuilder<WindowStore<K, V2>> otherWindowStoreBuilder,
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



        public string ToString()
        {
            return "StreamStreamJoinNode{" +
                   "thisWindowedStreamProcessorParameters=" + thisWindowedStreamProcessorParameters +
                   ", otherWindowedStreamProcessorParameters=" + otherWindowedStreamProcessorParameters +
                   ", thisWindowStoreBuilder=" + thisWindowStoreBuilder +
                   ", otherWindowStoreBuilder=" + otherWindowStoreBuilder +
                   ", joined=" + joined +
                   "} " + base.ToString();
        }


        public void writeToTopology(InternalTopologyBuilder topologyBuilder)
        {

            string thisProcessorName = thisProcessorParameters().processorName();
            string otherProcessorName = otherProcessorParameters().processorName();
            string thisWindowedStreamProcessorName = thisWindowedStreamProcessorParameters.processorName();
            string otherWindowedStreamProcessorName = otherWindowedStreamProcessorParameters.processorName();

            topologyBuilder.AddProcessor(thisProcessorName, thisProcessorParameters().processorSupplier(), thisWindowedStreamProcessorName);
            topologyBuilder.AddProcessor(otherProcessorName, otherProcessorParameters().processorSupplier(), otherWindowedStreamProcessorName);
            topologyBuilder.AddProcessor(mergeProcessorParameters().processorName(), mergeProcessorParameters().processorSupplier(), thisProcessorName, otherProcessorName);
            topologyBuilder.AddStateStore(thisWindowStoreBuilder, thisWindowedStreamProcessorName, otherProcessorName);
            topologyBuilder.AddStateStore(otherWindowStoreBuilder, otherWindowedStreamProcessorName, thisProcessorName);
        }

        public static StreamStreamJoinNodeBuilder<K, V1, V2, VR> streamStreamJoinNodeBuilder()
        {
            return new StreamStreamJoinNodeBuilder<>();
        }

        public static StreamStreamJoinNodeBuilder<K, V1, V2, VR> {

        private string nodeName;
        private ValueJoiner<V1, V2, VR> valueJoiner;
        private ProcessorParameters<K, V1> joinThisProcessorParameters;
        private ProcessorParameters<K, V2> joinOtherProcessorParameters;
        private ProcessorParameters<K, VR> joinMergeProcessorParameters;
        private ProcessorParameters<K, V1> thisWindowedStreamProcessorParameters;
        private ProcessorParameters<K, V2> otherWindowedStreamProcessorParameters;
        private StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder;
        private StoreBuilder<WindowStore<K, V2>> otherWindowStoreBuilder;
        private Joined<K, V1, V2> joined;


        private StreamStreamJoinNodeBuilder()
        {
        }


        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withValueJoiner(ValueJoiner<V1, V2, VR> valueJoiner)
        {
            this.valueJoiner = valueJoiner;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoinThisProcessorParameters(ProcessorParameters<K, V1> joinThisProcessorParameters)
        {
            this.joinThisProcessorParameters = joinThisProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withNodeName(string nodeName)
        {
            this.nodeName = nodeName;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoinOtherProcessorParameters(ProcessorParameters<K, V2> joinOtherProcessParameters)
        {
            this.joinOtherProcessorParameters = joinOtherProcessParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoinMergeProcessorParameters(ProcessorParameters<K, VR> joinMergeProcessorParameters)
        {
            this.joinMergeProcessorParameters = joinMergeProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withThisWindowedStreamProcessorParameters(ProcessorParameters<K, V1> thisWindowedStreamProcessorParameters)
        {
            this.thisWindowedStreamProcessorParameters = thisWindowedStreamProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withOtherWindowedStreamProcessorParameters(
             ProcessorParameters<K, V2> otherWindowedStreamProcessorParameters)
        {
            this.otherWindowedStreamProcessorParameters = otherWindowedStreamProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withThisWindowStoreBuilder(StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder)
        {
            this.thisWindowStoreBuilder = thisWindowStoreBuilder;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withOtherWindowStoreBuilder(StoreBuilder<WindowStore<K, V2>> otherWindowStoreBuilder)
        {
            this.otherWindowStoreBuilder = otherWindowStoreBuilder;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoined(Joined<K, V1, V2> joined)
        {
            this.joined = joined;
            return this;
        }

        public StreamStreamJoinNode<K, V1, V2, VR> build()
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
