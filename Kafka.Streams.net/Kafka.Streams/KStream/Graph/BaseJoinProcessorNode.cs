namespace Kafka.Streams.KStream.Internals.Graph
{
    /**
     * Utility base containing the common fields between
     * a Stream-Stream join and a Table-Table join
     */
    public abstract class BaseJoinProcessorNode<K, V1, V2, VR> : StreamsGraphNode
    {
        private readonly ProcessorParameters<K, V1> joinThisProcessorParameters;
        private readonly ProcessorParameters<K, V2> joinOtherProcessorParameters;
        private readonly ProcessorParameters<K, VR> joinMergeProcessorParameters;
        private readonly IValueJoiner<V1, V2, VR> valueJoiner;
        public string thisJoinSideNodeName { get; }
        public string otherJoinSideNodeName { get; }

        public BaseJoinProcessorNode(
            string nodeName,
            IValueJoiner<V1, V2, VR> valueJoiner,
            ProcessorParameters<K, V1> joinThisProcessorParameters,
            ProcessorParameters<K, V2> joinOtherProcessorParameters,
            ProcessorParameters<K, VR> joinMergeProcessorParameters,
            string thisJoinSideNodeName,
            string otherJoinSideNodeName)
            : base(nodeName)
        {
            this.valueJoiner = valueJoiner;
            this.joinThisProcessorParameters = joinThisProcessorParameters;
            this.joinOtherProcessorParameters = joinOtherProcessorParameters;
            this.joinMergeProcessorParameters = joinMergeProcessorParameters;
            this.thisJoinSideNodeName = thisJoinSideNodeName;
            this.otherJoinSideNodeName = otherJoinSideNodeName;
        }

        protected ProcessorParameters<K, V1> thisProcessorParameters()
        {
            return joinThisProcessorParameters;
        }

        protected ProcessorParameters<K, V2> otherProcessorParameters()
        {
            return joinOtherProcessorParameters;
        }

        protected ProcessorParameters<K, VR> mergeProcessorParameters()
        {
            return joinMergeProcessorParameters;
        }

        public override string ToString()
        {
            return "BaseJoinProcessorNode{" +
                   "joinThisProcessorParameters=" + joinThisProcessorParameters +
                   ", joinOtherProcessorParameters=" + joinOtherProcessorParameters +
                   ", joinMergeProcessorParameters=" + joinMergeProcessorParameters +
                   ", valueJoiner=" + valueJoiner +
                   ", thisJoinSideNodeName='" + thisJoinSideNodeName + '\'' +
                   ", otherJoinSideNodeName='" + otherJoinSideNodeName + '\'' +
                   "} " + base.ToString();
        }
    }
}
