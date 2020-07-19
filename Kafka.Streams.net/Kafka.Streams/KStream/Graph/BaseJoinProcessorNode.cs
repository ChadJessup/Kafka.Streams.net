using Kafka.Streams.Interfaces;

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
        private readonly ValueJoiner<V1, V2, VR> valueJoiner;
        public string ThisJoinSideNodeName { get; }
        public string OtherJoinSideNodeName { get; }

        public BaseJoinProcessorNode(
            string nodeName,
            ValueJoiner<V1, V2, VR> valueJoiner,
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
            this.ThisJoinSideNodeName = thisJoinSideNodeName;
            this.OtherJoinSideNodeName = otherJoinSideNodeName;
        }

        protected ProcessorParameters<K, V1> ThisProcessorParameters()
        {
            return this.joinThisProcessorParameters;
        }

        protected ProcessorParameters<K, V2> OtherProcessorParameters()
        {
            return this.joinOtherProcessorParameters;
        }

        protected ProcessorParameters<K, VR> MergeProcessorParameters()
        {
            return this.joinMergeProcessorParameters;
        }

        public override string ToString()
        {
            return "BaseJoinProcessorNode{" +
                   "joinThisProcessorParameters=" + this.joinThisProcessorParameters +
                   ", joinOtherProcessorParameters=" + this.joinOtherProcessorParameters +
                   ", joinMergeProcessorParameters=" + this.joinMergeProcessorParameters +
                   ", valueJoiner=" + this.valueJoiner +
                   ", thisJoinSideNodeName='" + this.ThisJoinSideNodeName + '\'' +
                   ", otherJoinSideNodeName='" + this.OtherJoinSideNodeName + '\'' +
                   "} " + base.ToString();
        }
    }
}
