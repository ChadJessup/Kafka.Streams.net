using Kafka.Streams.State;
using Kafka.Streams.Topologies;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class StateStoreNode<T> : StreamsGraphNode
        where T : IStateStore
    {
        protected IStoreBuilder<T> storeBuilder;

        public StateStoreNode(IStoreBuilder<T> storeBuilder)
            : base(storeBuilder.name)
        {
            this.storeBuilder = storeBuilder;
        }

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            // topologyBuilder.addStateStore(storeBuilder, false, Array.Empty<string>());
        }

        public override string ToString()
        {
            return "StateStoreNode{" +
                   $" name='{storeBuilder.name}'" +
                   $", logConfig={storeBuilder.logConfig}" +
                   $", loggingEnabled='{storeBuilder.loggingEnabled}'" +
                   "} ";
        }
    }
}
