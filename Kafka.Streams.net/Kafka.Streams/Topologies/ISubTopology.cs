using System.Collections.Generic;

namespace Kafka.Streams.Topologies
{
    /**
     * A connected sub-graph of a {@link Topology}.
     * <p>
     * Nodes of a {@code Subtopology} are connected {@link Topology.AddProcessor(string,
     * org.apache.kafka.streams.processor.IProcessorSupplier, string...) directly} or indirectly via
     * {@link Topology#connectProcessorAndStateStores(string, string...) state stores}
     * (i.e., if multiple processors share the same state).
     */
    public interface ISubtopology
    {
        /**
         * Internally assigned unique ID.
         * @return the ID of the sub-topology
         */
        int id { get; }

        /**
         * All nodes of this sub-topology.
         * @return set of all nodes within the sub-topology
         */
        HashSet<INode> nodes { get; }
    }
}
