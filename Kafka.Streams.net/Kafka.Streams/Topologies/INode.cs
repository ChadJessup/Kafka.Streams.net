using System.Collections.Generic;

namespace Kafka.Streams.Topologies
{
    /**
     * A node of a topology. Can be a source, sink, or processor node.
     */
    public interface INode
    {
        /**
         * The Name of the node. Will never be {@code null}.
         * @return the Name of the node
         */
        string Name { get; }

        /**
         * The predecessors of this node within a sub-topology.
         * Note, sources do not have any predecessors.
         * Will never be {@code null}.
         * @return set of All predecessors
         */
        HashSet<INode> Predecessors { get; }
        /**
         * The successor of this node within a sub-topology.
         * Note, sinks do not have any successors.
         * Will never be {@code null}.
         * @return set of All successor
         */
        HashSet<INode> Successors { get; }
    }
}
