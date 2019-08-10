using System.Collections.Generic;

namespace Kafka.Streams.Interfaces
{
    /**
     * A node of a topology. Can be a source, sink, or processor node.
     */
    public interface INode
    {
        /**
         * The name of the node. Will never be {@code null}.
         * @return the name of the node
         */
        string name { get; }

        /**
         * The predecessors of this node within a sub-topology.
         * Note, sources do not have any predecessors.
         * Will never be {@code null}.
         * @return set of all predecessors
         */
        HashSet<INode> predecessors { get; }
        /**
         * The successor of this node within a sub-topology.
         * Note, sinks do not have any successors.
         * Will never be {@code null}.
         * @return set of all successor
         */
        HashSet<INode> successors { get; }
    }
}
