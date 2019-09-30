using Kafka.Streams.Nodes;
using Kafka.Streams.Topologies;
using System.Collections.Generic;

namespace Kafka.Streams.Topologies
{
    /**
     * A processor node of a topology.
     */
    public interface IProcessor : INode
    {
        /**
         * The names of all connected stores.
         * @return set of store names
         */
        HashSet<string> stores { get; }
    }
}
