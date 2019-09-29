using Kafka.Streams.Nodes;
using System.Collections.Generic;

namespace Kafka.Streams.Interfaces
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
