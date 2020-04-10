using System.Collections.Generic;

namespace Kafka.Streams.Topologies
{
    /**
     * A processor node of a topology.
     */
    public interface IProcessor : INode
    {
        /**
         * The names of All connected stores.
         * @return set of store names
         */
        HashSet<string> stores { get; }
    }
}
