using Kafka.Streams.Interfaces;
using Kafka.Streams.Nodes;

namespace Kafka.Streams.Topologies
{
    /**
     * Represents a {@link Topology.AddGlobalStore(org.apache.kafka.streams.state.StoreBuilder, string,
     * org.apache.kafka.common.serialization.Deserializer, org.apache.kafka.common.serialization.Deserializer, string,
     * string, org.apache.kafka.streams.processor.IProcessorSupplier) global store}.
     * Adding a global store results in.Adding a source node and one stateful processor node.
     * Note, that all.Added global stores form a single unit (similar to a {@link Subtopology}) even if different
     * global stores are not connected to each other.
     * Furthermore, global stores are available to all processors without connecting them explicitly, and thus global
     * stores will never be part of any {@link Subtopology}.
     */
    public interface IGlobalStore
    {
        /**
         * The source node reading from a "global" topic.
         * @return the "global" source node
         */
        ISource source { get; }

        /**
         * The processor node maintaining the global store.
         * @return the "global" processor node
         */
        IProcessor processor { get; }

        int id { get; }
    }
}
