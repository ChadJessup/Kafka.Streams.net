using System.Collections.Generic;

namespace Kafka.Streams.Topologies
{
    /**
     * A meta representation of a {@link Topology topology}.
     * <p>
     * The nodes of a topology are grouped into {@link Subtopology sub-topologies} if they are connected.
     * In contrast, two sub-topologies are not connected but can be linked to each other via topics, i.e., if one
     * sub-topology {@link Topology.AddSink(string, string, string...) writes} into a topic and another sub-topology
     * {@link Topology.AddSource(string, string...) reads} from the same topic.
     * <p>
     * When {@link KafkaStreams#start()} is called, different sub-topologies will be constructed and executed as independent
     * {@link StreamTask tasks}.
     */
    public interface ITopology
    {
        /**
         * All sub-topologies of the represented topology.
         * @return set of All sub-topologies
         */
        HashSet<ISubtopology> subtopologies { get; }

        /**
         * All global stores of the represented topology.
         * @return set of All global stores
         */
        HashSet<IGlobalStore> globalStores { get; }
    }
}