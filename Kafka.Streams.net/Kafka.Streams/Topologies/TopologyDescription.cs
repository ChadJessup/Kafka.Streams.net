using System.Collections.Generic;
using System.Linq;
using System.Text;

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
    public class TopologyDescription : ITopology
    {
        public HashSet<ISubtopology> subtopologies { get; } = new HashSet<ISubtopology>(/*SUBTOPOLOGY_COMPARATOR*/);
        public HashSet<IGlobalStore> globalStores { get; } = new HashSet<IGlobalStore>(/*GLOBALSTORE_COMPARATOR*/);

        public void addSubtopology(ISubtopology subtopology)
        {
            subtopologies.Add(subtopology);
        }

        public void addGlobalStore(IGlobalStore globalStore)
        {
            globalStores.Add(globalStore);
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("Topologies:\n ");
            ISubtopology[] sortedSubtopologies =
                subtopologies.OrderByDescending(t => t.id).ToArray();
            IGlobalStore[] sortedGlobalStores =
                globalStores.OrderByDescending(s => s.id).ToArray();
            var expectedId = 0;
            var subtopologiesIndex = sortedSubtopologies.Length - 1;
            var globalStoresIndex = sortedGlobalStores.Length - 1;
            while (subtopologiesIndex != -1 && globalStoresIndex != -1)
            {
                sb.Append("  ");
                ISubtopology subtopology = sortedSubtopologies[subtopologiesIndex];
                IGlobalStore globalStore = sortedGlobalStores[globalStoresIndex];
                if (subtopology.id == expectedId)
                {
                    sb.Append(subtopology);
                    subtopologiesIndex--;
                }
                else
                {
                    sb.Append(globalStore);
                    globalStoresIndex--;
                }
                expectedId++;
            }

            while (subtopologiesIndex != -1)
            {
                ISubtopology subtopology = sortedSubtopologies[subtopologiesIndex];
                sb.Append("  ");
                sb.Append(subtopology);
                subtopologiesIndex--;
            }
            
            while (globalStoresIndex != -1)
            {
                IGlobalStore globalStore = sortedGlobalStores[globalStoresIndex];
                sb.Append("  ");
                sb.Append(globalStore);
                globalStoresIndex--;
            }

            return sb.ToString();
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || GetType() != o.GetType())
            {
                return false;
            }

            var that = (TopologyDescription)o;
            return subtopologies.Equals(that.subtopologies)
                && globalStores.Equals(that.globalStores);
        }

        public override int GetHashCode()
        {
            return (subtopologies, globalStores).GetHashCode();
        }


    }
}