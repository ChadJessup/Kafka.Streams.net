﻿using System.Collections.Generic;
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

        public void AddSubtopology(ISubtopology subtopology)
        {
            this.subtopologies.Add(subtopology);
        }

        public void AddGlobalStore(IGlobalStore globalStore)
        {
            this.globalStores.Add(globalStore);
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("Topologies:\n ");
            ISubtopology[] sortedSubtopologies =
                this.subtopologies.OrderByDescending(t => t.id).ToArray();
            IGlobalStore[] sortedGlobalStores =
                this.globalStores.OrderByDescending(s => s.id).ToArray();
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
            if (o == null || this.GetType() != o.GetType())
            {
                return false;
            }

            var that = (TopologyDescription)o;
            return this.subtopologies.Equals(that.subtopologies)
                && this.globalStores.Equals(that.globalStores);
        }

        public override int GetHashCode()
        {
            return (this.subtopologies, this.globalStores).GetHashCode();
        }


    }
}