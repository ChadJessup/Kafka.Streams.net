using Kafka.Streams.Nodes;
using Kafka.Streams.State;
using Kafka.Streams.Topologies;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Kafka.Streams.Processors.Internals
{
    public class ProcessorTopology
    {
        private readonly List<IProcessorNode> processorNodes;
        private readonly Dictionary<string, ISourceNode> sourcesByTopic;
        private readonly Dictionary<string, ISinkNode> sinksByTopic;

        public ProcessorTopology(
            IEnumerable<IProcessorNode> processorNodes,
            Dictionary<string, ISourceNode> sourcesByTopic,
            Dictionary<string, ISinkNode> sinksByTopic,
            IEnumerable<IStateStore> stateStores,
            IEnumerable<IStateStore> globalStateStores,
            Dictionary<string, string> storeToChangelogTopic,
            HashSet<string> repartitionTopics)
        {
            this.processorNodes = processorNodes.ToList();
            this.sourcesByTopic = sourcesByTopic;
            this.sinksByTopic = sinksByTopic;
            this.StateStores = stateStores.ToList();
            this.globalStateStores = globalStateStores.ToList();
            this.StoreToChangelogTopic = storeToChangelogTopic;
            this.repartitionTopics = repartitionTopics;
        }

        public List<IStateStore> StateStores { get; }
        public List<IStateStore> globalStateStores { get; }
        public Dictionary<string, string> StoreToChangelogTopic { get; }
        protected HashSet<string> repartitionTopics { get; }

        public List<string> SourceTopics => sourcesByTopic.Keys.ToList();

        public bool HasPersistentLocalStore()
        {
            foreach (IStateStore store in StateStores)
            {
                if (store.Persistent())
                {
                    return true;
                }
            }

            return false;
        }

        public bool HasPersistentGlobalStore()
        {
            foreach (IStateStore store in globalStateStores)
            {
                if (store.Persistent())
                {
                    return true;
                }
            }

            return false;
        }

        public ISourceNode Source(string topic)
            => sourcesByTopic[topic];

        public HashSet<ISourceNode> Sources()
            => new HashSet<ISourceNode>(sourcesByTopic.Values);

        public HashSet<string> SinkTopics()
        {
            return new HashSet<string>(sinksByTopic.Keys);
        }

        public ISinkNode Sink(string topic)
        {
            return sinksByTopic[topic];
        }

        public List<IProcessorNode> Processors()
            => processorNodes;

        public bool IsRepartitionTopic(string topic)
            => repartitionTopics.Contains(topic);

        private string ChildrenToString(string indent, List<IProcessorNode> children)
        {
            if (children == null || !children.Any())
            {
                return "";
            }

            var sb = new StringBuilder(indent + "\tchildren:\t[");
            foreach (var child in children)
            {
                sb.Append(child.Name);
                sb.Append(", ");
            }

            sb.Length -= 2;  // Remove the last comma
            sb.Append("]\n");

            // recursively print children
            foreach (var child in children)
            {
                sb.Append(child.ToString(indent)).Append(ChildrenToString(indent, child.Children));
            }

            return sb.ToString();
        }

        /**
         * Produces a string representation containing useful information this topology starting with the given indent.
         * This is useful in debugging scenarios.
         * @return A string representation of this instance.
         */

        public override string ToString()
            => ToString("");

        /**
         * Produces a string representation containing useful information this topology.
         * This is useful in debugging scenarios.
         * @return A string representation of this instance.
         */
        public string ToString(string indent)
        {
            var sb = new StringBuilder(indent + "ProcessorTopology:\n");

            // start from sources
            foreach (var source in sourcesByTopic.Values)
            {
                sb.Append(source.ToString(indent + "\t")).Append(ChildrenToString(indent + "\t", source.Children));
            }

            return sb.ToString();
        }

        // for testing only
        public HashSet<string> ProcessorConnectedStateStores(string processorName)
        {
            foreach (ProcessorNode node in processorNodes)
            {
                if (node.Name.Equals(processorName))
                {
                    return node.StateStores;
                }
            }

            return new HashSet<string>();
        }
    }
}
