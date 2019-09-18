using Kafka.Streams.Processor.Interfaces;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Kafka.Streams.Processor.Internals
{
    public class ProcessorTopology
    {
        private readonly List<ProcessorNode> processorNodes;
        private readonly Dictionary<string, SourceNode> sourcesByTopic;
        private readonly Dictionary<string, ISinkNode> sinksByTopic;

        public ProcessorTopology(
            IEnumerable<ProcessorNode> processorNodes,
            Dictionary<string, SourceNode> sourcesByTopic,
            Dictionary<string, ISinkNode> sinksByTopic,
            IEnumerable<IStateStore> stateStores,
            IEnumerable<IStateStore> globalStateStores,
            Dictionary<string, string> storeToChangelogTopic,
            HashSet<string> repartitionTopics)
        {
            this.processorNodes = processorNodes.ToList();
            this.sourcesByTopic = sourcesByTopic;
            this.sinksByTopic = sinksByTopic;
            this.stateStores = stateStores.ToList();
            this.globalStateStores = globalStateStores.ToList();
            this.storeToChangelogTopic = storeToChangelogTopic;
            this.repartitionTopics = repartitionTopics;
        }

        public List<IStateStore> stateStores { get; }
        public List<IStateStore> globalStateStores { get; }
        public Dictionary<string, string> storeToChangelogTopic { get; }
        protected HashSet<string> repartitionTopics { get; }

        public bool hasPersistentLocalStore()
        {
            foreach (IStateStore store in stateStores)
            {
                if (store.persistent())
                {
                    return true;
                }
            }

            return false;
        }

        public bool hasPersistentGlobalStore()
        {
            foreach (IStateStore store in globalStateStores)
            {
                if (store.persistent())
                {
                    return true;
                }
            }

            return false;
        }

        public SourceNode source(string topic)
            => sourcesByTopic[topic];

        public HashSet<SourceNode> sources()
            => new HashSet<SourceNode>(sourcesByTopic.Values);

        public HashSet<string> sinkTopics()
        {
            return new HashSet<string>(sinksByTopic.Keys);
        }

        public ISinkNode sink(string topic)
        {
            return sinksByTopic[topic];
        }

        public List<ProcessorNode> processors()
            => processorNodes;

        public bool isRepartitionTopic(string topic)
            => repartitionTopics.Contains(topic);

        private string childrenToString(string indent, List<ProcessorNode> children)
        {
            if (children == null || !children.Any())
            {
                return "";
            }

            StringBuilder sb = new StringBuilder(indent + "\tchildren:\t[");
            foreach (var child in children)
            {
                sb.Append(child.name);
                sb.Append(", ");
            }

            sb.Length -= 2;  // Remove the last comma
            sb.Append("]\n");

            // recursively print children
            foreach (ProcessorNode child in children)
            {
                sb.Append(child.ToString(indent)).Append(childrenToString(indent, child.children));
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
            StringBuilder sb = new StringBuilder(indent + "ProcessorTopology:\n");

            // start from sources
            foreach (var source in sourcesByTopic.Values)
            {
                sb.Append(source.ToString(indent + "\t")).Append(childrenToString(indent + "\t", source.children));
            }

            return sb.ToString();
        }

        // for testing only
        public HashSet<string> processorConnectedStateStores(string processorName)
        {
            foreach (ProcessorNode node in processorNodes)
            {
                if (node.name.Equals(processorName))
                {
                    return node.stateStores;
                }
            }

            return new HashSet<string>();
        }
    }
}