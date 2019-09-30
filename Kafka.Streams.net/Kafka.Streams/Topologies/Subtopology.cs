using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Topologies
{
    public class Subtopology : ISubtopology
    {
        public int id { get; private set; }
        public HashSet<INode> nodes { get; private set; }

        public Subtopology(int id, HashSet<INode> nodes)
        {
            this.id = id;
            this.nodes = new HashSet<INode>(/*NODE_COMPARATOR*/);
            this.nodes.UnionWith(nodes);
        }

        // visible for testing
        IEnumerator<INode> nodesInOrder()
        {
            return nodes.GetEnumerator();
        }

        public override string ToString()
        {
            return "Sub-topology: " + id + "\n" + nodesAsString() + "\n";
        }

        private string nodesAsString()
        {
            StringBuilder sb = new StringBuilder();
            foreach (INode node in nodes)
            {
                sb.Append("    ");
                sb.Append(node);
                sb.Append('\n');
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

            Subtopology that = (Subtopology)o;
            return id == that.id
                && nodes.Equals(that.nodes);
        }

        public override int GetHashCode()
        {
            return (id, nodes).GetHashCode();
        }
    }
}