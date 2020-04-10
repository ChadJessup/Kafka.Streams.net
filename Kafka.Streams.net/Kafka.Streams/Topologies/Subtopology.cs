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

        public override string ToString()
        {
            return "Sub-topology: " + this.id + "\n" + this.NodesAsString() + "\n";
        }

        private string NodesAsString()
        {
            var sb = new StringBuilder();
            foreach (INode node in this.nodes)
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

            if (o == null || this.GetType() != o.GetType())
            {
                return false;
            }

            var that = (Subtopology)o;
            return this.id == that.id
                && this.nodes.Equals(that.nodes);
        }

        public override int GetHashCode()
        {
            return (this.id, this.nodes).GetHashCode();
        }
    }
}