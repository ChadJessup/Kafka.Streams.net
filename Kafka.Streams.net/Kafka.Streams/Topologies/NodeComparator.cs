using System;
using System.Collections.Generic;

namespace Kafka.Streams.Topologies
{
    [Serializable]
    public class NodeComparator : IComparer<INode>
    {
        public int Compare(INode node1, INode node2)
        {
            if (node1.Equals(node2))
            {
                return 0;
            }

            var size1 = ((AbstractNode)node1).Size;
            var size2 = ((AbstractNode)node2).Size;

            // it is possible that two nodes have the same sub-tree size (think two nodes connected via state stores)
            // in this case default to processor name string
            if (size1 != size2)
            {
                return size2 - size1;
            }
            else
            {
                return node1.Name.CompareTo(node2.Name);
            }
        }
    }
}
