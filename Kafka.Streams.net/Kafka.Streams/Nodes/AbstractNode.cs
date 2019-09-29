using System;
using System.Collections.Generic;

namespace Kafka.Streams.Nodes
{
    public abstract class AbstractNode : INode
    {
        public string Name { get; }
        public HashSet<INode> Predecessors { get; } = new HashSet<INode>(/*NODE_COMPARATOR*/);
        public HashSet<INode> Successors { get; } = new HashSet<INode>(/*NODE_COMPARATOR*/);

        // size of the sub-topology rooted at this node, including the node itself
        public int Size { get; set; }

        public AbstractNode(string name)
        {
            this.Name = name ?? throw new ArgumentNullException(nameof(name));
            this.Size = 1;
        }

        public virtual void AddPredecessor(INode predecessor)
            => Predecessors.Add(predecessor);

        public void AddSuccessor(INode successor)
            => Successors.Add(successor);
    }
}
