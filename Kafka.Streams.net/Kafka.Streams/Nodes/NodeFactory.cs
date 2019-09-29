using Kafka.Streams.Processors.Internals;

namespace Kafka.Streams.Nodes
{
    public abstract class NodeFactory
    {
        public string name { get; protected set; }
        public string[] predecessors { get; protected set; }
    }

    public abstract class NodeFactory<K, V> : NodeFactory
    {
        public NodeFactory(
            string name,
            string[] predecessors)
        {
            this.name = name;
            this.predecessors = predecessors;
        }

        public abstract ProcessorNode<K, V> build();

        public abstract AbstractNode describe();
    }
}
