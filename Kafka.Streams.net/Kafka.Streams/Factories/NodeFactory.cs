using Kafka.Common;
using Kafka.Streams.Nodes;
using Kafka.Streams.Topologies;
using System.Collections.Generic;

namespace Kafka.Streams.Factories
{
    public abstract class NodeFactory<K, V> : INodeFactory<K, V>
    {
        public string Name { get; }
        public IEnumerable<string> Predecessors { get; }
        protected IClock Clock { get; }

        protected NodeFactory(
            IClock clock,
            string name,
            string[] predecessors)
        {
            this.Clock = clock;
            this.Name = name;
            this.Predecessors = predecessors;
        }

        public abstract IProcessorNode Build();

        public abstract INode Describe();
    }
}
