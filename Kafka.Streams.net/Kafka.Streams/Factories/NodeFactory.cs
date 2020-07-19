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
        protected KafkaStreamsContext context { get; }

        protected NodeFactory(
            KafkaStreamsContext context,
            string Name,
            string[] predecessors)
        {
            this.context = context;
            this.Name = Name;
            this.Predecessors = predecessors;
        }

        public abstract IProcessorNode Build();

        public abstract INode Describe();
    }
}
