using Confluent.Kafka;
using Kafka.Streams.Nodes;
using Kafka.Streams.Topologies;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Factories
{
    public abstract class NodeFactory<K, V> : INodeFactory
    {
        public string Name { get; }
        public IEnumerable<string> Predecessors { get; }

        protected NodeFactory(
            string name,
            string[] predecessors)
        {
            this.Name = name;
            this.Predecessors = predecessors;
        }

        public abstract ProcessorNode<K, V> Build();

        public abstract INode Describe();
    }
}
