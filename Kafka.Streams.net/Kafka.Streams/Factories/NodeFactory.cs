using Confluent.Kafka;
using Kafka.Streams.Nodes;
using Kafka.Streams.Topologies;
using NodaTime;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Factories
{
    public abstract class NodeFactory<K, V> : INodeFactory
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

        public abstract ProcessorNode<K, V> Build();

        public abstract INode Describe();
    }
}
