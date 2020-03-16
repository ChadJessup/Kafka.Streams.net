using Kafka.Streams.Nodes;
using Kafka.Streams.Topologies;
using System.Collections.Generic;

namespace Kafka.Streams.Factories
{
    public interface INodeFactory<K, V> : INodeFactory
    {
    }

    public interface INodeFactory
    {
        string Name { get; }
        IEnumerable<string> Predecessors { get; }
        abstract INode Describe();
        abstract IProcessorNode Build();
    }
}