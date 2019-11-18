using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Topologies;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Factories
{
    public class ProcessorNodeFactory<K, V> : NodeFactory<K, V>, IProcessorNodeFactory
    {
        public HashSet<string> stateStoreNames { get; } = new HashSet<string>();
        private readonly IProcessorSupplier<K, V> supplier;

        public ProcessorNodeFactory(
            string name,
            string[] predecessors,
            IProcessorSupplier<K, V> supplier)
            : base(name, predecessors.Select(p => p).ToArray())
        {
            this.supplier = supplier;
        }

        public void addStateStore(string stateStoreName)
        {
            stateStoreNames.Add(stateStoreName);
        }

        public override ProcessorNode<K, V> Build()
        {
            return new ProcessorNode<K, V>(Name, supplier.get(), stateStoreNames);
        }

        public override INode Describe()
        {
            return new Processor(Name, new HashSet<string>(stateStoreNames));
        }
    }
}
