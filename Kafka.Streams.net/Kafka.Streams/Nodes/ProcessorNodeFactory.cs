using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Topologies;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Nodes
{
    public class ProcessorNodeFactory<K, V> : NodeFactory<K, V>
    {
        private readonly IProcessorSupplier<K, V> supplier;
        public HashSet<string> stateStoreNames { get; } = new HashSet<string>();

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

        public override ProcessorNode<K, V> build()
        {
            return new ProcessorNode<K, V>(name, supplier.get(), stateStoreNames);
        }

        public override INode describe()
        {
            return new Processor(name, new HashSet<string>(stateStoreNames));
        }
    }
}
