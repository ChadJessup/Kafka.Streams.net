using Kafka.Common;
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
        private readonly IProcessorSupplier supplier;

        public ProcessorNodeFactory(
            IClock clock,
            string name,
            string[] predecessors,
            IProcessorSupplier supplier)
            : base(clock, name, predecessors.Select(p => p).ToArray())
        {
            this.supplier = supplier;
        }

        public void AddStateStore(string stateStoreName)
        {
            stateStoreNames.Add(stateStoreName);
        }

        public override IProcessorNode Build()
        {
            return new ProcessorNode<K, V>(this.Clock, Name, supplier.Get(), stateStoreNames);
        }

        public override INode Describe()
        {
            return new Processor(Name, new HashSet<string>(stateStoreNames));
        }
    }
}
