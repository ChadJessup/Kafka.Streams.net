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
            string Name,
            string[] predecessors,
            IProcessorSupplier supplier)
            : base(clock, Name, predecessors.Select(p => p).ToArray())
        {
            this.supplier = supplier;
        }

        public void AddStateStore(string stateStoreName)
        {
            this.stateStoreNames.Add(stateStoreName);
        }

        public override IProcessorNode Build()
        {
            return new ProcessorNode<K, V>(this.Clock, this.Name, this.supplier.Get(), this.stateStoreNames);
        }

        public override INode Describe()
        {
            return new Processor(this.Name, new HashSet<string>(this.stateStoreNames));
        }
    }
}
