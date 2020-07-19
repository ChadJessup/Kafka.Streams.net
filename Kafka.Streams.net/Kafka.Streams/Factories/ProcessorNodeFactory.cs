using System.Collections.Generic;
using System.Linq;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Topologies;

namespace Kafka.Streams.Factories
{
    public class ProcessorNodeFactory<K, V> : NodeFactory<K, V>, IProcessorNodeFactory
    {
        public HashSet<string> stateStoreNames { get; } = new HashSet<string>();
        private readonly IProcessorSupplier supplier;

        public ProcessorNodeFactory(
            KafkaStreamsContext context,
            string Name,
            string[] predecessors,
            IProcessorSupplier supplier)
            : base(context, Name, predecessors.Select(p => p).ToArray())
        {
            this.supplier = supplier;
        }

        public void AddStateStore(string stateStoreName)
        {
            this.stateStoreNames.Add(stateStoreName);
        }

        public override IProcessorNode Build()
        {
            return new ProcessorNode<K, V>(this.Context, this.Name, this.supplier.Get(), this.stateStoreNames);
        }

        public override INode Describe()
        {
            return new Processor(this.Name, new HashSet<string>(this.stateStoreNames));
        }
    }
}
