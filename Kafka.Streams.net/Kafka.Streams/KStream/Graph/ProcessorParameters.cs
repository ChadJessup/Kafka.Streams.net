using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals.Graph
{
    /**
     * Class used to represent a {@link IProcessorSupplier} and the name
     * used to register it with the {@link org.apache.kafka.streams.processor.Internals.InternalTopologyBuilder}
     *
     * Used by the Join nodes as there are several parameters, this abstraction helps
     * keep the number of arguments more reasonable.
     */
    public class ProcessorParameters : IProcessorParameters
    {
        public ProcessorParameters(IProcessorSupplier processorSupplier, string processorName)
        {
            this.ProcessorName = processorName;
            this.ProcessorSupplier = processorSupplier;
        }

        public string ProcessorName { get; }
        public IProcessorSupplier ProcessorSupplier { get; }
    }

    public interface IProcessorParameters<K, V> : IProcessorParameters
    {
        new IProcessorSupplier<K, V> ProcessorSupplier { get; }
    }

    public interface IProcessorParameters
    {
        string ProcessorName { get; }
        IProcessorSupplier ProcessorSupplier { get; }
    }

    public class ProcessorParameters<K, V> : ProcessorParameters, IProcessorParameters<K, V>
    {
        public new IProcessorSupplier<K, V> ProcessorSupplier { get; }

        public ProcessorParameters(IProcessorSupplier<K, V> processorSupplier, string processorName)
            : base(processorSupplier, processorName)
        {
            this.ProcessorSupplier = processorSupplier;
        }

        public override string ToString()
            => $"ProcessorParameters{{processor={ProcessorSupplier.GetType().Name}, " +
            $"processor name='{ProcessorName}'}}";
    }
}
