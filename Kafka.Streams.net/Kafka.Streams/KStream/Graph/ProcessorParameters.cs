using System;
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
    public class ProcessorParameters<K, V>
    {
        public IProcessorSupplier<K, V> ProcessorSupplier { get; }
        public string processorName { get; }

        public ProcessorParameters(
            IProcessorSupplier<K, V> processorSupplier,
            string processorName)
        {
            this.ProcessorSupplier = processorSupplier;
            this.processorName = processorName;
        }

        public override string ToString()
            => $"ProcessorParameters{{processor={ProcessorSupplier.GetType()}, " +
            $"processor name='{processorName}'}}";

        //public static ProcessorParameters<K, VR> ConvertFrom<VR>(
        //    ProcessorParameters<K, Change<V>> processorParameters)
        //{
        //    var pp = new ProcessorParameters<K, VR>(
        //        processorParameters.ProcessorSupplier,
        //        processorParameters.processorName);
        //}
    }
}
