using System;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Interfaces;

namespace Kafka.Streams.Processors.Internals
{
    internal class WrappedProcessorNodePunctuator<K, V> : IProcessorNodePunctuator<K, V>
    {
        private readonly ProcessorNodePunctuator<K, V> processorNodePunctuator;

        public WrappedProcessorNodePunctuator(ProcessorNodePunctuator<K, V> processorNodePunctuator)
        {
            this.processorNodePunctuator = processorNodePunctuator;
        }

        public void Punctuate(IProcessorNode<K, V> node, DateTime streamTime, PunctuationType type, IPunctuator punctuator)
            => this.processorNodePunctuator(node, streamTime, type, punctuator.Punctuate);
    }
}
