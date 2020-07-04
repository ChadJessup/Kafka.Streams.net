using System;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Internals;

namespace Kafka.Streams.Processors.Interfaces
{
    public delegate void ProcessorNodePunctuator<K, V>(
        IProcessorNode<K, V> node,
        DateTime now,
        PunctuationType type,
        Action<DateTime> punctuator);

    public interface IProcessorNodePunctuator<K, V>
    {
        public void Punctuate(
            IProcessorNode<K, V> node,
            DateTime streamTime,
            PunctuationType type,
            Action<DateTime> punctuator)
            => this.Punctuate(
                node,
                streamTime,
                type,
                new WrappedPunctuator(punctuator));

        void Punctuate(
            IProcessorNode<K, V> node,
            DateTime streamTime,
            PunctuationType type,
            IPunctuator punctuator);
    }
}
