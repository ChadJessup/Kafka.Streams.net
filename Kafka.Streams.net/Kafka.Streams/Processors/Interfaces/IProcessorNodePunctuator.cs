using System;
using Kafka.Streams.Nodes;

namespace Kafka.Streams.Processors.Interfaces
{
    public interface IProcessorNodePunctuator<K, V>
    {
        void Punctuate(
            IProcessorNode<K, V> node,
            DateTime streamTime,
            PunctuationType type,
            IPunctuator punctuator);
    }
}
