using Kafka.Streams.Nodes;

namespace Kafka.Streams.Processors.Interfaces
{
    public interface IProcessorNodePunctuator<K, V>
    {
        void punctuate(IProcessorNode<K, V> node, long streamTime, PunctuationType type, IPunctuator punctuator);
    }
}