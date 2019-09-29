using Kafka.Streams.Nodes;

namespace Kafka.Streams.Processors.Interfaces
{
    public interface IProcessorNodePunctuator<K, V>
    {
        void punctuate(ProcessorNode<K, V> node, long streamTime, PunctuationType type, Punctuator punctuator);
    }
}