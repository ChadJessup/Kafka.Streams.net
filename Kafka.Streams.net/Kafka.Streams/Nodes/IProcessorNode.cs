namespace Kafka.Streams.Nodes
{
    public interface IProcessorNode<K, V> : IProcessorNode
    {
        void AddChild(IProcessorNode<K, V> child);
        new IProcessorNode<K, V> GetChild(string childName);
    }
}
