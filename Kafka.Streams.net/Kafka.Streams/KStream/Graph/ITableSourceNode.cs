namespace Kafka.Streams.Nodes
{
    public interface ITableSourceNode
    {
        bool ShouldReuseSourceTopicForChangelog { get; set; }
    }
}