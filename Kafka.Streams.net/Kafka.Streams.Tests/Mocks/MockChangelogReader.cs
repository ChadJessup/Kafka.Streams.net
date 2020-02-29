using Confluent.Kafka;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Tasks;
using System.Collections.Generic;

public class MockChangelogReader : IChangelogReader
{
    private readonly List<TopicPartition> registered = new List<TopicPartition>();
    private Dictionary<TopicPartition, long> _restoredOffsets = new Dictionary<TopicPartition, long>();

    public void register(StateRestorer restorer)
    {
        registered.Add(restorer.partition);
    }

    public List<TopicPartition> restore(IRestoringTasks active)
    {
        return registered;
    }

    public Dictionary<TopicPartition, long> restoredOffsets()
    {
        return _restoredOffsets;
    }

    void setRestoredOffsets(Dictionary<TopicPartition, long> restoredOffsets)
    {
        this._restoredOffsets = restoredOffsets;
    }

    public void reset()
    {
        registered.Clear();
    }

    public bool wasRegistered(TopicPartition partition)
    {
        return registered.Contains(partition);
    }
}
