using Confluent.Kafka;

namespace Kafka.Streams.Tasks
{
    public interface IRestoringTasks
    {
        StreamTask restoringTaskFor(TopicPartition partition);
    }
}