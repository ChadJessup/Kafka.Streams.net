using Confluent.Kafka;

namespace Kafka.Streams.Tasks
{
    public interface IRestoringTasks
    {
        StreamTask RestoringTaskFor(TopicPartition partition);
    }
}