using Confluent.Kafka;
using Kafka.Streams.Processor.Internals;

namespace Kafka.Streams.Processor.Interfaces
{
    public interface IRestoringTasks
    {
        StreamTask restoringTaskFor(TopicPartition partition);
    }
}