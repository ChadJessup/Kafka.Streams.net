
using Confluent.Kafka;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Interfaces
{
    // Interface to indicate that an object has associated partition offsets that can be checkpointed
    public interface ICheckpointable
    {
        void Checkpoint(Dictionary<TopicPartition, long> offsets);
        Dictionary<TopicPartition, long?> Checkpointed();
    }
}
