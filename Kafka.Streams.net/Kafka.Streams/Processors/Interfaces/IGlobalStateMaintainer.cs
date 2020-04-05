using Confluent.Kafka;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    /**
     * Interface for maintaining global state stores. see {@link GlobalStateUpdateTask}
     */
    public interface IGlobalStateMaintainer
    {
        Dictionary<TopicPartition, long?> Initialize();
        void FlushState();
        void Close();
        void Update(ConsumeResult<byte[], byte[]> record);
    }
}