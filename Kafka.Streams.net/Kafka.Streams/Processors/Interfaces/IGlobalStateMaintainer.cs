using Confluent.Kafka;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    /**
     * Interface for maintaining global state stores. see {@link GlobalStateUpdateTask}
     */
    public interface IGlobalStateMaintainer
    {
        Dictionary<TopicPartition, long?> initialize();
        void flushState();
        void close();
        void update(ConsumeResult<byte[], byte[]> record);
    }
}