
using Confluent.Kafka;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public interface IRecordBatchingStateRestoreCallback : IBatchingStateRestoreCallback
    {
        void RestoreBatch(List<ConsumeResult<byte[], byte[]>> records);
    }
}
