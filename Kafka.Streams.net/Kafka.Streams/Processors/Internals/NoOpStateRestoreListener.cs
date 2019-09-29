using Confluent.Kafka;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class NoOpStateRestoreListener : AbstractNotifyingBatchingRestoreCallback, IRecordBatchingStateRestoreCallback
    {
        public void restoreBatch(List<ConsumeResult<byte[], byte[]>> records)
        {
        }
    }
}
