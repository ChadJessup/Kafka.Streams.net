using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Kafka.Streams.State.Interfaces;

namespace Kafka.Streams.Processors.Internals
{
    public class StateRestoreCallbackAdapter
    {
        private StateRestoreCallbackAdapter() { }

        public static IRecordBatchingStateRestoreCallback adapt(IStateRestoreCallback restoreCallback)
        {
            restoreCallback = restoreCallback ?? throw new ArgumentNullException(nameof(restoreCallback));

            if (restoreCallback is IRecordBatchingStateRestoreCallback)
            {
                return (IRecordBatchingStateRestoreCallback)restoreCallback;
            }
            else if (restoreCallback is IBatchingStateRestoreCallback)
            {
                return new BatchingStateRestoreCallback(restoreCallback);
            }
            else
            {
                return new BasicRestoreCallback(restoreCallback);
            }
        }

        public class BasicRestoreCallback : IRecordBatchingStateRestoreCallback
        {
            private readonly IStateRestoreCallback restoreCallback;

            public BasicRestoreCallback(IStateRestoreCallback restoreCallback)
            {
                this.restoreCallback = restoreCallback;
            }

            public void restore(byte[] key, byte[] value)
            {
                throw new NotImplementedException();
            }

            public void restoreAll(List<KeyValuePair<byte[], byte[]>> records)
            {
                throw new NotImplementedException();
            }

            public void restoreBatch(List<ConsumeResult<byte[], byte[]>> records)
            {
                foreach (ConsumeResult<byte[], byte[]> record in records)
                {
                    this.restoreCallback.restore(record.Key, record.Value);
                }
            }
        }
    }
}