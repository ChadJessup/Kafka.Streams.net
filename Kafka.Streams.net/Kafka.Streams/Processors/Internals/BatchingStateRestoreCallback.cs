﻿using Confluent.Kafka;
using Kafka.Streams.State.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class BatchingStateRestoreCallback : IBatchingStateRestoreCallback, IRecordBatchingStateRestoreCallback
    {
        private readonly IStateRestoreCallback restoreCallback;

        public BatchingStateRestoreCallback(IStateRestoreCallback restoreCallback)
        {
            this.restoreCallback = restoreCallback;
        }

        public void restore(byte[] key, byte[] value)
        {
            throw new NotImplementedException();
        }

        public void restoreAll(List<KeyValuePair<byte[], byte[]>> records)
        {
            var keyValues = new List<KeyValuePair<byte[], byte[]>>();
            foreach (var record in records)
            {
                keyValues.Add(new KeyValuePair<byte[], byte[]>(record.Key, record.Value));
            }

            ((IBatchingStateRestoreCallback)restoreCallback).restoreAll(keyValues);
        }

        public void restoreBatch(List<ConsumeResult<byte[], byte[]>> records)
        {
            throw new NotImplementedException();
        }
    }
}
