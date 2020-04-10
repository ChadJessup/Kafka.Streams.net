
using Confluent.Kafka;
using Kafka.Streams.State.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class StateRestorer
    {
        public const int NO_CHECKPOINT = -1;

        public long offsetLimit { get; }
        private readonly bool Persistent;
        public string storeName { get; }
        public TopicPartition partition { get; }
        private readonly CompositeRestoreListener compositeRestoreListener;
        private readonly IRecordConverter recordConverter;

        private long checkpointOffset;
        public long restoredOffset { get; private set; }
        private long startingOffset;
        private long endingOffset;

        public StateRestorer(TopicPartition partition,
                      CompositeRestoreListener compositeRestoreListener,
                      long checkpoint,
                      long offsetLimit,
                      bool Persistent,
                      string storeName,
                      IRecordConverter recordConverter)
        {
            this.partition = partition;
            this.compositeRestoreListener = compositeRestoreListener;
            this.checkpointOffset = checkpoint == null ? NO_CHECKPOINT : checkpoint;
            this.offsetLimit = offsetLimit;
            this.Persistent = Persistent;
            this.storeName = storeName;
            this.recordConverter = recordConverter;
        }

        public long Checkpoint()
        {
            return this.checkpointOffset;
        }

        public void SetCheckpointOffset(long checkpointOffset)
        {
            this.checkpointOffset = checkpointOffset;
        }

        public void RestoreStarted()
        {
            this.compositeRestoreListener.OnRestoreStart(this.partition, this.storeName, this.startingOffset, this.endingOffset);
        }

        public void RestoreDone()
        {
            this.compositeRestoreListener.OnRestoreEnd(this.partition, this.storeName, this.RestoredNumRecords());
        }

        public void RestoreBatchCompleted(long currentRestoredOffset, int numRestored)
        {
            this.compositeRestoreListener.OnBatchRestored(this.partition, this.storeName, currentRestoredOffset, numRestored);
        }

        public void Restore(List<ConsumeResult<byte[], byte[]>> records)
        {
            var convertedRecords = new List<ConsumeResult<byte[], byte[]>>(records.Count);
            foreach (ConsumeResult<byte[], byte[]> record in records)
            {
                convertedRecords.Add(this.recordConverter.Convert(record));
            }
            this.compositeRestoreListener.RestoreBatch(convertedRecords);
        }

        public bool IsPersistent()
        {
            return this.Persistent;
        }

        public void SetUserRestoreListener(IStateRestoreListener userRestoreListener)
        {
            this.compositeRestoreListener.SetUserRestoreListener(userRestoreListener);
        }

        public void SetRestoredOffset(long restoredOffset)
        {
            this.restoredOffset = Math.Min(this.offsetLimit, restoredOffset);
        }

        public void SetStartingOffset(long startingOffset)
        {
            this.startingOffset = Math.Min(this.offsetLimit, startingOffset);
        }

        public void SetEndingOffset(long endingOffset)
        {
            this.endingOffset = Math.Min(this.offsetLimit, endingOffset);
        }

        public bool HasCompleted(long recordOffset, long endOffset)
        {
            return endOffset == 0 || recordOffset >= this.ReadTo(endOffset);
        }

        private long RestoredNumRecords()
        {
            return this.restoredOffset - this.startingOffset;
        }

        private long ReadTo(long endOffset)
        {
            return endOffset < this.offsetLimit ? endOffset : this.offsetLimit;
        }
    }
}
