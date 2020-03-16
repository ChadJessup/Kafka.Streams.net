/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using Confluent.Kafka;
using Kafka.Streams.State.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class StateRestorer
    {
        public static int NO_CHECKPOINT = -1;

        public long offsetLimit { get; }
        private readonly bool persistent;
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
                      bool persistent,
                      string storeName,
                      IRecordConverter recordConverter)
        {
            this.partition = partition;
            this.compositeRestoreListener = compositeRestoreListener;
            this.checkpointOffset = checkpoint == null ? NO_CHECKPOINT : checkpoint;
            this.offsetLimit = offsetLimit;
            this.persistent = persistent;
            this.storeName = storeName;
            this.recordConverter = recordConverter;
        }

        public long checkpoint()
        {
            return checkpointOffset;
        }

        public void setCheckpointOffset(long checkpointOffset)
        {
            this.checkpointOffset = checkpointOffset;
        }

        public void restoreStarted()
        {
            compositeRestoreListener.onRestoreStart(partition, storeName, startingOffset, endingOffset);
        }

        public void restoreDone()
        {
            compositeRestoreListener.onRestoreEnd(partition, storeName, restoredNumRecords());
        }

        public void restoreBatchCompleted(long currentRestoredOffset, int numRestored)
        {
            compositeRestoreListener.onBatchRestored(partition, storeName, currentRestoredOffset, numRestored);
        }

        public void restore(List<ConsumeResult<byte[], byte[]>> records)
        {
            var convertedRecords = new List<ConsumeResult<byte[], byte[]>>(records.Count);
            foreach (ConsumeResult<byte[], byte[]> record in records)
            {
                convertedRecords.Add(recordConverter.convert(record));
            }
            compositeRestoreListener.restoreBatch(convertedRecords);
        }

        public bool isPersistent()
        {
            return persistent;
        }

        public void setUserRestoreListener(IStateRestoreListener userRestoreListener)
        {
            this.compositeRestoreListener.setUserRestoreListener(userRestoreListener);
        }

        public void setRestoredOffset(long restoredOffset)
        {
            this.restoredOffset = Math.Min(offsetLimit, restoredOffset);
        }

        public void setStartingOffset(long startingOffset)
        {
            this.startingOffset = Math.Min(offsetLimit, startingOffset);
        }

        public void setEndingOffset(long endingOffset)
        {
            this.endingOffset = Math.Min(offsetLimit, endingOffset);
        }

        public bool hasCompleted(long recordOffset, long endOffset)
        {
            return endOffset == 0 || recordOffset >= readTo(endOffset);
        }

        long restoredNumRecords()
        {
            return restoredOffset - startingOffset;
        }

        private long readTo(long endOffset)
        {
            return endOffset < offsetLimit ? endOffset : offsetLimit;
        }
    }
}