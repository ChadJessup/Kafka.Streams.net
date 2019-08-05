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
namespace Kafka.Streams.Processor.Internals;


using Kafka.Common.TopicPartition;






public class StateRestorer
{


    static int NO_CHECKPOINT = -1;

    private long offsetLimit;
    private bool persistent;
    private string storeName;
    private TopicPartition partition;
    private CompositeRestoreListener compositeRestoreListener;
    private RecordConverter recordConverter;

    private long checkpointOffset;
    private long restoredOffset;
    private long startingOffset;
    private long endingOffset;

    StateRestorer(TopicPartition partition,
                  CompositeRestoreListener compositeRestoreListener,
                  long checkpoint,
                  long offsetLimit,
                  bool persistent,
                  string storeName,
                  RecordConverter recordConverter)
{
        this.partition = partition;
        this.compositeRestoreListener = compositeRestoreListener;
        this.checkpointOffset = checkpoint == null ? NO_CHECKPOINT : checkpoint;
        this.offsetLimit = offsetLimit;
        this.persistent = persistent;
        this.storeName = storeName;
        this.recordConverter = recordConverter;
    }

    public TopicPartition partition()
{
        return partition;
    }

    public string storeName()
{
        return storeName;
    }

    long checkpoint()
{
        return checkpointOffset;
    }

    void setCheckpointOffset(long checkpointOffset)
{
        this.checkpointOffset = checkpointOffset;
    }

    void restoreStarted()
{
        compositeRestoreListener.onRestoreStart(partition, storeName, startingOffset, endingOffset);
    }

    void restoreDone()
{
        compositeRestoreListener.onRestoreEnd(partition, storeName, restoredNumRecords());
    }

    void restoreBatchCompleted(long currentRestoredOffset, int numRestored)
{
        compositeRestoreListener.onBatchRestored(partition, storeName, currentRestoredOffset, numRestored);
    }

    void restore(Collection<ConsumeResult<byte[], byte[]>> records)
{
        Collection<ConsumeResult<byte[], byte[]>> convertedRecords = new List<>(records.size());
        foreach (ConsumeResult<byte[], byte[]> record in records)
{
            convertedRecords.Add(recordConverter.convert(record));
        }
        compositeRestoreListener.restoreBatch(convertedRecords);
    }

    bool isPersistent()
{
        return persistent;
    }

    void setUserRestoreListener(StateRestoreListener userRestoreListener)
{
        this.compositeRestoreListener.setUserRestoreListener(userRestoreListener);
    }

    void setRestoredOffset(long restoredOffset)
{
        this.restoredOffset = Math.Min(offsetLimit, restoredOffset);
    }

    void setStartingOffset(long startingOffset)
{
        this.startingOffset = Math.Min(offsetLimit, startingOffset);
    }

    void setEndingOffset(long endingOffset)
{
        this.endingOffset = Math.Min(offsetLimit, endingOffset);
    }

    long startingOffset()
{
        return startingOffset;
    }

    bool hasCompleted(long recordOffset, long endOffset)
{
        return endOffset == 0 || recordOffset >= readTo(endOffset);
    }

    long restoredOffset()
{
        return restoredOffset;
    }

    long restoredNumRecords()
{
        return restoredOffset - startingOffset;
    }

    long offsetLimit()
{
        return offsetLimit;
    }

    private long readTo(long endOffset)
{
        return endOffset < offsetLimit ? endOffset : offsetLimit;
    }

}
