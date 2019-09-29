using Confluent.Kafka;
using Kafka.Common.Metrics;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using Priority_Queue;

namespace Kafka.Streams.Processors.Internals
{
    /**
     * PartitionGroup is used to buffer all co-partitioned records for processing.
     *
     * In other words, it represents the "same" partition over multiple co-partitioned topics, and it is used
     * to buffer records from that partition in each of the contained topic-partitions.
     * Each StreamTask has exactly one PartitionGroup.
     *
     * PartitionGroup : the algorithm that determines in what order buffered records are selected for processing.
     *
     * Specifically, when polled, it returns the record from the topic-partition with the lowest stream-time.
     * Stream-time for a topic-partition is defined as the highest timestamp
     * yet observed at the head of that topic-partition.
     *
     * PartitionGroup also maintains a stream-time for the group as a whole.
     * This is defined as the highest timestamp of any record yet polled from the PartitionGroup.
     * Note however that any computation that depends on stream-time should track it on a per-operator basis to obtain an
     * accurate view of the local time as seen by that processor.
     *
     * The PartitionGroups's stream-time is initially UNKNOWN (-1), and it set to a known value upon first poll.
     * As a consequence of the definition, the PartitionGroup's stream-time is non-decreasing
     * (i.e., it increases or stays the same over time).
     */
    public class PartitionGroup
    {
        private readonly Dictionary<TopicPartition, RecordQueue> partitionQueues;
        private readonly Sensor recordLatenessSensor;
        private readonly SimplePriorityQueue<RecordQueue> nonEmptyQueuesByTime;

        public long streamTime { get; set; }
        private int totalBuffered;
        private bool allBuffered;
        private readonly ProcessorContextImpl<object, object> processorContextImpl;

        PartitionGroup(Dictionary<TopicPartition, RecordQueue> partitionQueues, Sensor recordLatenessSensor)
        {
            nonEmptyQueuesByTime = new SimplePriorityQueue<RecordQueue>();

            this.partitionQueues = partitionQueues;
            this.recordLatenessSensor = recordLatenessSensor;
            totalBuffered = 0;
            allBuffered = false;
            streamTime = RecordQueue.UNKNOWN;
        }

        public PartitionGroup(Dictionary<TopicPartition, RecordQueue> partitionQueues, ProcessorContextImpl<object, object> processorContextImpl)
        {
            this.partitionQueues = partitionQueues;
            this.processorContextImpl = processorContextImpl;
        }

        /**
         * Get the next record and queue
         *
         * @return StampedRecord
         */
        public StampedRecord nextRecord<K, V>(RecordInfo LogInformation)
        {
            StampedRecord record = null;

            if (nonEmptyQueuesByTime.TryDequeue(out var queue))
            {
            }

            LogInformation.queue = queue;

            if (queue != null)
            {
                // get the first record from this queue.
                record = null;// queue.Peek();

                if (record != null)
                {
                    --totalBuffered;

                    if (queue.isEmpty())
                    {
                        // if a certain queue has been drained, reset the flag
                        allBuffered = false;
                    }
                    else
                    {

                        nonEmptyQueuesByTime.Enqueue(queue, queue.headRecordTimestamp);
                    }

                    // always update the stream-time to the record's timestamp yet to be processed if it is larger
                    if (record.timestamp > streamTime)
                    {
                        streamTime = record.timestamp;
                        recordLatenessSensor.record(0);
                    }
                    else
                    {

                        recordLatenessSensor.record(streamTime - record.timestamp);
                    }
                }
            }

            return record;
        }

        /**
         * Adds raw records to this partition group
         *
         * @param partition the partition
         * @param rawRecords  the raw records
         * @return the queue size for the partition
         */
        public int addRawRecords(TopicPartition partition, IEnumerable<ConsumeResult<byte[], byte[]>> rawRecords)
        {
            RecordQueue recordQueue = partitionQueues[partition];

            int oldSize = recordQueue.size();
            int newSize = recordQueue.addRawRecords(rawRecords);

            // add this record queue to be considered for processing in the future if it was empty before
            if (oldSize == 0 && newSize > 0)
            {
                nonEmptyQueuesByTime.Enqueue(recordQueue, recordQueue.headRecordTimestamp);

                // if all partitions now are non-empty, set the flag
                // we do not need to update the stream-time here since this task will definitely be
                // processed next, and hence the stream-time will be updated when we retrieved records by then
                if (nonEmptyQueuesByTime.Count == this.partitionQueues.Count)
                {
                    allBuffered = true;
                }
            }

            totalBuffered += newSize - oldSize;

            return newSize;
        }

        public HashSet<TopicPartition> partitions()
        {
            return new HashSet<TopicPartition>(partitionQueues.Keys);
        }

        /**
         * Return the stream-time of this partition group defined as the largest timestamp seen across all partitions
         */

        /**
         * @throws InvalidOperationException if the record's partition does not belong to this partition group
         */
        public int numBuffered(TopicPartition partition)
        {
            RecordQueue recordQueue = partitionQueues[partition];

            if (recordQueue == null)
            {
                throw new InvalidOperationException(string.Format("Record's partition %s does not belong to this partition-group.", partition));
            }

            return recordQueue.size();
        }

        public int numBuffered()
        {
            return totalBuffered;
        }

        public bool allPartitionsBuffered() => allBuffered;

        public void close()
        {
            clear();
            partitionQueues.Clear();
        }

        public void clear()
        {
            nonEmptyQueuesByTime.Clear();
            streamTime = RecordQueue.UNKNOWN;
            foreach (RecordQueue queue in partitionQueues.Values)
            {
                queue.clear();
            }
        }
    }
}