using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Errors;
using Kafka.Streams.Errors.Interfaces;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Interfaces;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Processors.Internals
{
    /**
     * RecordQueue is a FIFO queue of {@link StampedRecord} (ConsumerRecord + timestamp). It also keeps track of the
     * partition timestamp defined as the largest timestamp seen on the partition so far; this is passed to the
     * timestamp extractor.
     */
    public abstract class RecordQueue
    {
        public static DateTime UNKNOWN { get; } = DateTime.MinValue; //DateTime.ConsumerRecord.NO_TIMESTAMP;
        protected ILogger<RecordQueue> logger { get; set; }

        /// <summary>
        /// The partition with which this queue is associated.
        /// </summary>
        public TopicPartition partition { get; protected set; }
        protected ITimestampExtractor timestampExtractor;
        protected Queue<ConsumeResult<byte[], byte[]>> fifoQueue;
        protected DateTime partitionTime = RecordQueue.UNKNOWN;
        protected StampedRecord? headRecord = null;
        protected IProcessorContext processorContext { get; set; }

        /**
         * Returns the head record's timestamp
         *
         * @return timestamp
         */
        public DateTime headRecordTimestamp
            => this.headRecord == null ? UNKNOWN : this.headRecord.timestamp;

        /**
         * Add a batch of {@link ConsumerRecord} into the queue
         *
         * @param rawRecords the raw records
         * @return the size of this queue
         */
        public int AddRawRecords(IEnumerable<ConsumeResult<byte[], byte[]>> rawRecords)
        {
            foreach (var rawRecord in rawRecords)
            {
                this.fifoQueue.Enqueue(rawRecord);
            }

            this.UpdateHead();

            return this.Size();
        }

        /**
         * Get the next {@link StampedRecord} from the queue
         *
         * @return StampedRecord
         */
        public StampedRecord Poll()
        {
            StampedRecord recordToReturn = this.headRecord;
            this.headRecord = null;

            this.UpdateHead();

            return recordToReturn;
        }

        /**
         * Returns the number of records in the queue
         *
         * @return the number of records
         */
        public int Size()
        {
            // plus one deserialized head record for timestamp tracking
            return this.fifoQueue.Count + (this.headRecord == null ? 0 : 1);
        }

        /**
         * Tests if the queue is empty
         *
         * @return true if the queue is empty, otherwise false
         */
        public bool IsEmpty()
        {
            return !this.fifoQueue.Any()
                && this.headRecord == null;
        }

        /**
         * Clear the fifo queue of its elements, also clear the time tracker's kept stamped elements
         */
        public void Clear()
        {
            this.fifoQueue.Clear();
            this.headRecord = null;
            this.partitionTime = RecordQueue.UNKNOWN;
        }

        protected abstract RecordDeserializer GetRecordDeserializer();

        private void UpdateHead()
        {
            while (this.headRecord == null && this.fifoQueue.Any())
            {
                ConsumeResult<byte[], byte[]> raw = this.fifoQueue.Peek();
                var recordDeserializer = GetRecordDeserializer();

                ConsumeResult<object, object> deserialized = recordDeserializer.Deserialize<object, object>(processorContext, raw);

                if (deserialized == null)
                {
                    // this only happens if the deserializer decides to skip. It has already logged the reason.
                    continue;
                }

                DateTime? timestamp = null;
                try
                {
                    timestamp = timestampExtractor.Extract(deserialized, partitionTime);
                }
                catch (StreamsException internalFatalExtractorException)
                {
                    throw;
                }
                catch (Exception fatalUserException)
                {
                    //throw new StreamsException(
                    //        string.Format("Fatal user code error in TimestampExtractor callback for record %s.", deserialized),
                    //        fatalUserException);
                }

                // log.LogTrace($"Source node {source.Name} extracted timestamp {timestamp} for record {deserialized}");

                // drop message if TS is invalid, i.e., negative
                if (timestamp.Value.Ticks < 0)
                {
                    //log.LogWarning(
                    //        "Skipping record due to negative extracted timestamp. topic=[{}] partition=[{}] offset=[{}] extractedTimestamp=[{}] extractor=[{}]",
                    //        deserialized.Topic, deserialized.Partition, deserialized.Offset, timestamp, timestampExtractor.GetType().FullName);

                    continue;
                }

                headRecord = new StampedRecord(deserialized, timestamp.Value);

                this.partitionTime = this.partitionTime.GetNewest(timestamp.Value);
            }
        }
    }

    public class RecordQueue<K, V> : RecordQueue
    {
        private readonly ISourceNode source;
        private readonly RecordDeserializer<K, V> recordDeserializer;
        public RecordQueue(
            TopicPartition partition,
            ISourceNode source,
            ITimestampExtractor timestampExtractor,
            IDeserializationExceptionHandler deserializationExceptionHandler,
            IInternalProcessorContext processorContext)
        {
            this.source = source;
            this.partition = partition;
            this.fifoQueue = new Queue<ConsumeResult<byte[], byte[]>>();
            this.timestampExtractor = timestampExtractor;
            this.processorContext = processorContext;

            this.recordDeserializer = new RecordDeserializer<K, V>(
                null,
                source,
                deserializationExceptionHandler);
        }

        protected override RecordDeserializer GetRecordDeserializer()
            => this.recordDeserializer;
    }
}
