using Confluent.Kafka;
using Kafka.Common;
using Kafka.Common.Metrics;
using Kafka.Streams.Errors;
using Kafka.Streams.Errors.Interfaces;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processor.Interfaces;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Processor.Internals
{
    /**
     * RecordQueue is a FIFO queue of {@link StampedRecord} (ConsumerRecord + timestamp). It also keeps track of the
     * partition timestamp defined as the largest timestamp seen on the partition so far; this is passed to the
     * timestamp extractor.
     */
    public abstract class RecordQueue
    {
        public static long UNKNOWN = ConsumerRecord.NO_TIMESTAMP;
        protected ILogger log { get; set; }

        /// <summary>
        /// The partition with which this queue is associated.
        /// </summary>
        public TopicPartition partition { get; protected set; }
        protected ITimestampExtractor timestampExtractor;
        protected Queue<ConsumeResult<byte[], byte[]>> fifoQueue;
        protected long partitionTime = RecordQueue.UNKNOWN;
        protected Sensor skipRecordsSensor;
        protected StampedRecord? headRecord = null;
        protected IProcessorContext processorContext { get; set; }

        /**
         * Returns the head record's timestamp
         *
         * @return timestamp
         */
        public long headRecordTimestamp
            => headRecord == null ? UNKNOWN : headRecord.timestamp;

        /**
         * Add a batch of {@link ConsumerRecord} into the queue
         *
         * @param rawRecords the raw records
         * @return the size of this queue
         */
        public int addRawRecords(IEnumerable<ConsumeResult<byte[], byte[]>> rawRecords)
        {
            foreach (var rawRecord in rawRecords)
            {
                fifoQueue.Enqueue(rawRecord);
            }

            updateHead();

            return size();
        }

        /**
         * Get the next {@link StampedRecord} from the queue
         *
         * @return StampedRecord
         */
        public StampedRecord poll()
        {
            StampedRecord recordToReturn = headRecord;
            headRecord = null;

            updateHead();

            return recordToReturn;
        }

        /**
         * Returns the number of records in the queue
         *
         * @return the number of records
         */
        public int size()
        {
            // plus one deserialized head record for timestamp tracking
            return fifoQueue.Count + (headRecord == null ? 0 : 1);
        }

        /**
         * Tests if the queue is empty
         *
         * @return true if the queue is empty, otherwise false
         */
        public bool isEmpty()
        {
            return !fifoQueue.Any() && headRecord == null;
        }


        /**
         * Clear the fifo queue of its elements, also clear the time tracker's kept stamped elements
         */
        public void clear()
        {
            fifoQueue.Clear();
            headRecord = null;
            partitionTime = RecordQueue.UNKNOWN;
        }

        // protected abstract RecordDeserializer<K, V> GetRecordDeserializer<K, V>();
        private void updateHead()
        {
            while (headRecord == null && fifoQueue.Any())
            {
                ConsumeResult<byte[], byte[]> raw = fifoQueue.Peek();
                //var recordDeserializer = GetRecordDeserializer<K, V>();

                //ConsumeResult<K, V> deserialized = recordDeserializer.deserialize(processorContext, raw);

                if (true)//deserialized == null)
                {
                    // this only happens if the deserializer decides to skip. It has already logged the reason.
                    continue;
                }

                long timestamp;
                try
                {
                    //timestamp = timestampExtractor.Extract(deserialized, partitionTime);
                }
                catch (StreamsException internalFatalExtractorException)
                {
                    throw internalFatalExtractorException;
                }
                catch (Exception fatalUserException)
                {
                    //throw new StreamsException(
                    //        string.Format("Fatal user code error in TimestampExtractor callback for record %s.", deserialized),
                    //        fatalUserException);
                }

                // log.LogTrace($"Source node {source.name} extracted timestamp {timestamp} for record {deserialized}");

                // drop message if TS is invalid, i.e., negative
                if (timestamp < 0)
                {
                    //log.LogWarning(
                    //        "Skipping record due to negative extracted timestamp. topic=[{}] partition=[{}] offset=[{}] extractedTimestamp=[{}] extractor=[{}]",
                    //        deserialized.Topic, deserialized.Partition, deserialized.Offset, timestamp, timestampExtractor.GetType().FullName);

                    skipRecordsSensor.record();
                    continue;
                }

                //headRecord = new StampedRecord(deserialized, timestamp);

                partitionTime = Math.Max(partitionTime, timestamp);
            }
        }
    }

    public class RecordQueue<K, V> : RecordQueue
    {
        private readonly SourceNode<K, V> source;
        private readonly RecordDeserializer<K, V> recordDeserializer;
        public RecordQueue(
            TopicPartition partition,
            SourceNode<K, V> source,
            ITimestampExtractor timestampExtractor,
            IDeserializationExceptionHandler deserializationExceptionHandler,
            IInternalProcessorContext processorContext,
            LogContext logContext)
        {
            this.source = source;
            this.partition = partition;
            this.fifoQueue = new Queue<ConsumeResult<byte[], byte[]>>();
            this.timestampExtractor = timestampExtractor;
            this.processorContext = processorContext;
            //skipRecordsSensor = ThreadMetrics.skipRecordSensor(processorContext.metrics());
            recordDeserializer = new RecordDeserializer<K, V>(
                source,
                deserializationExceptionHandler,
                logContext,
                skipRecordsSensor);

            this.log = logContext.logger(typeof(RecordQueue<K, V>));
        }

        //protected override RecordDeserializer<K, V> GetRecordDeserializer<K, V>()
        //    => (RecordDeserializer<K, V>)this.recordDeserializer;
    }
}
