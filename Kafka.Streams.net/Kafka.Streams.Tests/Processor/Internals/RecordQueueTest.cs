using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using Kafka.Streams.Processors.Internals;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class RecordQueueTest
    {
        private ISerializer<int> intSerializer = Serializers.Int32;
        private IDeserializer<int> intDeserializer = Deserializers.Int32;
        private ITimestampExtractor timestampExtractor = new MockTimestampExtractor();
        private string[] topics = { "topic" };

        // private Sensor skippedRecordsSensor = new Metrics().sensor("skipped-records");

        InternalMockProcessorContext context = new InternalMockProcessorContext(
            StateSerdes.withBuiltinTypes("anyName", Bytes, Bytes),
            new RecordCollectorImpl(
                null,
                new LogContext("record-queue-test "),
                new DefaultProductionExceptionHandler(),
                skippedRecordsSensor
            )
        );
        private MockSourceNode mockSourceNodeWithMetrics = new MockSourceNode<>(topics, intDeserializer, intDeserializer);
        private RecordQueue queue = new RecordQueue(
            new TopicPartition(topics[0], 1),
            mockSourceNodeWithMetrics,
            timestampExtractor,
            new LogAndFailExceptionHandler(),
            context,
            new LogContext());
        private RecordQueue queueThatSkipsDeserializeErrors = new RecordQueue(
            new TopicPartition(topics[0], 1),
            mockSourceNodeWithMetrics,
            timestampExtractor,
            new LogAndContinueExceptionHandler(),
            context,
            new LogContext());

        private byte[] recordValue = intSerializer.serialize(null, 10);
        private byte[] recordKey = intSerializer.serialize(null, 1);


        public void Before()
        {
            mockSourceNodeWithMetrics.init(context);
        }


        public void After()
        {
            mockSourceNodeWithMetrics.close();
        }

        [Xunit.Fact]
        public void TestTimeTracking()
        {

            Assert.True(queue.isEmpty());
            Assert.Equal(0, queue.size());
            Assert.Equal(RecordQueue.UNKNOWN, queue.headRecordTimestamp);

            // add three 3 out-of-order records with timestamp 2, 1, 3
            List<ConsumeResult<byte[], byte[]>> list1 = Array.asList(
                new ConsumeResult<byte[], byte[]>("topic", 1, 2, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
                new ConsumeResult<byte[], byte[]>("topic", 1, 1, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
                new ConsumeResult<byte[], byte[]>("topic", 1, 3, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue));

            queue.addRawRecords(list1);

            Assert.Equal(3, queue.size());
            Assert.Equal(2L, queue.headRecordTimestamp);

            // poll the first record, now with 1, 3
            Assert.Equal(2L, queue.poll().timestamp);
            Assert.Equal(2, queue.size());
            Assert.Equal(1L, queue.headRecordTimestamp);

            // poll the second record, now with 3
            Assert.Equal(1L, queue.poll().timestamp);
            Assert.Equal(1, queue.size());
            Assert.Equal(3L, queue.headRecordTimestamp);

            // add three 3 out-of-order records with timestamp 4, 1, 2
            // now with 3, 4, 1, 2
            List<ConsumeResult<byte[], byte[]>> list2 = Array.asList(
                new ConsumeResult<byte[], byte[]>("topic", 1, 4, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
                new ConsumeResult<byte[], byte[]>("topic", 1, 1, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
                new ConsumeResult<byte[], byte[]>("topic", 1, 2, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue));

            queue.addRawRecords(list2);

            Assert.Equal(4, queue.size());
            Assert.Equal(3L, queue.headRecordTimestamp);

            // poll the third record, now with 4, 1, 2
            Assert.Equal(3L, queue.poll().timestamp);
            Assert.Equal(3, queue.size());
            Assert.Equal(4L, queue.headRecordTimestamp);

            // poll the rest records
            Assert.Equal(4L, queue.poll().timestamp);
            Assert.Equal(1L, queue.headRecordTimestamp);

            Assert.Equal(1L, queue.poll().timestamp);
            Assert.Equal(2L, queue.headRecordTimestamp);

            Assert.Equal(2L, queue.poll().timestamp);
            Assert.True(queue.isEmpty());
            Assert.Equal(0, queue.size());
            Assert.Equal(RecordQueue.UNKNOWN, queue.headRecordTimestamp);

            // add three more records with 4, 5, 6
            List<ConsumeResult<byte[], byte[]>> list3 = Array.asList(
                new ConsumeResult<byte[], byte[]>("topic", 1, 4, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
                new ConsumeResult<byte[], byte[]>("topic", 1, 5, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
                new ConsumeResult<byte[], byte[]>("topic", 1, 6, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue));

            queue.addRawRecords(list3);

            Assert.Equal(3, queue.size());
            Assert.Equal(4L, queue.headRecordTimestamp);

            // poll one record again, the timestamp should advance now
            Assert.Equal(4L, queue.poll().timestamp);
            Assert.Equal(2, queue.size());
            Assert.Equal(5L, queue.headRecordTimestamp);

            // clear the queue
            queue.clear();
            Assert.True(queue.isEmpty());
            Assert.Equal(0, queue.size());
            Assert.Equal(RecordQueue.UNKNOWN, queue.headRecordTimestamp);

            // re-insert the three records with 4, 5, 6
            queue.addRawRecords(list3);

            Assert.Equal(3, queue.size());
            Assert.Equal(4L, queue.headRecordTimestamp);
        }

        [Xunit.Fact]
        public void ShouldTrackPartitionTimeAsMaxSeenTimestamp()
        {

            Assert.True(queue.isEmpty());
            Assert.Equal(0, queue.size());
            Assert.Equal(RecordQueue.UNKNOWN, queue.headRecordTimestamp);

            // add three 3 out-of-order records with timestamp 2, 1, 3, 4
            List<ConsumeResult<byte[], byte[]>> list1 = Array.asList(
                new ConsumeResult<byte[], byte[]>("topic", 1, 2, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
                new ConsumeResult<byte[], byte[]>("topic", 1, 1, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
                new ConsumeResult<byte[], byte[]>("topic", 1, 3, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
                new ConsumeResult<byte[], byte[]>("topic", 1, 4, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue));

            Assert.Equal(queue.partitionTime(), RecordQueue.UNKNOWN);

            queue.addRawRecords(list1);

            Assert.Equal(queue.partitionTime(), 2L);

            queue.poll();
            Assert.Equal(queue.partitionTime(), 2L);

            queue.poll();
            Assert.Equal(queue.partitionTime(), 3L);
        }

        [Xunit.Fact]// (expected = StreamsException)
        public void ShouldThrowStreamsExceptionWhenKeyDeserializationFails()
        {
            byte[] key = Serdes.Long().Serializer.serialize("foo", 1L);
            List<ConsumeResult<byte[], byte[]>> records = Collections.singletonList(
                new ConsumeResult<byte[], byte[]>("topic", 1, 1, 0L, TimestampType.CreateTime, 0L, 0, 0, key, recordValue));

            queue.addRawRecords(records);
        }

        [Xunit.Fact]// (expected = StreamsException)
        public void ShouldThrowStreamsExceptionWhenValueDeserializationFails()
        {
            byte[] value = Serdes.Long().Serializer.serialize("foo", 1L);
            List<ConsumeResult<byte[], byte[]>> records = Collections.singletonList(
                new ConsumeResult<byte[], byte[]>("topic", 1, 1, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, value));

            queue.addRawRecords(records);
        }

        [Xunit.Fact]
        public void ShouldNotThrowStreamsExceptionWhenKeyDeserializationFailsWithSkipHandler()
        {
            byte[] key = Serdes.Long().Serializer.serialize("foo", 1L);
            List<ConsumeResult<byte[], byte[]>> records = Collections.singletonList(
                new ConsumeResult<byte[], byte[]>("topic", 1, 1, 0L, TimestampType.CreateTime, 0L, 0, 0, key, recordValue));

            queueThatSkipsDeserializeErrors.addRawRecords(records);
            Assert.Equal(0, queueThatSkipsDeserializeErrors.Count);
        }

        [Xunit.Fact]
        public void ShouldNotThrowStreamsExceptionWhenValueDeserializationFailsWithSkipHandler()
        {
            byte[] value = Serdes.Long().Serializer.serialize("foo", 1L);
            List<ConsumeResult<byte[], byte[]>> records = Collections.singletonList(
                new ConsumeResult<byte[], byte[]>("topic", 1, 1, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, value));

            queueThatSkipsDeserializeErrors.addRawRecords(records);
            Assert.Equal(0, queueThatSkipsDeserializeErrors.Count);
        }


        [Xunit.Fact]// (expected = StreamsException)
        public void ShouldThrowOnNegativeTimestamp()
        {
            List<ConsumeResult<byte[], byte[]>> records = Collections.singletonList(
                new ConsumeResult<byte[], byte[]>("topic", 1, 1, -1L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue));

            RecordQueue queue = new RecordQueue(
                new TopicPartition(topics[0], 1),
                new MockSourceNode<>(topics, intDeserializer, intDeserializer),
                new FailOnInvalidTimestamp(),
                new LogAndContinueExceptionHandler(),
                new InternalMockProcessorContext(),
                new LogContext());

            queue.addRawRecords(records);
        }

        [Xunit.Fact]
        public void ShouldDropOnNegativeTimestamp()
        {
            List<ConsumeResult<byte[], byte[]>> records = Collections.singletonList(
                new ConsumeResult<byte[], byte[]>("topic", 1, 1, -1L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue));

            RecordQueue queue = new RecordQueue(
                new TopicPartition(topics[0], 1),
                new MockSourceNode<>(topics, intDeserializer, intDeserializer),
                new LogAndSkipOnInvalidTimestamp(),
                new LogAndContinueExceptionHandler(),
                new InternalMockProcessorContext(),
                new LogContext());
            queue.addRawRecords(records);

            Assert.Equal(0, queue.size());
        }

        [Xunit.Fact]
        public void ShouldPassPartitionTimeToTimestampExtractor()
        {

            PartitionTimeTrackingTimestampExtractor timestampExtractor = new PartitionTimeTrackingTimestampExtractor();
            RecordQueue queue = new RecordQueue(
                new TopicPartition(topics[0], 1),
                mockSourceNodeWithMetrics,
                timestampExtractor,
                new LogAndFailExceptionHandler(),
                context,
                new LogContext());

            Assert.True(queue.isEmpty());
            Assert.Equal(0, queue.size());
            Assert.Equal(RecordQueue.UNKNOWN, queue.headRecordTimestamp);

            // add three 3 out-of-order records with timestamp 2, 1, 3, 4
            List<ConsumeResult<byte[], byte[]>> list1 = Array.asList(
                new ConsumeResult<byte[], byte[]>("topic", 1, 2, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
                new ConsumeResult<byte[], byte[]>("topic", 1, 1, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
                new ConsumeResult<byte[], byte[]>("topic", 1, 3, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
                new ConsumeResult<byte[], byte[]>("topic", 1, 4, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue));

            Assert.Equal(RecordQueue.UNKNOWN, timestampExtractor.partitionTime);

            queue.addRawRecords(list1);

            // no (known) timestamp has yet been passed to the timestamp extractor
            Assert.Equal(RecordQueue.UNKNOWN, timestampExtractor.partitionTime);

            queue.poll();
            Assert.Equal(2L, timestampExtractor.partitionTime);

            queue.poll();
            Assert.Equal(2L, timestampExtractor.partitionTime);

            queue.poll();
            Assert.Equal(3L, timestampExtractor.partitionTime);

        }

        class PartitionTimeTrackingTimestampExtractor : ITimestampExtractor
        {
            private long partitionTime = RecordQueue.UNKNOWN;

            public long Extract(ConsumeResult<object, object> record, long partitionTime)
            {
                if (partitionTime < this.partitionTime)
                {
                    throw new IllegalStateException("Partition time should not decrease");
                }
                this.partitionTime = partitionTime;
                return record.Offset;
            }
        }
    }
}
