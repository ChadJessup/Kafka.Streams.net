//using Confluent.Kafka;
//using Kafka.Streams.Errors;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream;
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Internals;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    public class RecordQueueTest
//    {
//        private readonly ISerializer<int> intSerializer = Serializers.Int32;
//        private readonly IDeserializer<int> intDeserializer = Deserializers.Int32;
//        private readonly ITimestampExtractor timestampExtractor = new MockTimestampExtractor();
//        private readonly string[] topics = { "topic" };

//        // private Sensor skippedRecordsSensor = new Metrics().sensor("skipped-records");

//        InternalMockProcessorContext context = new InternalMockProcessorContext(
//            StateSerdes.WithBuiltinTypes("anyName", Bytes, Bytes),
//            new RecordCollector(
//                null,
//                new LogContext("record-queue-test "),
//                new DefaultProductionExceptionHandler(),
//                skippedRecordsSensor
//            )
//        );
//        private MockSourceNode mockSourceNodeWithMetrics = new MockSourceNode<>(topics, intDeserializer, intDeserializer);
//        private readonly RecordQueue queue = new RecordQueue(
//            new TopicPartition(topics[0], 1),
//            mockSourceNodeWithMetrics,
//            timestampExtractor,
//            new LogAndFailExceptionHandler(),
//            context,
//            new LogContext());
//        private readonly RecordQueue queueThatSkipsDeserializeErrors = new RecordQueue(
//            new TopicPartition(topics[0], 1),
//            mockSourceNodeWithMetrics,
//            timestampExtractor,
//            new LogAndContinueExceptionHandler(),
//            context,
//            new LogContext());

//        private readonly byte[] recordValue = intSerializer.Serialize(null, 10);
//        private readonly byte[] recordKey = intSerializer.Serialize(null, 1);


//        public void Before()
//        {
//            mockSourceNodeWithMetrics.Init(context);
//        }


//        public void After()
//        {
//            mockSourceNodeWithMetrics.Close();
//        }

//        [Fact]
//        public void TestTimeTracking()
//        {

//            Assert.True(queue.IsEmpty());
//            Assert.Equal(0, queue.Size());
//            Assert.Equal(RecordQueue.UNKNOWN, queue.headRecordTimestamp);

//            // add three 3 out-of-order records with timestamp 2, 1, 3
//            List<ConsumeResult<byte[], byte[]>> list1 = Arrays.asList(
//                new ConsumeResult<byte[], byte[]>("topic", 1, 2, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
//                new ConsumeResult<byte[], byte[]>("topic", 1, 1, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
//                new ConsumeResult<byte[], byte[]>("topic", 1, 3, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue));

//            queue.AddRawRecords(list1);

//            Assert.Equal(3, queue.Size());
//            Assert.Equal(2L, queue.headRecordTimestamp);

//            // poll the first record, now with 1, 3
//            Assert.Equal(2L, queue.Poll().timestamp);
//            Assert.Equal(2, queue.Size());
//            Assert.Equal(1L, queue.headRecordTimestamp);

//            // poll the second record, now with 3
//            Assert.Equal(1L, queue.Poll().timestamp);
//            Assert.Equal(1, queue.Size());
//            Assert.Equal(3L, queue.headRecordTimestamp);

//            // add three 3 out-of-order records with timestamp 4, 1, 2
//            // now with 3, 4, 1, 2
//            List<ConsumeResult<byte[], byte[]>> list2 = Arrays.asList(
//                new ConsumeResult<byte[], byte[]>("topic", 1, 4, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
//                new ConsumeResult<byte[], byte[]>("topic", 1, 1, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
//                new ConsumeResult<byte[], byte[]>("topic", 1, 2, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue));

//            queue.AddRawRecords(list2);

//            Assert.Equal(4, queue.Size());
//            Assert.Equal(3L, queue.headRecordTimestamp);

//            // poll the third record, now with 4, 1, 2
//            Assert.Equal(3L, queue.Poll().timestamp);
//            Assert.Equal(3, queue.Size());
//            Assert.Equal(4L, queue.headRecordTimestamp);

//            // poll the rest records
//            Assert.Equal(4L, queue.Poll().timestamp);
//            Assert.Equal(1L, queue.headRecordTimestamp);

//            Assert.Equal(1L, queue.Poll().timestamp);
//            Assert.Equal(2L, queue.headRecordTimestamp);

//            Assert.Equal(2L, queue.Poll().timestamp);
//            Assert.True(queue.IsEmpty());
//            Assert.Equal(0, queue.Size());
//            Assert.Equal(RecordQueue.UNKNOWN, queue.headRecordTimestamp);

//            // add three more records with 4, 5, 6
//            List<ConsumeResult<byte[], byte[]>> list3 = Arrays.asList(
//                new ConsumeResult<byte[], byte[]>("topic", 1, 4, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
//                new ConsumeResult<byte[], byte[]>("topic", 1, 5, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
//                new ConsumeResult<byte[], byte[]>("topic", 1, 6, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue));

//            queue.AddRawRecords(list3);

//            Assert.Equal(3, queue.Size());
//            Assert.Equal(4L, queue.headRecordTimestamp);

//            // poll one record again, the timestamp should advance now
//            Assert.Equal(4L, queue.Poll().timestamp);
//            Assert.Equal(2, queue.Size());
//            Assert.Equal(5L, queue.headRecordTimestamp);

//            // clear the queue
//            queue.Clear();
//            Assert.True(queue.IsEmpty());
//            Assert.Equal(0, queue.Size());
//            Assert.Equal(RecordQueue.UNKNOWN, queue.headRecordTimestamp);

//            // re-insert the three records with 4, 5, 6
//            queue.AddRawRecords(list3);

//            Assert.Equal(3, queue.Size());
//            Assert.Equal(4L, queue.headRecordTimestamp);
//        }

//        [Fact]
//        public void ShouldTrackPartitionTimeAsMaxSeenTimestamp()
//        {

//            Assert.True(queue.IsEmpty());
//            Assert.Equal(0, queue.Size());
//            Assert.Equal(RecordQueue.UNKNOWN, queue.headRecordTimestamp);

//            // add three 3 out-of-order records with timestamp 2, 1, 3, 4
//            List<ConsumeResult<byte[], byte[]>> list1 = Arrays.asList(
//                new ConsumeResult<byte[], byte[]>("topic", 1, 2, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
//                new ConsumeResult<byte[], byte[]>("topic", 1, 1, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
//                new ConsumeResult<byte[], byte[]>("topic", 1, 3, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
//                new ConsumeResult<byte[], byte[]>("topic", 1, 4, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue));

//            Assert.Equal(queue.partitionTime(), RecordQueue.UNKNOWN);

//            queue.AddRawRecords(list1);

//            Assert.Equal(queue.partitionTime(), 2L);

//            queue.Poll();
//            Assert.Equal(queue.partitionTime(), 2L);

//            queue.Poll();
//            Assert.Equal(queue.partitionTime(), 3L);
//        }

//        [Fact]// (expected = StreamsException)
//        public void ShouldThrowStreamsExceptionWhenKeyDeserializationFails()
//        {
//            byte[] key = Serdes.Long().Serializer.Serialize("foo", 1L);
//            List<ConsumeResult<byte[], byte[]>> records = Collections.singletonList(
//                new ConsumeResult<byte[], byte[]>("topic", 1, 1, 0L, TimestampType.CreateTime, 0L, 0, 0, key, recordValue));

//            queue.AddRawRecords(records);
//        }

//        [Fact]// (expected = StreamsException)
//        public void ShouldThrowStreamsExceptionWhenValueDeserializationFails()
//        {
//            byte[] value = Serdes.Long().Serializer.Serialize("foo", 1L);
//            List<ConsumeResult<byte[], byte[]>> records = Collections.singletonList(
//                new ConsumeResult<byte[], byte[]>("topic", 1, 1, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, value));

//            queue.AddRawRecords(records);
//        }

//        [Fact]
//        public void ShouldNotThrowStreamsExceptionWhenKeyDeserializationFailsWithSkipHandler()
//        {
//            byte[] key = Serdes.Long().Serializer.Serialize(1L, new SerializationContext(MessageComponentType.Key, "foo"));
//            List<ConsumeResult<byte[], byte[]>> records = Collections.singletonList(
//                new ConsumeResult<byte[], byte[]>("topic", 1, 1, 0L, TimestampType.CreateTime, 0L, 0, 0, key, recordValue));

//            queueThatSkipsDeserializeErrors.AddRawRecords(records);
//            Assert.Equal(0, queueThatSkipsDeserializeErrors.Count);
//        }

//        [Fact]
//        public void ShouldNotThrowStreamsExceptionWhenValueDeserializationFailsWithSkipHandler()
//        {
//            byte[] value = Serdes.Long().Serializer.Serialize(1L, new SerializationContext(MessageComponentType.Value, "foo"));
//            List<ConsumeResult<byte[], byte[]>> records = Collections.singletonList(
//                new ConsumeResult<byte[], byte[]>("topic", 1, 1, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, value));

//            queueThatSkipsDeserializeErrors.AddRawRecords(records);
//            Assert.Equal(0, queueThatSkipsDeserializeErrors.Count);
//        }


//        [Fact]// (expected = StreamsException)
//        public void ShouldThrowOnNegativeTimestamp()
//        {
//            List<ConsumeResult<byte[], byte[]>> records = Collections.singletonList(
//                new ConsumeResult<byte[], byte[]>("topic", 1, 1, -1L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue));

//            RecordQueue queue = new RecordQueue(
//                new TopicPartition(topics[0], 1),
//                new MockSourceNode<>(topics, intDeserializer, intDeserializer),
//                new FailOnInvalidTimestamp(),
//                new LogAndContinueExceptionHandler(),
//                new InternalMockProcessorContext(),
//                new LogContext());

//            queue.AddRawRecords(records);
//        }

//        [Fact]
//        public void ShouldDropOnNegativeTimestamp()
//        {
//            List<ConsumeResult<byte[], byte[]>> records = Collections.singletonList(
//                new ConsumeResult<byte[], byte[]>("topic", 1, 1, -1L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue));

//            RecordQueue queue = new RecordQueue(
//                new TopicPartition(topics[0], 1),
//                new MockSourceNode<>(topics, intDeserializer, intDeserializer),
//                new LogAndSkipOnInvalidTimestamp(),
//                new LogAndContinueExceptionHandler(),
//                new InternalMockProcessorContext(),
//                new LogContext());
//            queue.AddRawRecords(records);

//            Assert.Equal(0, queue.Size());
//        }

//        [Fact]
//        public void ShouldPassPartitionTimeToTimestampExtractor()
//        {

//            PartitionTimeTrackingTimestampExtractor timestampExtractor = new PartitionTimeTrackingTimestampExtractor();
//            RecordQueue queue = new RecordQueue(
//                new TopicPartition(topics[0], 1),
//                mockSourceNodeWithMetrics,
//                timestampExtractor,
//                new LogAndFailExceptionHandler(),
//                context,
//                new LogContext());

//            Assert.True(queue.IsEmpty());
//            Assert.Equal(0, queue.Size());
//            Assert.Equal(RecordQueue.UNKNOWN, queue.headRecordTimestamp);

//            // add three 3 out-of-order records with timestamp 2, 1, 3, 4
//            List<ConsumeResult<byte[], byte[]>> list1 = Arrays.asList(
//                new ConsumeResult<byte[], byte[]>("topic", 1, 2, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
//                new ConsumeResult<byte[], byte[]>("topic", 1, 1, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
//                new ConsumeResult<byte[], byte[]>("topic", 1, 3, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue),
//                new ConsumeResult<byte[], byte[]>("topic", 1, 4, 0L, TimestampType.CreateTime, 0L, 0, 0, recordKey, recordValue));

//            Assert.Equal(RecordQueue.UNKNOWN, timestampExtractor.partitionTime);

//            queue.AddRawRecords(list1);

//            // no (known) timestamp has yet been passed to the timestamp extractor
//            Assert.Equal(RecordQueue.UNKNOWN, timestampExtractor.partitionTime);

//            queue.Poll();
//            Assert.Equal(2L, timestampExtractor.partitionTime);

//            queue.Poll();
//            Assert.Equal(2L, timestampExtractor.partitionTime);

//            queue.Poll();
//            //Assert.Equal(3L, timestampExtractor.partitionTime);
//        }

//        class PartitionTimeTrackingTimestampExtractor : ITimestampExtractor
//        {
//            private long partitionTime = RecordQueue.UNKNOWN;

//            public long Extract<K, V>(ConsumeResult<K, V> record, long partitionTime)
//            {
//                if (partitionTime < this.partitionTime)
//                {
//                    throw new InvalidOperationException("Partition time should not decrease");
//                }

//                this.partitionTime = partitionTime;
//                return record.Offset;
//            }
//        }
//    }
//}
