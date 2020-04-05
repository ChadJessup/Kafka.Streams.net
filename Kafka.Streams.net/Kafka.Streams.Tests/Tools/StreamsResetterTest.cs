//using Confluent.Kafka;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Tools
//{
//    public class StreamsResetterTest
//    {
//        private const string TOPIC = "topic1";
//        private StreamsResetter streamsResetter = new StreamsResetter();
//        private readonly MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
//        private readonly TopicPartition topicPartition = new TopicPartition(TOPIC, 0);
//        private HashSet<TopicPartition> inputTopicPartitions = new HashSet<>(Collections.singletonList(topicPartition));

//        public void SetUp()
//        {
//            consumer.assign(Collections.singletonList(topicPartition));

//            consumer.AddRecord(new ConsumeResult<>(TOPIC, 0, 0L, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
//            consumer.AddRecord(new ConsumeResult<>(TOPIC, 0, 1L, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
//            consumer.AddRecord(new ConsumeResult<>(TOPIC, 0, 2L, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
//            consumer.AddRecord(new ConsumeResult<>(TOPIC, 0, 3L, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
//            consumer.AddRecord(new ConsumeResult<>(TOPIC, 0, 4L, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
//        }

//        [Fact]
//        public void TestResetToSpecificOffsetWhenBetweenBeginningAndEndOffset()
//        {
//            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
//            endOffsets.put(topicPartition, 4L);
//            consumer.updateEndOffsets(endOffsets);

//            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
//            beginningOffsets.put(topicPartition, 0L);
//            consumer.updateBeginningOffsets(beginningOffsets);

//            streamsResetter.resetOffsetsTo(consumer, inputTopicPartitions, 2L);

//            ConsumeResult<byte[], byte[]> records = consumer.poll(Duration.FromMilliseconds(500));
//            Assert.Equal(3, records.count());
//        }

//        [Fact]
//        public void TestResetToSpecificOffsetWhenBeforeBeginningOffset()
//        {
//            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
//            endOffsets.put(topicPartition, 4L);
//            consumer.updateEndOffsets(endOffsets);

//            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
//            beginningOffsets.put(topicPartition, 3L);
//            consumer.updateBeginningOffsets(beginningOffsets);

//            streamsResetter.resetOffsetsTo(consumer, inputTopicPartitions, 2L);

//            ConsumeResult<byte[], byte[]> records = consumer.poll(Duration.FromMilliseconds(500));
//            Assert.Equal(2, records.count());
//        }

//        [Fact]
//        public void TestResetToSpecificOffsetWhenAfterEndOffset()
//        {
//            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
//            endOffsets.put(topicPartition, 3L);
//            consumer.updateEndOffsets(endOffsets);

//            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
//            beginningOffsets.put(topicPartition, 0L);
//            consumer.updateBeginningOffsets(beginningOffsets);

//            streamsResetter.resetOffsetsTo(consumer, inputTopicPartitions, 4L);

//            ConsumeResult<byte[], byte[]> records = consumer.poll(Duration.FromMilliseconds(500));
//            Assert.Equal(2, records.count());
//        }

//        [Fact]
//        public void TestShiftOffsetByWhenBetweenBeginningAndEndOffset()
//        {
//            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
//            endOffsets.put(topicPartition, 4L);
//            consumer.updateEndOffsets(endOffsets);

//            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
//            beginningOffsets.put(topicPartition, 0L);
//            consumer.updateBeginningOffsets(beginningOffsets);

//            streamsResetter.shiftOffsetsBy(consumer, inputTopicPartitions, 3L);

//            ConsumeResult<byte[], byte[]> records = consumer.poll(Duration.FromMilliseconds(500));
//            Assert.Equal(2, records.count());
//        }

//        [Fact]
//        public void TestShiftOffsetByWhenBeforeBeginningOffset()
//        {
//            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
//            endOffsets.put(topicPartition, 4L);
//            consumer.updateEndOffsets(endOffsets);

//            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
//            beginningOffsets.put(topicPartition, 0L);
//            consumer.updateBeginningOffsets(beginningOffsets);

//            streamsResetter.shiftOffsetsBy(consumer, inputTopicPartitions, -3L);

//            ConsumeResult<byte[], byte[]> records = consumer.poll(Duration.FromMilliseconds(500));
//            Assert.Equal(5, records.count());
//        }

//        [Fact]
//        public void TestShiftOffsetByWhenAfterEndOffset()
//        {
//            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
//            endOffsets.put(topicPartition, 3L);
//            consumer.updateEndOffsets(endOffsets);

//            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
//            beginningOffsets.put(topicPartition, 0L);
//            consumer.updateBeginningOffsets(beginningOffsets);

//            streamsResetter.shiftOffsetsBy(consumer, inputTopicPartitions, 5L);

//            ConsumeResult<byte[], byte[]> records = consumer.poll(Duration.FromMilliseconds(500));
//            Assert.Equal(2, records.count());
//        }

//        [Fact]
//        public void TestResetUsingPlanWhenBetweenBeginningAndEndOffset()
//        {
//            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
//            endOffsets.put(topicPartition, 4L);
//            consumer.updateEndOffsets(endOffsets);

//            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
//            beginningOffsets.put(topicPartition, 0L);
//            consumer.updateBeginningOffsets(beginningOffsets);

//            Dictionary<TopicPartition, long> topicPartitionsAndOffset = new HashMap<>();
//            topicPartitionsAndOffset.put(topicPartition, 3L);
//            streamsResetter.resetOffsetsFromResetPlan(consumer, inputTopicPartitions, topicPartitionsAndOffset);

//            ConsumeResult<byte[], byte[]> records = consumer.poll(Duration.FromMilliseconds(500));
//            Assert.Equal(2, records.count());
//        }

//        [Fact]
//        public void TestResetUsingPlanWhenBeforeBeginningOffset()
//        {
//            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
//            endOffsets.put(topicPartition, 4L);
//            consumer.updateEndOffsets(endOffsets);

//            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
//            beginningOffsets.put(topicPartition, 3L);
//            consumer.updateBeginningOffsets(beginningOffsets);

//            Dictionary<TopicPartition, long> topicPartitionsAndOffset = new HashMap<>();
//            topicPartitionsAndOffset.put(topicPartition, 1L);
//            streamsResetter.resetOffsetsFromResetPlan(consumer, inputTopicPartitions, topicPartitionsAndOffset);

//            ConsumeResult<byte[], byte[]> records = consumer.poll(Duration.FromMilliseconds(500));
//            Assert.Equal(2, records.count());
//        }

//        [Fact]
//        public void TestResetUsingPlanWhenAfterEndOffset()
//        {
//            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
//            endOffsets.put(topicPartition, 3L);
//            consumer.updateEndOffsets(endOffsets);

//            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
//            beginningOffsets.put(topicPartition, 0L);
//            consumer.updateBeginningOffsets(beginningOffsets);

//            Dictionary<TopicPartition, long> topicPartitionsAndOffset = new HashMap<>();
//            topicPartitionsAndOffset.put(topicPartition, 5L);
//            streamsResetter.resetOffsetsFromResetPlan(consumer, inputTopicPartitions, topicPartitionsAndOffset);

//            ConsumeResult<byte[], byte[]> records = consumer.poll(Duration.FromMilliseconds(500));
//            Assert.Equal(2, records.count());
//        }

//        [Fact]
//        public void ShouldSeekToEndOffset()
//        {
//            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
//            endOffsets.put(topicPartition, 3L);
//            consumer.updateEndOffsets(endOffsets);

//            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
//            beginningOffsets.put(topicPartition, 0L);
//            consumer.updateBeginningOffsets(beginningOffsets);

//            HashSet<TopicPartition> intermediateTopicPartitions = new HashSet<>();
//            intermediateTopicPartitions.Add(topicPartition);
//            streamsResetter.maybeSeekToEnd("g1", consumer, intermediateTopicPartitions);

//            ConsumeResult<byte[], byte[]> records = consumer.poll(Duration.FromMilliseconds(500));
//            Assert.Equal(2, records.count());
//        }

//        [Fact]//  throws InterruptedException, ExecutionException
//        public void ShouldDeleteTopic()
//        {
//            Cluster cluster = CreateCluster(1);
//            MockAdminClient adminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0));
//            TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, cluster.nodeById(0), cluster.nodes(), Collections.< Node > emptyList());
//            adminClient.addTopic(false, TOPIC, Collections.singletonList(topicPartitionInfo), null);
//            streamsResetter.doDelete(Collections.singletonList(TOPIC), adminClient);
//            Assert.Equal(Collections.emptySet(), adminClient.listTopics().names().Get());
//        }

//        private Cluster CreateCluster(int numNodes)
//        {
//            HashDictionary<int, Node> nodes = new HashMap<>();
//            for (int i = 0; i < numNodes; ++i)
//            {
//                nodes.put(i, new Node(i, "localhost", 8121 + i));
//            }
//            return new Cluster("mockClusterId", nodes.values(),
//                Collections.< PartitionInfo > emptySet(), Collections.< string > emptySet(),
//                Collections.< string > emptySet(), nodes.Get(0));
//        }

//        [Fact]
//        public void ShouldAcceptValidDateFormats() // throws ParseException
//        {
//            //check valid formats
//            InvokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"));
//            InvokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
//            InvokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX"));
//            InvokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXX"));
//            InvokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"));
//        }

//        [Fact]
//        public void ShouldThrowOnInvalidDateFormat() // throws ParseException

//        {
//            //check some invalid formats
//            try
//            {
//                InvokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"));
//                Assert.True(false, "Call to getDateTime should fail");
//            }
//            catch (Exception e)
//            {
//                e.printStackTrace();
//            }

//            try
//            {
//                InvokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.X"));
//                Assert.True(false, "Call to getDateTime should fail");
//            }
//            catch (Exception e)
//            {
//                e.printStackTrace();
//            }
//        }

//        private void InvokeGetDateTimeMethod(SimpleDateFormat format) // throws ParseException

//        {
//            Date checkpoint = new Date();
//            StreamsResetter streamsResetter = new StreamsResetter();
//            string formattedCheckpoint = format.format(checkpoint);
//            streamsResetter.getDateTime(formattedCheckpoint);
//        }
//    }
//}
