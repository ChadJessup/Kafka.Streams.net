using Confluent.Kafka;
using Xunit;

namespace Kafka.Streams.Tests.Tools
{
    public class StreamsResetterTest
    {

        private static string TOPIC = "topic1";
        private StreamsResetter streamsResetter = new StreamsResetter();
        private MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        private TopicPartition topicPartition = new TopicPartition(TOPIC, 0);
        private HashSet<TopicPartition> inputTopicPartitions = new HashSet<>(Collections.singletonList(topicPartition));


        public void setUp()
        {
            consumer.assign(Collections.singletonList(topicPartition));

            consumer.addRecord(new ConsumeResult<>(TOPIC, 0, 0L, new byte[] { }, new byte[] { }));
            consumer.addRecord(new ConsumeResult<>(TOPIC, 0, 1L, new byte[] { }, new byte[] { }));
            consumer.addRecord(new ConsumeResult<>(TOPIC, 0, 2L, new byte[] { }, new byte[] { }));
            consumer.addRecord(new ConsumeResult<>(TOPIC, 0, 3L, new byte[] { }, new byte[] { }));
            consumer.addRecord(new ConsumeResult<>(TOPIC, 0, 4L, new byte[] { }, new byte[] { }));
        }

        [Xunit.Fact]
        public void testResetToSpecificOffsetWhenBetweenBeginningAndEndOffset()
        {
            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
            endOffsets.put(topicPartition, 4L);
            consumer.updateEndOffsets(endOffsets);

            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
            beginningOffsets.put(topicPartition, 0L);
            consumer.updateBeginningOffsets(beginningOffsets);

            streamsResetter.resetOffsetsTo(consumer, inputTopicPartitions, 2L);

            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
            Assert.Equal(3, records.count());
        }

        [Xunit.Fact]
        public void testResetToSpecificOffsetWhenBeforeBeginningOffset()
        {
            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
            endOffsets.put(topicPartition, 4L);
            consumer.updateEndOffsets(endOffsets);

            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
            beginningOffsets.put(topicPartition, 3L);
            consumer.updateBeginningOffsets(beginningOffsets);

            streamsResetter.resetOffsetsTo(consumer, inputTopicPartitions, 2L);

            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
            Assert.Equal(2, records.count());
        }

        [Xunit.Fact]
        public void testResetToSpecificOffsetWhenAfterEndOffset()
        {
            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
            endOffsets.put(topicPartition, 3L);
            consumer.updateEndOffsets(endOffsets);

            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
            beginningOffsets.put(topicPartition, 0L);
            consumer.updateBeginningOffsets(beginningOffsets);

            streamsResetter.resetOffsetsTo(consumer, inputTopicPartitions, 4L);

            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
            Assert.Equal(2, records.count());
        }

        [Xunit.Fact]
        public void testShiftOffsetByWhenBetweenBeginningAndEndOffset()
        {
            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
            endOffsets.put(topicPartition, 4L);
            consumer.updateEndOffsets(endOffsets);

            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
            beginningOffsets.put(topicPartition, 0L);
            consumer.updateBeginningOffsets(beginningOffsets);

            streamsResetter.shiftOffsetsBy(consumer, inputTopicPartitions, 3L);

            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
            Assert.Equal(2, records.count());
        }

        [Xunit.Fact]
        public void testShiftOffsetByWhenBeforeBeginningOffset()
        {
            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
            endOffsets.put(topicPartition, 4L);
            consumer.updateEndOffsets(endOffsets);

            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
            beginningOffsets.put(topicPartition, 0L);
            consumer.updateBeginningOffsets(beginningOffsets);

            streamsResetter.shiftOffsetsBy(consumer, inputTopicPartitions, -3L);

            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
            Assert.Equal(5, records.count());
        }

        [Xunit.Fact]
        public void testShiftOffsetByWhenAfterEndOffset()
        {
            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
            endOffsets.put(topicPartition, 3L);
            consumer.updateEndOffsets(endOffsets);

            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
            beginningOffsets.put(topicPartition, 0L);
            consumer.updateBeginningOffsets(beginningOffsets);

            streamsResetter.shiftOffsetsBy(consumer, inputTopicPartitions, 5L);

            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
            Assert.Equal(2, records.count());
        }

        [Xunit.Fact]
        public void testResetUsingPlanWhenBetweenBeginningAndEndOffset()
        {
            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
            endOffsets.put(topicPartition, 4L);
            consumer.updateEndOffsets(endOffsets);

            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
            beginningOffsets.put(topicPartition, 0L);
            consumer.updateBeginningOffsets(beginningOffsets);

            Dictionary<TopicPartition, long> topicPartitionsAndOffset = new HashMap<>();
            topicPartitionsAndOffset.put(topicPartition, 3L);
            streamsResetter.resetOffsetsFromResetPlan(consumer, inputTopicPartitions, topicPartitionsAndOffset);

            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
            Assert.Equal(2, records.count());
        }

        [Xunit.Fact]
        public void testResetUsingPlanWhenBeforeBeginningOffset()
        {
            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
            endOffsets.put(topicPartition, 4L);
            consumer.updateEndOffsets(endOffsets);

            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
            beginningOffsets.put(topicPartition, 3L);
            consumer.updateBeginningOffsets(beginningOffsets);

            Dictionary<TopicPartition, long> topicPartitionsAndOffset = new HashMap<>();
            topicPartitionsAndOffset.put(topicPartition, 1L);
            streamsResetter.resetOffsetsFromResetPlan(consumer, inputTopicPartitions, topicPartitionsAndOffset);

            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
            Assert.Equal(2, records.count());
        }

        [Xunit.Fact]
        public void testResetUsingPlanWhenAfterEndOffset()
        {
            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
            endOffsets.put(topicPartition, 3L);
            consumer.updateEndOffsets(endOffsets);

            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
            beginningOffsets.put(topicPartition, 0L);
            consumer.updateBeginningOffsets(beginningOffsets);

            Dictionary<TopicPartition, long> topicPartitionsAndOffset = new HashMap<>();
            topicPartitionsAndOffset.put(topicPartition, 5L);
            streamsResetter.resetOffsetsFromResetPlan(consumer, inputTopicPartitions, topicPartitionsAndOffset);

            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
            Assert.Equal(2, records.count());
        }

        [Xunit.Fact]
        public void shouldSeekToEndOffset()
        {
            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
            endOffsets.put(topicPartition, 3L);
            consumer.updateEndOffsets(endOffsets);

            Dictionary<TopicPartition, long> beginningOffsets = new HashMap<>();
            beginningOffsets.put(topicPartition, 0L);
            consumer.updateBeginningOffsets(beginningOffsets);

            HashSet<TopicPartition> intermediateTopicPartitions = new HashSet<>();
            intermediateTopicPartitions.add(topicPartition);
            streamsResetter.maybeSeekToEnd("g1", consumer, intermediateTopicPartitions);

            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
            Assert.Equal(2, records.count());
        }

        [Xunit.Fact]//  throws InterruptedException, ExecutionException
        public void shouldDeleteTopic()
        {
            Cluster cluster = createCluster(1);
            try
            {
                (MockAdminClient adminClient = new MockAdminClient(cluster.nodes(), cluster.nodeById(0)));
                TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, cluster.nodeById(0), cluster.nodes(), Collections.< Node > emptyList());
                adminClient.addTopic(false, TOPIC, Collections.singletonList(topicPartitionInfo), null);
                streamsResetter.doDelete(Collections.singletonList(TOPIC), adminClient);
                Assert.Equal(Collections.emptySet(), adminClient.listTopics().names().get());
            }
            }

        private Cluster createCluster(int numNodes)
        {
            HashDictionary<int, Node> nodes = new HashMap<>();
            for (int i = 0; i < numNodes; ++i)
            {
                nodes.put(i, new Node(i, "localhost", 8121 + i));
            }
            return new Cluster("mockClusterId", nodes.values(),
                Collections.< PartitionInfo > emptySet(), Collections.< string > emptySet(),
                Collections.< string > emptySet(), nodes.get(0));
        }

        [Xunit.Fact]
        public void shouldAcceptValidDateFormats() // throws ParseException
        {
            //check valid formats
            invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"));
            invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
            invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX"));
            invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXX"));
            invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"));
        }

        [Xunit.Fact]
        public void shouldThrowOnInvalidDateFormat() // throws ParseException

        {
            //check some invalid formats
            try
            {
                invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"));
                Assert.True(false, "Call to getDateTime should fail");
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

            try
            {
                invokeGetDateTimeMethod(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.X"));
                Assert.True(false, "Call to getDateTime should fail");
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        private void invokeGetDateTimeMethod(SimpleDateFormat format) // throws ParseException

        {
            Date checkpoint = new Date();
            StreamsResetter streamsResetter = new StreamsResetter();
            string formattedCheckpoint = format.format(checkpoint);
            streamsResetter.getDateTime(formattedCheckpoint);
        }
    }
}
