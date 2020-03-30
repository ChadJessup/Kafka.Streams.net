/*






 *

 *





 */



























public class PartitionGroupTest {
    private LogContext logContext = new LogContext();
    private Serializer<int> intSerializer = new IntegerSerializer();
    private Deserializer<int> intDeserializer = Serializers.Int32;
    private TimestampExtractor timestampExtractor = new MockTimestampExtractor();
    private readonly string[] topics = {"topic"};
    private TopicPartition partition1 = new TopicPartition(topics[0], 1);
    private TopicPartition partition2 = new TopicPartition(topics[0], 2);
    private RecordQueue queue1 = new RecordQueue(
        partition1,
        new MockSourceNode<>(topics, intDeserializer, intDeserializer),
        timestampExtractor,
        new LogAndContinueExceptionHandler(),
        new InternalMockProcessorContext(),
        logContext
    );
    private RecordQueue queue2 = new RecordQueue(
        partition2,
        new MockSourceNode<>(topics, intDeserializer, intDeserializer),
        timestampExtractor,
        new LogAndContinueExceptionHandler(),
        new InternalMockProcessorContext(),
        logContext
    );

    private readonly byte[] recordValue = intSerializer.serialize(null, 10);
    private readonly byte[] recordKey = intSerializer.serialize(null, 1);

    private Metrics metrics = new Metrics();
    private MetricName lastLatenessValue = new MetricName("record-lateness-last-value", "", "", mkMap());

    private PartitionGroup group = new PartitionGroup(
        mkMap(mkEntry(partition1, queue1), mkEntry(partition2, queue2)),
        GetValueSensor(metrics, lastLatenessValue)
    );

    private static Sensor GetValueSensor(Metrics metrics, MetricName metricName) {
        Sensor lastRecordedValue = metrics.sensor(metricName.name());
        lastRecordedValue.add(metricName, new Value());
        return lastRecordedValue;
    }

    [Xunit.Fact]
    public void TestTimeTracking() {
        Assert.Equal(0, group.numBuffered());

        // add three 3 records with timestamp 1, 3, 5 to partition-1
        List<ConsumeResult<byte[], byte[]>> list1 = Array.asList(
            new ConsumeResult<>("topic", 1, 1L, recordKey, recordValue),
            new ConsumeResult<>("topic", 1, 3L, recordKey, recordValue),
            new ConsumeResult<>("topic", 1, 5L, recordKey, recordValue));

        group.addRawRecords(partition1, list1);

        // add three 3 records with timestamp 2, 4, 6 to partition-2
        List<ConsumeResult<byte[], byte[]>> list2 = Array.asList(
            new ConsumeResult<>("topic", 2, 2L, recordKey, recordValue),
            new ConsumeResult<>("topic", 2, 4L, recordKey, recordValue),
            new ConsumeResult<>("topic", 2, 6L, recordKey, recordValue));

        group.addRawRecords(partition2, list2);
        // 1:[1, 3, 5]
        // 2:[2, 4, 6]
        // st: -1 since no records was being processed yet

        VerifyBuffered(6, 3, 3);
        Assert.Equal(-1L, group.streamTime());
        Assert.Equal(0.0, metrics.metric(lastLatenessValue).metricValue());

        StampedRecord record;
        PartitionGroup.RecordInfo info = new PartitionGroup.RecordInfo();

        // get one record, now the time should be advanced
        record = group.nextRecord(info);
        // 1:[3, 5]
        // 2:[2, 4, 6]
        // st: 1
        Assert.Equal(partition1, info.partition());
        VerifyTimes(record, 1L, 1L);
        VerifyBuffered(5, 2, 3);
        Assert.Equal(0.0, metrics.metric(lastLatenessValue).metricValue());

        // get one record, now the time should be advanced
        record = group.nextRecord(info);
        // 1:[3, 5]
        // 2:[4, 6]
        // st: 2
        Assert.Equal(partition2, info.partition());
        VerifyTimes(record, 2L, 2L);
        VerifyBuffered(4, 2, 2);
        Assert.Equal(0.0, metrics.metric(lastLatenessValue).metricValue());

        // add 2 more records with timestamp 2, 4 to partition-1
        List<ConsumeResult<byte[], byte[]>> list3 = Array.asList(
            new ConsumeResult<>("topic", 1, 2L, recordKey, recordValue),
            new ConsumeResult<>("topic", 1, 4L, recordKey, recordValue));

        group.addRawRecords(partition1, list3);
        // 1:[3, 5, 2, 4]
        // 2:[4, 6]
        // st: 2 (just adding records shouldn't change it)
        VerifyBuffered(6, 4, 2);
        Assert.Equal(2L, group.streamTime());
        Assert.Equal(0.0, metrics.metric(lastLatenessValue).metricValue());

        // get one record, time should be advanced
        record = group.nextRecord(info);
        // 1:[5, 2, 4]
        // 2:[4, 6]
        // st: 3
        Assert.Equal(partition1, info.partition());
        VerifyTimes(record, 3L, 3L);
        VerifyBuffered(5, 3, 2);
        Assert.Equal(0.0, metrics.metric(lastLatenessValue).metricValue());

        // get one record, time should be advanced
        record = group.nextRecord(info);
        // 1:[5, 2, 4]
        // 2:[6]
        // st: 4
        Assert.Equal(partition2, info.partition());
        VerifyTimes(record, 4L, 4L);
        VerifyBuffered(4, 3, 1);
        Assert.Equal(0.0, metrics.metric(lastLatenessValue).metricValue());

        // get one more record, time should be advanced
        record = group.nextRecord(info);
        // 1:[2, 4]
        // 2:[6]
        // st: 5
        Assert.Equal(partition1, info.partition());
        VerifyTimes(record, 5L, 5L);
        VerifyBuffered(3, 2, 1);
        Assert.Equal(0.0, metrics.metric(lastLatenessValue).metricValue());

        // get one more record, time should not be advanced
        record = group.nextRecord(info);
        // 1:[4]
        // 2:[6]
        // st: 5
        Assert.Equal(partition1, info.partition());
        VerifyTimes(record, 2L, 5L);
        VerifyBuffered(2, 1, 1);
        Assert.Equal(3.0, metrics.metric(lastLatenessValue).metricValue());

        // get one more record, time should not be advanced
        record = group.nextRecord(info);
        // 1:[]
        // 2:[6]
        // st: 5
        Assert.Equal(partition1, info.partition());
        VerifyTimes(record, 4L, 5L);
        VerifyBuffered(1, 0, 1);
        Assert.Equal(1.0, metrics.metric(lastLatenessValue).metricValue());

        // get one more record, time should be advanced
        record = group.nextRecord(info);
        // 1:[]
        // 2:[]
        // st: 6
        Assert.Equal(partition2, info.partition());
        VerifyTimes(record, 6L, 6L);
        VerifyBuffered(0, 0, 0);
        Assert.Equal(0.0, metrics.metric(lastLatenessValue).metricValue());

    }

    [Xunit.Fact]
    public void ShouldChooseNextRecordBasedOnHeadTimestamp() {
        Assert.Equal(0, group.numBuffered());

        // add three 3 records with timestamp 1, 5, 3 to partition-1
        List<ConsumeResult<byte[], byte[]>> list1 = Array.asList(
            new ConsumeResult<>("topic", 1, 1L, recordKey, recordValue),
            new ConsumeResult<>("topic", 1, 5L, recordKey, recordValue),
            new ConsumeResult<>("topic", 1, 3L, recordKey, recordValue));

        group.addRawRecords(partition1, list1);

        VerifyBuffered(3, 3, 0);
        Assert.Equal(-1L, group.streamTime());
        Assert.Equal(0.0, metrics.metric(lastLatenessValue).metricValue());

        StampedRecord record;
        PartitionGroup.RecordInfo info = new PartitionGroup.RecordInfo();

        // get first two records from partition 1
        record = group.nextRecord(info);
        Assert.Equal(record.timestamp, 1L);
        record = group.nextRecord(info);
        Assert.Equal(record.timestamp, 5L);

        // add three 3 records with timestamp 2, 4, 6 to partition-2
        List<ConsumeResult<byte[], byte[]>> list2 = Array.asList(
            new ConsumeResult<>("topic", 2, 2L, recordKey, recordValue),
            new ConsumeResult<>("topic", 2, 4L, recordKey, recordValue),
            new ConsumeResult<>("topic", 2, 6L, recordKey, recordValue));

        group.addRawRecords(partition2, list2);
        // 1:[3]
        // 2:[2, 4, 6]

        // get one record, next record should be ts=2 from partition 2
        record = group.nextRecord(info);
        // 1:[3]
        // 2:[4, 6]
        Assert.Equal(record.timestamp, 2L);

        // get one record, next up should have ts=3 from partition 1 (even though it has seen a larger max timestamp =5)
        record = group.nextRecord(info);
        // 1:[]
        // 2:[4, 6]
        Assert.Equal(record.timestamp, 3L);
    }

    private void VerifyTimes(StampedRecord record, long recordTime, long streamTime) {
        Assert.Equal(recordTime, record.timestamp);
        Assert.Equal(streamTime, group.streamTime());
    }

    private void VerifyBuffered(int totalBuffered, int partitionOneBuffered, int partitionTwoBuffered) {
        Assert.Equal(totalBuffered, group.numBuffered());
        Assert.Equal(partitionOneBuffered, group.numBuffered(partition1));
        Assert.Equal(partitionTwoBuffered, group.numBuffered(partition2));
    }
}
