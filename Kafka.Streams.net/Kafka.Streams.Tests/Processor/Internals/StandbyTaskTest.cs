namespace Kafka.Streams.Tests.Processor.Internals
{
}
///*






// *

// *





// */















































































//public class StandbyTaskTest {

//    private TaskId taskId = new TaskId(0, 1);
//    private StandbyTask task;
//    private Serializer<int> intSerializer = new IntegerSerializer();

//    private string applicationId = "test-application";
//    private string storeName1 = "store1";
//    private string storeName2 = "store2";
//    private string storeChangelogTopicName1 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName1);
//    private string storeChangelogTopicName2 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName2);
//    private string globalStoreName = "ktable1";

//    private TopicPartition partition1 = new TopicPartition(storeChangelogTopicName1, 1);
//    private TopicPartition partition2 = new TopicPartition(storeChangelogTopicName2, 1);
//    private MockStateRestoreListener stateRestoreListener = new MockStateRestoreListener();

//    private HashSet<TopicPartition> topicPartitions = Collections.emptySet();
//    private ProcessorTopology topology = ProcessorTopologyFactories.withLocalStores(
//        asList(new MockKeyValueStoreBuilder(storeName1, false).Build(),
//               new MockKeyValueStoreBuilder(storeName2, true).Build()),
//        mkMap(
//            mkEntry(storeName1, storeChangelogTopicName1),
//            mkEntry(storeName2, storeChangelogTopicName2)
//        )
//    );
//    private TopicPartition globalTopicPartition = new TopicPartition(globalStoreName, 0);
//    private HashSet<TopicPartition> ktablePartitions = Utils.mkSet(globalTopicPartition);
//    private ProcessorTopology ktableTopology = ProcessorTopologyFactories.withLocalStores(
//        singletonList(new MockKeyValueStoreBuilder(globalTopicPartition.Topic, true)
//                          .withLoggingDisabled().Build()),
//        mkMap(
//            mkEntry(globalStoreName, globalTopicPartition.Topic)
//        )
//    );

//    private File baseDir;
//    private StateDirectory stateDirectory;

//    private StreamsConfig createConfig(File baseDir){ //throws IOException
//        return new StreamsConfig(mkProperties(mkMap(
//            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, applicationId),
//            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171"),
//            mkEntry(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3"),
//            mkEntry(StreamsConfig.STATE_DIR_CONFIG, baseDir.getCanonicalPath()),
//            mkEntry(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.getName())
//        )));
//    }

//    private MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
//    private MockRestoreConsumer<int, int> restoreStateConsumer = new MockRestoreConsumer<>(
//        new IntegerSerializer(),
//        new IntegerSerializer()
//    );
//    private StoreChangelogReader changelogReader = new StoreChangelogReader(
//        restoreStateConsumer,
//        Duration.TimeSpan.Zero,
//        stateRestoreListener,
//        new LogContext("standby-task-test ")
//    );

//    private byte[] recordValue = intSerializer.Serialize(null, 10);
//    private byte[] recordKey = intSerializer.Serialize(null, 1);

//    private string threadName = "threadName";
//    private StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), threadName);


//    public void setup() {// throws Exception
//        restoreStateConsumer.reset();
//        restoreStateConsumer.updatePartitions(storeChangelogTopicName1, asList(
//            new PartitionInfo(storeChangelogTopicName1, 0, Node.noNode(), new Node[0], new Node[0]),
//            new PartitionInfo(storeChangelogTopicName1, 1, Node.noNode(), new Node[0], new Node[0]),
//            new PartitionInfo(storeChangelogTopicName1, 2, Node.noNode(), new Node[0], new Node[0])
//        ));

//        restoreStateConsumer.updatePartitions(storeChangelogTopicName2, asList(
//            new PartitionInfo(storeChangelogTopicName2, 0, Node.noNode(), new Node[0], new Node[0]),
//            new PartitionInfo(storeChangelogTopicName2, 1, Node.noNode(), new Node[0], new Node[0]),
//            new PartitionInfo(storeChangelogTopicName2, 2, Node.noNode(), new Node[0], new Node[0])
//        ));
//        baseDir = TestUtils.GetTempDirectory();
//        stateDirectory = new StateDirectory(createConfig(baseDir), new MockTime(), true);
//    }


//    public void cleanup(){ //throws IOException
//        if (task != null && !task.isClosed()) {
//            task.close(true, false);
//            task = null;
//        }
//        Utils.delete(baseDir);
//    }

//    [Fact]
//    public void testStorePartitions(){ //throws IOException
//        StreamsConfig config = createConfig(baseDir);
//        task = new StandbyTask(taskId,
//                               topicPartitions,
//                               topology,
//                               consumer,
//                               changelogReader,
//                               config,
//                               streamsMetrics,
//                               stateDirectory);
//        task.initializeStateStores();
//        Assert.Equal(Utils.mkSet(partition2, partition1), new HashSet<>(task.checkpointedOffsets().keySet()));
//    }


//    [Fact]
//    public void testUpdateNonInitializedStore(){ //throws IOException
//        StreamsConfig config = createConfig(baseDir);
//        task = new StandbyTask(taskId,
//                               topicPartitions,
//                               topology,
//                               consumer,
//                               changelogReader,
//                               config,
//                               streamsMetrics,
//                               stateDirectory);

//        restoreStateConsumer.assign(new ArrayList<>(task.checkpointedOffsets().keySet()));

//        try {
//            task.update(partition1,
//                        singletonList(
//                            new ConsumeResult<>(
//                                partition1.Topic,
//                                partition1.Partition,
//                                10,
//                                0L,
//                                TimestampType.CreateTime,
//                                0L,
//                                0,
//                                0,
//                                recordKey,
//                                recordValue))
//            );
//            Assert.True(false, "expected an exception");
//        } catch (NullPointerException npe) {
//            Assert.Equal(npe.getMessage(), containsString("stateRestoreCallback must not be null"));
//        }

//    }

//    [Fact]
//    public void testUpdate(){ //throws IOException
//        StreamsConfig config = createConfig(baseDir);
//        task = new StandbyTask(taskId,
//                               topicPartitions,
//                               topology,
//                               consumer,
//                               changelogReader,
//                               config,
//                               streamsMetrics,
//                               stateDirectory);
//        task.initializeStateStores();
//        HashSet<TopicPartition> partition = Collections.singleton(partition2);
//        restoreStateConsumer.assign(partition);

//        foreach (ConsumeResult<int, int> record in asList(new ConsumeResult<>(partition2.Topic,
//                                                                                         partition2.Partition,
//                                                                                         10,
//                                                                                         0L,
//                                                                                         TimestampType.CreateTime,
//                                                                                         0L,
//                                                                                         0,
//                                                                                         0,
//                                                                                         1,
//                                                                                         100),
//                                                                    new ConsumeResult<>(partition2.Topic,
//                                                                                         partition2.Partition,
//                                                                                         20,
//                                                                                         0L,
//                                                                                         TimestampType.CreateTime,
//                                                                                         0L,
//                                                                                         0,
//                                                                                         0,
//                                                                                         2,
//                                                                                         100),
//                                                                    new ConsumeResult<>(partition2.Topic,
//                                                                                         partition2.Partition,
//                                                                                         30,
//                                                                                         0L,
//                                                                                         TimestampType.CreateTime,
//                                                                                         0L,
//                                                                                         0,
//                                                                                         0,
//                                                                                         3,
//                                                                                         100))) {
//            restoreStateConsumer.bufferRecord(record);
//        }

//        restoreStateConsumer.seekToBeginning(partition);
//        task.update(partition2, restoreStateConsumer.poll(FromMilliseconds(100)).records(partition2));

//        StandbyContextImpl context = (StandbyContextImpl) task.context;
//        MockKeyValueStore store1 = (MockKeyValueStore) context.getStateMgr().getStore(storeName1);
//        MockKeyValueStore store2 = (MockKeyValueStore) context.getStateMgr().getStore(storeName2);

//        Assert.Equal(Collections.emptyList(), store1.keys);
//        Assert.Equal(asList(1, 2, 3), store2.keys);
//    }

//    [Fact]
//    public void shouldRestoreToWindowedStores(){ //throws IOException
//        string storeName = "windowed-store";
//        string changelogName = applicationId + "-" + storeName + "-changelog";

//        TopicPartition topicPartition = new TopicPartition(changelogName, 1);

//        List<TopicPartition> partitions = Collections.singletonList(topicPartition);

//        consumer.assign(partitions);

//        InternalTopologyBuilder internalTopologyBuilder = new InternalTopologyBuilder().setApplicationId(applicationId);

//        InternalStreamsBuilder builder = new InternalStreamsBuilder(internalTopologyBuilder);

//        builder
//            .Stream(Collections.singleton("topic"), new ConsumedInternal<>())
//            .groupByKey()
//            .windowedBy(TimeWindows.of(FromMilliseconds(60_000)).grace(FromMilliseconds(0L)))
//            .count(Materialized<object, long, IWindowStore<Bytes, byte[]>>.As(storeName).withRetention(FromMilliseconds(120_000L)));

//        builder.buildAndOptimizeTopology();

//        task = new StandbyTask(
//            taskId,
//            partitions,
//            internalTopologyBuilder.Build(0),
//            consumer,
//            new StoreChangelogReader(
//                restoreStateConsumer,
//                Duration.TimeSpan.Zero,
//                stateRestoreListener,
//                new LogContext("standby-task-test ")
//            ),
//            createConfig(baseDir),
//            new MockStreamsMetrics(new Metrics()),
//            stateDirectory
//        );

//        task.initializeStateStores();

//        consumer.commitSync(mkMap(mkEntry(topicPartition, new OffsetAndMetadata(35L))));
//        task.commit();

//        List<ConsumeResult<byte[], byte[]>> remaining1 = task.update(
//            topicPartition,
//            asList(
//                makeWindowedConsumerRecord(changelogName, 10, 1, 0L, 60_000L),
//                makeWindowedConsumerRecord(changelogName, 20, 2, 60_000L, 120_000),
//                makeWindowedConsumerRecord(changelogName, 30, 3, 120_000L, 180_000),
//                makeWindowedConsumerRecord(changelogName, 40, 4, 180_000L, 240_000)
//            )
//        );

//        Assert.Equal(
//            asList(
//                KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0, 60_000)), ValueAndTimestamp.Make(100L, 60_000L)),
//                KeyValuePair.Create(new Windowed<>(2, new TimeWindow(60_000, 120_000)), ValueAndTimestamp.Make(100L, 120_000L)),
//                KeyValuePair.Create(new Windowed<>(3, new TimeWindow(120_000, 180_000)), ValueAndTimestamp.Make(100L, 180_000L))
//            ),
//            getWindowedStoreContents(storeName, task)
//        );

//        consumer.commitSync(mkMap(mkEntry(topicPartition, new OffsetAndMetadata(45L))));
//        task.commit();

//        List<ConsumeResult<byte[], byte[]>> remaining2 = task.update(topicPartition, remaining1);
//        Assert.Equal(emptyList(), remaining2);

//        // the first record's window should have expired.
//        Assert.Equal(
//            asList(
//                KeyValuePair.Create(new Windowed<>(2, new TimeWindow(60_000, 120_000)), ValueAndTimestamp.Make(100L, 120_000L)),
//                KeyValuePair.Create(new Windowed<>(3, new TimeWindow(120_000, 180_000)), ValueAndTimestamp.Make(100L, 180_000L)),
//                KeyValuePair.Create(new Windowed<>(4, new TimeWindow(180_000, 240_000)), ValueAndTimestamp.Make(100L, 240_000L))
//            ),
//            getWindowedStoreContents(storeName, task)
//        );
//    }

//    private ConsumeResult<byte[], byte[]> makeWindowedConsumerRecord(string changelogName,
//                                                                      int offset,
//                                                                      int key,
//                                                                      long start,
//                                                                      long end) {
//        Windowed<int> data = new Windowed<>(key, new TimeWindow(start, end));
//        Bytes wrap = Bytes.Wrap(new IntegerSerializer().Serialize(null, data.Key));
//        byte[] keyBytes = WindowKeySchema.toStoreKeyBinary(new Windowed<>(wrap, data.window()), 1).Get();
//        return new ConsumeResult<>(
//            changelogName,
//            1,
//            offset,
//            end,
//            TimestampType.CreateTime,
//            0L,
//            0,
//            0,
//            keyBytes,
//            new Serdes.Long().Serializer().Serialize(null, 100L)
//        );
//    }

//    [Fact]
//    public void shouldWriteCheckpointFile(){ //throws IOException
//        string storeName = "checkpoint-file-store";
//        string changelogName = applicationId + "-" + storeName + "-changelog";

//        TopicPartition topicPartition = new TopicPartition(changelogName, 1);
//        List<TopicPartition> partitions = Collections.singletonList(topicPartition);

//        InternalTopologyBuilder internalTopologyBuilder = new InternalTopologyBuilder().setApplicationId(applicationId);

//        InternalStreamsBuilder builder = new InternalStreamsBuilder(internalTopologyBuilder);
//        builder.Stream(Collections.singleton("topic"), new ConsumedInternal<>())
//            .groupByKey()
//            .count(Materialized.As(storeName));

//        builder.buildAndOptimizeTopology();

//        consumer.assign(partitions);

//        task = new StandbyTask(
//            taskId,
//            partitions,
//            internalTopologyBuilder.Build(0),
//            consumer,
//            changelogReader,
//            createConfig(baseDir),
//            new MockStreamsMetrics(new Metrics()),
//            stateDirectory
//        );
//        task.initializeStateStores();

//        consumer.commitSync(mkMap(mkEntry(topicPartition, new OffsetAndMetadata(20L))));
//        task.commit();

//        task.update(
//            topicPartition,
//            singletonList(makeWindowedConsumerRecord(changelogName, 10, 1, 0L, 60_000L))
//        );

//        task.suspend();
//        task.close(true, false);

//        File taskDir = stateDirectory.directoryForTask(taskId);
//        OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(taskDir, StateManagerUtil.CHECKPOINT_FILE_NAME));
//        Dictionary<TopicPartition, long> offsets = checkpoint.read();

//        Assert.Equal(1, offsets.Count);
//        Assert.Equal(new long(11L), offsets.Get(topicPartition));
//    }


//    private List<KeyValuePair<Windowed<int>, ValueAndTimestamp<long>>> getWindowedStoreContents(string storeName,
//                                                                                                StandbyTask task) {
//        StandbyContextImpl context = (StandbyContextImpl) task.context;

//        List<KeyValuePair<Windowed<int>, ValueAndTimestamp<long>>> result = new ArrayList<>();

//        try (IKeyValueIterator<Windowed<byte[]>, ValueAndTimestamp<long>> iterator =
//                 ((ITimestampedWindowStore) context.getStateMgr().getStore(storeName)).all()) {

//            while (iterator.hasNext()) {
//                KeyValuePair<Windowed<byte[]>, ValueAndTimestamp<long>> next = iterator.MoveNext();
//                int deserializedKey = Serializers.Int32.deserialize(null, next.key.Key);
//                result.Add(KeyValuePair.Create(new Windowed<>(deserializedKey, next.key.window()), next.value));
//            }
//        }

//        return result;
//    }

//    [Fact]
//    public void shouldRestoreToKTable(){ //throws IOException
//        consumer.assign(Collections.singletonList(globalTopicPartition));
//        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(0L))));

//        task = new StandbyTask(
//            taskId,
//            ktablePartitions,
//            ktableTopology,
//            consumer,
//            changelogReader,
//            createConfig(baseDir),
//            streamsMetrics,
//            stateDirectory
//        );
//        task.initializeStateStores();

//        // The commit offset is at 0L. Records should not be processed
//        List<ConsumeResult<byte[], byte[]>> remaining = task.update(
//            globalTopicPartition,
//            asList(
//                makeConsumerRecord(globalTopicPartition, 10, 1),
//                makeConsumerRecord(globalTopicPartition, 20, 2),
//                makeConsumerRecord(globalTopicPartition, 30, 3),
//                makeConsumerRecord(globalTopicPartition, 40, 4),
//                makeConsumerRecord(globalTopicPartition, 50, 5)
//            )
//        );
//        Assert.Equal(5, remaining.Count);

//        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(10L))));
//        task.commit(); // update offset limits

//        // The commit offset has not reached, yet.
//        remaining = task.update(globalTopicPartition, remaining);
//        Assert.Equal(5, remaining.Count);

//        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(11L))));
//        task.commit(); // update offset limits

//        // one record should be processed.
//        remaining = task.update(globalTopicPartition, remaining);
//        Assert.Equal(4, remaining.Count);

//        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(45L))));
//        task.commit(); // update offset limits

//        // The commit offset is now 45. All record except for the last one should be processed.
//        remaining = task.update(globalTopicPartition, remaining);
//        Assert.Equal(1, remaining.Count);

//        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(50L))));
//        task.commit(); // update offset limits

//        // The commit offset is now 50. Still the last record remains.
//        remaining = task.update(globalTopicPartition, remaining);
//        Assert.Equal(1, remaining.Count);

//        consumer.commitSync(mkMap(mkEntry(globalTopicPartition, new OffsetAndMetadata(60L))));
//        task.commit(); // update offset limits

//        // The commit offset is now 60. No record should be left.
//        remaining = task.update(globalTopicPartition, remaining);
//        Assert.Equal(emptyList(), remaining);
//    }

//    private ConsumeResult<byte[], byte[]> makeConsumerRecord(TopicPartition topicPartition,
//                                                              long offset,
//                                                              int key) {
//        IntegerSerializer integerSerializer = new IntegerSerializer();
//        return new ConsumeResult<>(
//            topicPartition.Topic,
//            topicPartition.Partition,
//            offset,
//            0L,
//            TimestampType.CreateTime,
//            0L,
//            0,
//            0,
//            integerSerializer.Serialize(null, key),
//            integerSerializer.Serialize(null, 100)
//        );
//    }

//    [Fact]
//    public void shouldInitializeStateStoreWithoutException(){ //throws IOException
//        InternalStreamsBuilder builder = new InternalStreamsBuilder(new InternalTopologyBuilder());
//        builder.Stream(Collections.singleton("topic"), new ConsumedInternal<>()).groupByKey().count();

//        initializeStandbyStores(builder);
//    }

//    [Fact]
//    public void shouldInitializeWindowStoreWithoutException(){ //throws IOException
//        InternalStreamsBuilder builder = new InternalStreamsBuilder(new InternalTopologyBuilder());
//        builder.Stream(Collections.singleton("topic"),
//                       new ConsumedInternal<>()).groupByKey().windowedBy(TimeWindows.of(FromMilliseconds(100))).count();

//        initializeStandbyStores(builder);
//    }

//    private void initializeStandbyStores(InternalStreamsBuilder builder){ //throws IOException
//        StreamsConfig config = createConfig(baseDir);
//        builder.buildAndOptimizeTopology();
//        InternalTopologyBuilder internalTopologyBuilder = InternalStreamsBuilderTest.internalTopologyBuilder(builder);
//        ProcessorTopology topology = internalTopologyBuilder.setApplicationId(applicationId).Build(0);

//        task = new StandbyTask(
//            taskId,
//            emptySet(),
//            topology,
//            consumer,
//            changelogReader,
//            config,
//            new MockStreamsMetrics(new Metrics()),
//            stateDirectory
//        );

//        task.initializeStateStores();

//        Assert.True(task.hasStateStores());
//    }

//    [Fact]
//    public void shouldCheckpointStoreOffsetsOnCommit(){ //throws IOException
//        consumer.assign(Collections.singletonList(globalTopicPartition));
//        Dictionary<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
//        committedOffsets.put(new TopicPartition(globalTopicPartition.Topic, globalTopicPartition.Partition),
//                             new OffsetAndMetadata(100L));
//        consumer.commitSync(committedOffsets);

//        restoreStateConsumer.updatePartitions(
//            globalStoreName,
//            Collections.singletonList(new PartitionInfo(globalStoreName, 0, Node.noNode(), new Node[0], new Node[0]))
//        );

//        TaskId taskId = new TaskId(0, 0);
//        MockTime time = new MockTime();
//        StreamsConfig config = createConfig(baseDir);
//        task = new StandbyTask(
//            taskId,
//            ktablePartitions,
//            ktableTopology,
//            consumer,
//            changelogReader,
//            config,
//            streamsMetrics,
//            stateDirectory
//        );
//        task.initializeStateStores();

//        restoreStateConsumer.assign(new ArrayList<>(task.checkpointedOffsets().keySet()));

//        byte[] serializedValue = Serdes.Int().Serializer.Serialize("", 1);
//        task.update(
//            globalTopicPartition,
//            singletonList(new ConsumeResult<>(globalTopicPartition.Topic,
//                                               globalTopicPartition.Partition,
//                                        50L,
//                                               serializedValue,
//                                               serializedValue))
//        );

//        time.sleep(config.GetLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG));
//        task.commit();

//        Dictionary<TopicPartition, long> checkpoint = new OffsetCheckpoint(
//            new File(stateDirectory.directoryForTask(taskId), StateManagerUtil.CHECKPOINT_FILE_NAME)
//        ).read();
//        Assert.Equal(checkpoint, (Collections.singletonMap(globalTopicPartition, 51L)));

//    }

//    [Fact]
//    public void shouldCloseStateMangerOnTaskCloseWhenCommitFailed() {// throws Exception
//        consumer.assign(Collections.singletonList(globalTopicPartition));
//        Dictionary<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
//        committedOffsets.put(new TopicPartition(globalTopicPartition.Topic, globalTopicPartition.Partition),
//                             new OffsetAndMetadata(100L));
//        consumer.commitSync(committedOffsets);

//        restoreStateConsumer.updatePartitions(
//            globalStoreName,
//            Collections.singletonList(new PartitionInfo(globalStoreName, 0, Node.noNode(), new Node[0], new Node[0]))
//        );

//        StreamsConfig config = createConfig(baseDir);
//        AtomicBoolean closedStateManager = new AtomicBoolean(false);
//        task = new StandbyTask(
//            taskId,
//            ktablePartitions,
//            ktableTopology,
//            consumer,
//            changelogReader,
//            config,
//            streamsMetrics,
//            stateDirectory
//        ) {

//            public void commit() {
//                throw new RuntimeException("KABOOM!");
//            }


//            void closeStateManager(bool clean) {// throws ProcessorStateException
//                closedStateManager.set(true);
//            }
//        };
//        task.initializeStateStores();
//        try {
//            task.close(true, false);
//            Assert.True(false, "should have thrown exception");
//        } catch (Exception e) {
//            // expected
//            task = null;
//        }
//        Assert.True(closedStateManager.Get());
//    }

//    private MetricName setupCloseTaskMetric() {
//        MetricName metricName = new MetricName("name", "group", "description", Collections.emptyMap());
//        Sensor sensor = streamsMetrics.threadLevelSensor("task-closed", Sensor.RecordingLevel.INFO);
//        sensor.Add(metricName, new CumulativeSum());
//        return metricName;
//    }

//    private void verifyCloseTaskMetric(double expected,
//                                       StreamsMetricsImpl streamsMetrics,
//                                       MetricName metricName) {
//        KafkaMetric metric = (KafkaMetric) streamsMetrics.metrics().Get(metricName);
//        double totalCloses = metric.measurable().measure(metric.config(), System.currentTimeMillis());
//        Assert.Equal(totalCloses, (expected));
//    }

//    [Fact]
//    public void shouldRecordTaskClosedMetricOnClose(){ //throws IOException
//        MetricName metricName = setupCloseTaskMetric();
//        StandbyTask task = new StandbyTask(
//            taskId,
//            ktablePartitions,
//            ktableTopology,
//            consumer,
//            changelogReader,
//            createConfig(baseDir),
//            streamsMetrics,
//            stateDirectory
//        );

//        bool clean = true;
//        bool isZombie = false;
//        task.close(clean, isZombie);

//        double expectedCloseTaskMetric = 1.0;
//        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);
//    }

//    [Fact]
//    public void shouldRecordTaskClosedMetricOnCloseSuspended(){ //throws IOException
//        MetricName metricName = setupCloseTaskMetric();
//        StandbyTask task = new StandbyTask(
//            taskId,
//            ktablePartitions,
//            ktableTopology,
//            consumer,
//            changelogReader,
//            createConfig(baseDir),
//            streamsMetrics,
//            stateDirectory
//        );

//        bool clean = true;
//        bool isZombie = false;
//        task.closeSuspended(clean, isZombie, new RuntimeException());

//        double expectedCloseTaskMetric = 1.0;
//        verifyCloseTaskMetric(expectedCloseTaskMetric, streamsMetrics, metricName);
//    }
//}
