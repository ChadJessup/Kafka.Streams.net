/*






 *

 *





 */









































































public class StreamTaskTest {

    private Serializer<int> intSerializer = Serdes.Int().Serializer;
    private Serializer<byte[]> bytesSerializer = Serdes.ByteArray().Serializer;
    private Deserializer<int> intDeserializer = Serdes.Int().deserializer();
    private readonly string topic1 = "topic1";
    private readonly string topic2 = "topic2";
    private TopicPartition partition1 = new TopicPartition(topic1, 1);
    private TopicPartition partition2 = new TopicPartition(topic2, 1);
    private HashSet<TopicPartition> partitions = Utils.mkSet(partition1, partition2);

    private MockSourceNode<int, int> source1 = new MockSourceNode<>(new string[]{topic1}, intDeserializer, intDeserializer);
    private MockSourceNode<int, int> source2 = new MockSourceNode<>(new string[]{topic2}, intDeserializer, intDeserializer);
    private MockSourceNode<int, int> source3 = new MockSourceNode<int, int>(new string[]{topic2}, intDeserializer, intDeserializer) {
        
        public void Process(int key, int value) {
            throw new RuntimeException("KABOOM!");
        }

        
        public void Close() {
            throw new RuntimeException("KABOOM!");
        }
    };
    private MockProcessorNode<int, int> processorStreamTime = new MockProcessorNode<>(10L);
    private MockProcessorNode<int, int> processorSystemTime = new MockProcessorNode<>(10L, PunctuationType.WALL_CLOCK_TIME);

    private string storeName = "store";
    private StateStore stateStore = new MockKeyValueStore(storeName, false);
    private TopicPartition changelogPartition = new TopicPartition("store-changelog", 0);
    private long offset = 543L;

    private ProcessorTopology topology = withSources(
        asList(source1, source2, processorStreamTime, processorSystemTime),
        mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2))
    );

    private MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private MockProducer<byte[], byte[]> producer;
    private MockConsumer<byte[], byte[]> restoreStateConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private StateRestoreListener stateRestoreListener = new MockStateRestoreListener();
    private StoreChangelogReader changelogReader = new StoreChangelogReader(restoreStateConsumer, Duration.ZERO, stateRestoreListener, new LogContext("stream-task-test ")) {
        
        public Dictionary<TopicPartition, long> RestoredOffsets() {
            return Collections.singletonMap(changelogPartition, offset);
        }
    };
    private byte[] recordValue = intSerializer.serialize(null, 10);
    private byte[] recordKey = intSerializer.serialize(null, 1);
    private Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG));
    private StreamsMetricsImpl streamsMetrics = new MockStreamsMetrics(metrics);
    private TaskId taskId00 = new TaskId(0, 0);
    private MockTime time = new MockTime();
    private File baseDir = TestUtils.tempDirectory();
    private StateDirectory stateDirectory;
    private StreamTask task;
    private long punctuatedAt;

    private Punctuator punctuator = new Punctuator() {
        
        public void Punctuate(long timestamp) {
            punctuatedAt = timestamp;
        }
    };

    static ProcessorTopology WithRepartitionTopics(List<ProcessorNode> processorNodes,
                                                   Dictionary<string, SourceNode> sourcesByTopic,
                                                   HashSet<string> repartitionTopics) {
        return new ProcessorTopology(processorNodes,
                                     sourcesByTopic,
                                     Collections.emptyMap(),
                                     Collections.emptyList(),
                                     Collections.emptyList(),
                                     Collections.emptyMap(),
                                     repartitionTopics);
    }

    static ProcessorTopology WithSources(List<ProcessorNode> processorNodes,
                                         Dictionary<string, SourceNode> sourcesByTopic) {
        return new ProcessorTopology(processorNodes,
                                     sourcesByTopic,
                                     Collections.emptyMap(),
                                     Collections.emptyList(),
                                     Collections.emptyList(),
                                     Collections.emptyMap(),
                                     Collections.emptySet());
    }

    private StreamsConfig CreateConfig(bool enableEoS) {
        string canonicalPath;
        try {
            canonicalPath = baseDir.getCanonicalPath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new StreamsConfig(mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "stream-task-test"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171"),
            mkEntry(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, canonicalPath),
            mkEntry(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.getName()),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, enableEoS ? StreamsConfig.EXACTLY_ONCE : StreamsConfig.AT_LEAST_ONCE),
            mkEntry(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, "100")
        )));
    }

    
    public void Setup() {
        consumer.assign(asList(partition1, partition2));
        stateDirectory = new StateDirectory(createConfig(false), new MockTime(), true);
    }

    
    public void Cleanup(){ //throws IOException
        try {
            if (task != null) {
                try {
                    task.close(true, false);
                } catch (Exception e) {
                    // swallow
                }
            }
        } finally {
            Utils.delete(baseDir);
        }
    }

    [Xunit.Fact]
    public void ShouldHandleInitTransactionsTimeoutExceptionOnCreation() {
        LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

        ProcessorTopology topology = withSources(
            asList(source1, source2, processorStreamTime, processorSystemTime),
            mkMap(mkEntry(topic1, (SourceNode) source1), mkEntry(topic2, (SourceNode) source2))
        );

        source1.addChild(processorStreamTime);
        source2.addChild(processorStreamTime);
        source1.addChild(processorSystemTime);
        source2.addChild(processorSystemTime);

        try {
            new StreamTask(
                taskId00,
                partitions,
                topology,
                consumer,
                changelogReader,
                createConfig(true),
                streamsMetrics,
                stateDirectory,
                null,
                time,
                () => producer = new MockProducer<byte[], byte[]>(false, bytesSerializer, bytesSerializer) {
                    
                    public void initTransactions() {
                        throw new TimeoutException("test");
                    }
                },
                null
            );
            Assert.True(false, "Expected an exception");
        } catch (StreamsException expected) {
            // make sure we log the explanation as an ERROR
            assertTimeoutErrorLog(appender);

            // make sure we report the correct message
            Assert.Equal(expected.getMessage(), ("task [0_0] Failed to initialize task 0_0 due to timeout."));

            // make sure we preserve the cause
            Assert.Equal(expected.getCause().getClass(), TimeoutException);
            Assert.Equal(expected.getCause().getMessage(), ("test"));
        }
        LogCaptureAppender.unregister(appender);
    }

    [Xunit.Fact]
    public void ShouldHandleInitTransactionsTimeoutExceptionOnResume() {
        LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

        ProcessorTopology topology = withSources(
            asList(source1, source2, processorStreamTime, processorSystemTime),
            mkMap(mkEntry(topic1, (SourceNode) source1), mkEntry(topic2, (SourceNode) source2))
        );

        source1.addChild(processorStreamTime);
        source2.addChild(processorStreamTime);
        source1.addChild(processorSystemTime);
        source2.addChild(processorSystemTime);

        AtomicBoolean timeOut = new AtomicBoolean(false);

        StreamTask testTask = new StreamTask(
            taskId00,
            partitions,
            topology,
            consumer,
            changelogReader,
            createConfig(true),
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () => producer = new MockProducer<byte[], byte[]>(false, bytesSerializer, bytesSerializer) {
                
                public void initTransactions() {
                    if (timeOut.get()) {
                        throw new TimeoutException("test");
                    } else {
                        base.initTransactions();
                    }
                }
            },
            null
        );
        testTask.initializeTopology();
        testTask.suspend();
        timeOut.set(true);
        try {
            testTask.resume();
            Assert.True(false, "Expected an exception");
        } catch (StreamsException expected) {
            // make sure we log the explanation as an ERROR
            assertTimeoutErrorLog(appender);

            // make sure we report the correct message
            Assert.Equal(expected.getMessage(), ("task [0_0] Failed to initialize task 0_0 due to timeout."));

            // make sure we preserve the cause
            Assert.Equal(expected.getCause().getClass(), TimeoutException);
            Assert.Equal(expected.getCause().getMessage(), ("test"));
        }
        LogCaptureAppender.unregister(appender);
    }

    private void AssertTimeoutErrorLog(LogCaptureAppender appender) {

        string expectedErrorLogMessage =
            "task [0_0] Timeout exception caught when initializing transactions for task 0_0. " +
                "This might happen if the broker is slow to respond, if the network " +
                "connection to the broker was interrupted, or if similar circumstances arise. " +
                "You can increase producer parameter `max.block.ms` to increase this timeout.";

        List<string> expectedError =
            appender
                .getEvents()
                .stream()
                .filter(event => event.getMessage().equals(expectedErrorLogMessage))
                .map(LogCaptureAppender.Event::getLevel)
                .collect(Collectors.toList());
        Assert.Equal(expectedError, (singletonList("ERROR")));
    }

    
    [Xunit.Fact]
    public void TestProcessOrder() {
        task = createStatelessTask(createConfig(false));

        task.addRecords(partition1, asList(
            getConsumerRecord(partition1, 10),
            getConsumerRecord(partition1, 20),
            getConsumerRecord(partition1, 30)
        ));

        task.addRecords(partition2, asList(
            getConsumerRecord(partition2, 25),
            getConsumerRecord(partition2, 35),
            getConsumerRecord(partition2, 45)
        ));

        Assert.True(task.process());
        Assert.Equal(5, task.numBuffered());
        Assert.Equal(1, source1.numReceived);
        Assert.Equal(0, source2.numReceived);

        Assert.True(task.process());
        Assert.Equal(4, task.numBuffered());
        Assert.Equal(2, source1.numReceived);
        Assert.Equal(0, source2.numReceived);

        Assert.True(task.process());
        Assert.Equal(3, task.numBuffered());
        Assert.Equal(2, source1.numReceived);
        Assert.Equal(1, source2.numReceived);

        Assert.True(task.process());
        Assert.Equal(2, task.numBuffered());
        Assert.Equal(3, source1.numReceived);
        Assert.Equal(1, source2.numReceived);

        Assert.True(task.process());
        Assert.Equal(1, task.numBuffered());
        Assert.Equal(3, source1.numReceived);
        Assert.Equal(2, source2.numReceived);

        Assert.True(task.process());
        Assert.Equal(0, task.numBuffered());
        Assert.Equal(3, source1.numReceived);
        Assert.Equal(3, source2.numReceived);
    }


    [Xunit.Fact]
    public void TestMetrics() {
        task = createStatelessTask(createConfig(false));

        assertNotNull(getMetric("%s-latency-avg", "The average latency of %s operation.", task.id().toString()));
        assertNotNull(getMetric("%s-latency-max", "The max latency of %s operation.", task.id().toString()));
        assertNotNull(getMetric("%s-rate", "The average number of occurrence of %s operation per second.", task.id().toString()));

        assertNotNull(getMetric("%s-latency-avg", "The average latency of %s operation.", "all"));
        assertNotNull(getMetric("%s-latency-max", "The max latency of %s operation.", "all"));
        assertNotNull(getMetric("%s-rate", "The average number of occurrence of %s operation per second.", "all"));

        JmxReporter reporter = new JmxReporter("kafka.streams");
        metrics.addReporter(reporter);
        Assert.True(reporter.containsMbean(string.format("kafka.streams:type=stream-task-metrics,client-id=test,task-id=%s", task.id.toString())));
        Assert.True(reporter.containsMbean("kafka.streams:type=stream-task-metrics,client-id=test,task-id=all"));
    }

    private KafkaMetric GetMetric(string nameFormat, string descriptionFormat, string taskId) {
        return metrics.metrics().get(metrics.metricName(
            string.format(nameFormat, "commit"),
            "stream-task-metrics",
            string.format(descriptionFormat, "commit"),
            mkMap(mkEntry("task-id", taskId), mkEntry("client-id", "test"))
        ));
    }

    
    [Xunit.Fact]
    public void TestPauseResume() {
        task = createStatelessTask(createConfig(false));

        task.addRecords(partition1, asList(
            getConsumerRecord(partition1, 10),
            getConsumerRecord(partition1, 20)
        ));

        task.addRecords(partition2, asList(
            getConsumerRecord(partition2, 35),
            getConsumerRecord(partition2, 45),
            getConsumerRecord(partition2, 55),
            getConsumerRecord(partition2, 65)
        ));

        Assert.True(task.process());
        Assert.Equal(1, source1.numReceived);
        Assert.Equal(0, source2.numReceived);

        Assert.Equal(1, consumer.paused().Count);
        Assert.True(consumer.paused().Contains(partition2));

        task.addRecords(partition1, asList(
            getConsumerRecord(partition1, 30),
            getConsumerRecord(partition1, 40),
            getConsumerRecord(partition1, 50)
        ));

        Assert.Equal(2, consumer.paused().Count);
        Assert.True(consumer.paused().Contains(partition1));
        Assert.True(consumer.paused().Contains(partition2));

        Assert.True(task.process());
        Assert.Equal(2, source1.numReceived);
        Assert.Equal(0, source2.numReceived);

        Assert.Equal(1, consumer.paused().Count);
        Assert.True(consumer.paused().Contains(partition2));

        Assert.True(task.process());
        Assert.Equal(3, source1.numReceived);
        Assert.Equal(0, source2.numReceived);

        Assert.Equal(1, consumer.paused().Count);
        Assert.True(consumer.paused().Contains(partition2));

        Assert.True(task.process());
        Assert.Equal(3, source1.numReceived);
        Assert.Equal(1, source2.numReceived);

        Assert.Equal(0, consumer.paused().Count);
    }

    
    [Xunit.Fact]
    public void ShouldPunctuateOnceStreamTimeAfterGap() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();

        task.addRecords(partition1, asList(
            getConsumerRecord(partition1, 20),
            getConsumerRecord(partition1, 142),
            getConsumerRecord(partition1, 155),
            getConsumerRecord(partition1, 160)
        ));

        task.addRecords(partition2, asList(
            getConsumerRecord(partition2, 25),
            getConsumerRecord(partition2, 145),
            getConsumerRecord(partition2, 159),
            getConsumerRecord(partition2, 161)
        ));

        // st: -1
        Assert.False(task.maybePunctuateStreamTime()); // punctuate at 20

        // st: 20
        Assert.True(task.process());
        Assert.Equal(7, task.numBuffered());
        Assert.Equal(1, source1.numReceived);
        Assert.Equal(0, source2.numReceived);
        Assert.True(task.maybePunctuateStreamTime());

        // st: 25
        Assert.True(task.process());
        Assert.Equal(6, task.numBuffered());
        Assert.Equal(1, source1.numReceived);
        Assert.Equal(1, source2.numReceived);
        Assert.False(task.maybePunctuateStreamTime());

        // st: 142
        // punctuate at 142
        Assert.True(task.process());
        Assert.Equal(5, task.numBuffered());
        Assert.Equal(2, source1.numReceived);
        Assert.Equal(1, source2.numReceived);
        Assert.True(task.maybePunctuateStreamTime());

        // st: 145
        // only one punctuation after 100ms gap
        Assert.True(task.process());
        Assert.Equal(4, task.numBuffered());
        Assert.Equal(2, source1.numReceived);
        Assert.Equal(2, source2.numReceived);
        Assert.False(task.maybePunctuateStreamTime());

        // st: 155
        // punctuate at 155
        Assert.True(task.process());
        Assert.Equal(3, task.numBuffered());
        Assert.Equal(3, source1.numReceived);
        Assert.Equal(2, source2.numReceived);
        Assert.True(task.maybePunctuateStreamTime());

        // st: 159
        Assert.True(task.process());
        Assert.Equal(2, task.numBuffered());
        Assert.Equal(3, source1.numReceived);
        Assert.Equal(3, source2.numReceived);
        Assert.False(task.maybePunctuateStreamTime());

        // st: 160, aligned at 0
        Assert.True(task.process());
        Assert.Equal(1, task.numBuffered());
        Assert.Equal(4, source1.numReceived);
        Assert.Equal(3, source2.numReceived);
        Assert.True(task.maybePunctuateStreamTime());

        // st: 161
        Assert.True(task.process());
        Assert.Equal(0, task.numBuffered());
        Assert.Equal(4, source1.numReceived);
        Assert.Equal(4, source2.numReceived);
        Assert.False(task.maybePunctuateStreamTime());

        processorStreamTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.STREAM_TIME, 20L, 142L, 155L, 160L);
    }

    [Xunit.Fact]
    public void ShouldRespectPunctuateCancellationStreamTime() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();

        task.addRecords(partition1, asList(
            getConsumerRecord(partition1, 20),
            getConsumerRecord(partition1, 30),
            getConsumerRecord(partition1, 40)
        ));

        task.addRecords(partition2, asList(
            getConsumerRecord(partition2, 25),
            getConsumerRecord(partition2, 35),
            getConsumerRecord(partition2, 45)
        ));

        Assert.False(task.maybePunctuateStreamTime());

        // st is now 20
        Assert.True(task.process());

        Assert.True(task.maybePunctuateStreamTime());

        // st is now 25
        Assert.True(task.process());

        Assert.False(task.maybePunctuateStreamTime());

        // st is now 30
        Assert.True(task.process());

        processorStreamTime.mockProcessor.scheduleCancellable.cancel();

        Assert.False(task.maybePunctuateStreamTime());

        processorStreamTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.STREAM_TIME, 20L);
    }

    [Xunit.Fact]
    public void ShouldRespectPunctuateCancellationSystemTime() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();
        long now = time.milliseconds();
        time.sleep(10);
        Assert.True(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.scheduleCancellable.cancel();
        time.sleep(10);
        Assert.False(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 10);
    }

    [Xunit.Fact]
    public void ShouldRespectCommitNeeded() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();

        Assert.False(task.commitNeeded());

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        Assert.True(task.process());
        Assert.True(task.commitNeeded());

        task.commit();
        Assert.False(task.commitNeeded());

        Assert.True(task.maybePunctuateStreamTime());
        Assert.True(task.commitNeeded());

        task.commit();
        Assert.False(task.commitNeeded());

        time.sleep(10);
        Assert.True(task.maybePunctuateSystemTime());
        Assert.True(task.commitNeeded());

        task.commit();
        Assert.False(task.commitNeeded());
    }

    [Xunit.Fact]
    public void ShouldRespectCommitRequested() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();

        task.requestCommit();
        Assert.True(task.commitRequested());
    }

    [Xunit.Fact]
    public void ShouldBeProcessableIfAllPartitionsBuffered() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();

        Assert.False(task.isProcessable(0L));

        byte[] bytes = ByteBuffer.allocate(4).putInt(1).array();

        task.addRecords(partition1, Collections.singleton(new ConsumeResult<>(topic1, 1, 0, bytes, bytes)));

        Assert.False(task.isProcessable(0L));

        task.addRecords(partition2, Collections.singleton(new ConsumeResult<>(topic2, 1, 0, bytes, bytes)));

        Assert.True(task.isProcessable(0L));
    }

    [Xunit.Fact]
    public void ShouldBeProcessableIfWaitedForTooLong() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();

        MetricName enforcedProcessMetric = metrics.metricName("enforced-processing-total", "stream-task-metrics", mkMap(mkEntry("client-id", "test"), mkEntry("task-id", taskId00.toString())));

        Assert.False(task.isProcessable(0L));
        Assert.Equal(0.0, metrics.metric(enforcedProcessMetric).metricValue());

        byte[] bytes = ByteBuffer.allocate(4).putInt(1).array();

        task.addRecords(partition1, Collections.singleton(new ConsumeResult<>(topic1, 1, 0, bytes, bytes)));

        Assert.False(task.isProcessable(time.milliseconds()));

        Assert.False(task.isProcessable(time.milliseconds() + 50L));

        Assert.True(task.isProcessable(time.milliseconds() + 100L));
        Assert.Equal(1.0, metrics.metric(enforcedProcessMetric).metricValue());

        // once decided to enforce, continue doing that
        Assert.True(task.isProcessable(time.milliseconds() + 101L));
        Assert.Equal(2.0, metrics.metric(enforcedProcessMetric).metricValue());

        task.addRecords(partition2, Collections.singleton(new ConsumeResult<>(topic2, 1, 0, bytes, bytes)));

        Assert.True(task.isProcessable(time.milliseconds() + 130L));
        Assert.Equal(2.0, metrics.metric(enforcedProcessMetric).metricValue());

        // one resumed to normal processing, the timer should be reset
        task.process();

        Assert.False(task.isProcessable(time.milliseconds() + 150L));
        Assert.Equal(2.0, metrics.metric(enforcedProcessMetric).metricValue());

        Assert.False(task.isProcessable(time.milliseconds() + 249L));
        Assert.Equal(2.0, metrics.metric(enforcedProcessMetric).metricValue());

        Assert.True(task.isProcessable(time.milliseconds() + 250L));
        Assert.Equal(3.0, metrics.metric(enforcedProcessMetric).metricValue());
    }


    [Xunit.Fact]
    public void ShouldPunctuateSystemTimeWhenIntervalElapsed() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();
        long now = time.milliseconds();
        time.sleep(10);
        Assert.True(task.maybePunctuateSystemTime());
        time.sleep(10);
        Assert.True(task.maybePunctuateSystemTime());
        time.sleep(9);
        Assert.False(task.maybePunctuateSystemTime());
        time.sleep(1);
        Assert.True(task.maybePunctuateSystemTime());
        time.sleep(20);
        Assert.True(task.maybePunctuateSystemTime());
        Assert.False(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 10, now + 20, now + 30, now + 50);
    }

    [Xunit.Fact]
    public void ShouldNotPunctuateSystemTimeWhenIntervalNotElapsed() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();
        Assert.False(task.maybePunctuateSystemTime());
        time.sleep(9);
        Assert.False(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME);
    }

    [Xunit.Fact]
    public void ShouldPunctuateOnceSystemTimeAfterGap() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();
        long now = time.milliseconds();
        time.sleep(100);
        Assert.True(task.maybePunctuateSystemTime());
        Assert.False(task.maybePunctuateSystemTime());
        time.sleep(10);
        Assert.True(task.maybePunctuateSystemTime());
        time.sleep(12);
        Assert.True(task.maybePunctuateSystemTime());
        time.sleep(7);
        Assert.False(task.maybePunctuateSystemTime());
        time.sleep(1); // punctuate at now + 130
        Assert.True(task.maybePunctuateSystemTime());
        time.sleep(105); // punctuate at now + 235
        Assert.True(task.maybePunctuateSystemTime());
        Assert.False(task.maybePunctuateSystemTime());
        time.sleep(5); // punctuate at now + 240, still aligned on the initial punctuation
        Assert.True(task.maybePunctuateSystemTime());
        Assert.False(task.maybePunctuateSystemTime());
        processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 100, now + 110, now + 122, now + 130, now + 235, now + 240);
    }

    [Xunit.Fact]
    public void ShouldWrapKafkaExceptionsWithStreamsExceptionAndAddContext() {
        task = createTaskThatThrowsException(false);
        task.initializeStateStores();
        task.initializeTopology();
        task.addRecords(partition2, singletonList(getConsumerRecord(partition2, 0)));

        try {
            task.process();
            Assert.True(false, "Should've thrown StreamsException");
        } catch (Exception e) {
            Assert.Equal(task.processorContext.currentNode(), nullValue());
        }
    }

    [Xunit.Fact]
    public void ShouldWrapKafkaExceptionsWithStreamsExceptionAndAddContextWhenPunctuatingStreamTime() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();

        try {
            task.punctuate(processorStreamTime, 1, PunctuationType.STREAM_TIME, new Punctuator() {
                
                public void punctuate(long timestamp) {
                    throw new KafkaException("KABOOM!");
                }
            });
            Assert.True(false, "Should've thrown StreamsException");
        } catch (StreamsException e) {
            string message = e.getMessage();
            Assert.True("message=" + message + " should contain processor", message.Contains("processor '" + processorStreamTime.name() + "'"));
            Assert.Equal(task.processorContext.currentNode(), nullValue());
        }
    }

    [Xunit.Fact]
    public void ShouldWrapKafkaExceptionsWithStreamsExceptionAndAddContextWhenPunctuatingWallClockTimeTime() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();

        try {
            task.punctuate(processorSystemTime, 1, PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
                
                public void punctuate(long timestamp) {
                    throw new KafkaException("KABOOM!");
                }
            });
            Assert.True(false, "Should've thrown StreamsException");
        } catch (StreamsException e) {
            string message = e.getMessage();
            Assert.True("message=" + message + " should contain processor", message.Contains("processor '" + processorSystemTime.name() + "'"));
            Assert.Equal(task.processorContext.currentNode(), nullValue());
        }
    }

    [Xunit.Fact]
    public void ShouldFlushRecordCollectorOnFlushState() {
        AtomicBoolean flushed = new AtomicBoolean(false);
        StreamsMetricsImpl streamsMetrics = new MockStreamsMetrics(new Metrics());
        StreamTask streamTask = new StreamTask(
            taskId00,
            partitions,
            topology,
            consumer,
            changelogReader,
            createConfig(false),
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () => producer = new MockProducer<>(false, bytesSerializer, bytesSerializer),
            new NoOpRecordCollector() {
                
                public void flush() {
                    flushed.set(true);
                }
            });
        streamTask.flushState();
        Assert.True(flushed.get());
    }

    [Xunit.Fact]
    public void ShouldCheckpointOffsetsOnCommit(){ //throws IOException
        task = createStatefulTask(createConfig(false), true);
        task.initializeStateStores();
        task.initializeTopology();
        task.commit();
        OffsetCheckpoint checkpoint = new OffsetCheckpoint(
            new File(stateDirectory.directoryForTask(taskId00), StateManagerUtil.CHECKPOINT_FILE_NAME)
        );

        Assert.Equal(checkpoint.read(), (Collections.singletonMap(changelogPartition, offset)));
    }

    [Xunit.Fact]
    public void ShouldNotCheckpointOffsetsOnCommitIfEosIsEnabled() {
        task = createStatefulTask(createConfig(true), true);
        task.initializeStateStores();
        task.initializeTopology();
        task.commit();
        File checkpointFile = new File(
            stateDirectory.directoryForTask(taskId00),
            StateManagerUtil.CHECKPOINT_FILE_NAME
        );

        Assert.False(checkpointFile.exists());
    }

    [Xunit.Fact]
    public void ShouldThrowIllegalStateExceptionIfCurrentNodeIsNotNullWhenPunctuateCalled() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();
        task.processorContext.setCurrentNode(processorStreamTime);
        try {
            task.punctuate(processorStreamTime, 10, PunctuationType.STREAM_TIME, punctuator);
            Assert.True(false, "Should throw illegal state exception as current node is not null");
        } catch (IllegalStateException e) {
            // pass
        }
    }

    [Xunit.Fact]
    public void ShouldCallPunctuateOnPassedInProcessorNode() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();
        task.punctuate(processorStreamTime, 5, PunctuationType.STREAM_TIME, punctuator);
        Assert.Equal(punctuatedAt, (5L));
        task.punctuate(processorStreamTime, 10, PunctuationType.STREAM_TIME, punctuator);
        Assert.Equal(punctuatedAt, (10L));
    }

    [Xunit.Fact]
    public void ShouldSetProcessorNodeOnContextBackToNullAfterSuccessfulPunctuate() {
        task = createStatelessTask(createConfig(false));
        task.initializeStateStores();
        task.initializeTopology();
        task.punctuate(processorStreamTime, 5, PunctuationType.STREAM_TIME, punctuator);
        Assert.Equal(((ProcessorContextImpl) task.context()).currentNode(), nullValue());
    }

    [Xunit.Fact]// (expected = IllegalStateException)
    public void ShouldThrowIllegalStateExceptionOnScheduleIfCurrentNodeIsNull() {
        task = createStatelessTask(createConfig(false));
        task.schedule(1, PunctuationType.STREAM_TIME, new Punctuator() {
            
            public void punctuate(long timestamp) {
                // no-op
            }
        });
    }

    [Xunit.Fact]
    public void ShouldNotThrowExceptionOnScheduleIfCurrentNodeIsNotNull() {
        task = createStatelessTask(createConfig(false));
        task.processorContext.setCurrentNode(processorStreamTime);
        task.schedule(1, PunctuationType.STREAM_TIME, new Punctuator() {
            
            public void punctuate(long timestamp) {
                // no-op
            }
        });
    }

    [Xunit.Fact]
    public void ShouldNotCloseProducerOnCleanCloseWithEosDisabled() {
        task = createStatelessTask(createConfig(false));
        task.close(true, false);
        task = null;

        Assert.False(producer.closed());
    }

    [Xunit.Fact]
    public void ShouldNotCloseProducerOnUncleanCloseWithEosDisabled() {
        task = createStatelessTask(createConfig(false));
        task.close(false, false);
        task = null;

        Assert.False(producer.closed());
    }

    [Xunit.Fact]
    public void ShouldNotCloseProducerOnErrorDuringCleanCloseWithEosDisabled() {
        task = createTaskThatThrowsException(false);

        try {
            task.close(true, false);
            Assert.True(false, "should have thrown runtime exception");
        } catch (RuntimeException expected) {
            task = null;
        }

        Assert.False(producer.closed());
    }

    [Xunit.Fact]
    public void ShouldNotCloseProducerOnErrorDuringUncleanCloseWithEosDisabled() {
        task = createTaskThatThrowsException(false);

        task.close(false, false);
        task = null;

        Assert.False(producer.closed());
    }

    [Xunit.Fact]
    public void ShouldCommitTransactionAndCloseProducerOnCleanCloseWithEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();

        task.close(true, false);
        task = null;

        Assert.True(producer.transactionCommitted());
        Assert.False(producer.transactionInFlight());
        Assert.True(producer.closed());
    }

    [Xunit.Fact]
    public void ShouldNotAbortTransactionAndNotCloseProducerOnErrorDuringCleanCloseWithEosEnabled() {
        task = createTaskThatThrowsException(true);
        task.initializeTopology();

        try {
            task.close(true, false);
            Assert.True(false, "should have thrown runtime exception");
        } catch (RuntimeException expected) {
            task = null;
        }

        Assert.True(producer.transactionInFlight());
        Assert.False(producer.closed());
    }

    [Xunit.Fact]
    public void ShouldOnlyCloseProducerIfFencedOnCommitDuringCleanCloseWithEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();
        producer.fenceProducer();

        try {
            task.close(true, false);
            Assert.True(false, "should have thrown TaskMigratedException");
        } catch (TaskMigratedException expected) {
            task = null;
            Assert.True(expected.getCause() is ProducerFencedException);
        }

        Assert.False(producer.transactionCommitted());
        Assert.True(producer.transactionInFlight());
        Assert.False(producer.transactionAborted());
        Assert.False(producer.transactionCommitted());
        Assert.True(producer.closed());
    }

    [Xunit.Fact]
    public void ShouldNotCloseProducerIfFencedOnCloseDuringCleanCloseWithEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();
        producer.fenceProducerOnClose();

        try {
            task.close(true, false);
            Assert.True(false, "should have thrown TaskMigratedException");
        } catch (TaskMigratedException expected) {
            task = null;
            Assert.True(expected.getCause() is ProducerFencedException);
        }

        Assert.True(producer.transactionCommitted());
        Assert.False(producer.transactionInFlight());
        Assert.False(producer.closed());
    }

    [Xunit.Fact]
    public void ShouldAbortTransactionAndCloseProducerOnUncleanCloseWithEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();

        task.close(false, false);
        task = null;

        Assert.True(producer.transactionAborted());
        Assert.False(producer.transactionInFlight());
        Assert.True(producer.closed());
    }

    [Xunit.Fact]
    public void ShouldAbortTransactionAndCloseProducerOnErrorDuringUncleanCloseWithEosEnabled() {
        task = createTaskThatThrowsException(true);
        task.initializeTopology();

        task.close(false, false);

        Assert.True(producer.transactionAborted());
        Assert.True(producer.closed());
    }

    [Xunit.Fact]
    public void ShouldOnlyCloseProducerIfFencedOnAbortDuringUncleanCloseWithEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();
        producer.fenceProducer();

        task.close(false, false);
        task = null;

        Assert.True(producer.transactionInFlight());
        Assert.False(producer.transactionAborted());
        Assert.False(producer.transactionCommitted());
        Assert.True(producer.closed());
    }

    [Xunit.Fact]
    public void ShouldOnlyCloseFencedProducerOnUncleanClosedWithEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();
        producer.fenceProducer();

        task.close(false, true);
        task = null;

        Assert.False(producer.transactionAborted());
        Assert.True(producer.closed());
    }

    [Xunit.Fact]
    public void ShouldAbortTransactionButNotCloseProducerIfFencedOnCloseDuringUncleanCloseWithEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();
        producer.fenceProducerOnClose();

        task.close(false, false);
        task = null;

        Assert.True(producer.transactionAborted());
        Assert.False(producer.closed());
    }

    [Xunit.Fact]
    public void ShouldThrowExceptionIfAnyExceptionsRaisedDuringCloseButStillCloseAllProcessorNodesTopology() {
        task = createTaskThatThrowsException(false);
        task.initializeStateStores();
        task.initializeTopology();
        try {
            task.close(true, false);
            Assert.True(false, "should have thrown runtime exception");
        } catch (RuntimeException expected) {
            task = null;
        }
        Assert.True(processorSystemTime.closed);
        Assert.True(processorStreamTime.closed);
        Assert.True(source1.closed);
    }

    [Xunit.Fact]
    public void ShouldInitAndBeginTransactionOnCreateIfEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();

        Assert.True(producer.transactionInitialized());
        Assert.True(producer.transactionInFlight());
    }

    [Xunit.Fact]
    public void ShouldWrapProducerFencedExceptionWithTaskMigratedExceptionForBeginTransaction() {
        task = createStatelessTask(createConfig(true));
        producer.fenceProducer();

        try {
            task.initializeTopology();
            Assert.True(false, "Should have throws TaskMigratedException");
        } catch (TaskMigratedException expected) {
            Assert.True(expected.getCause() is ProducerFencedException);
        }
    }

    [Xunit.Fact]
    public void ShouldNotThrowOnCloseIfTaskWasNotInitializedWithEosEnabled() {
        task = createStatelessTask(createConfig(true));

        Assert.False(producer.transactionInFlight());
        task.close(false, false);
    }

    [Xunit.Fact]
    public void ShouldNotInitOrBeginTransactionOnCreateIfEosDisabled() {
        task = createStatelessTask(createConfig(false));

        Assert.False(producer.transactionInitialized());
        Assert.False(producer.transactionInFlight());
    }

    [Xunit.Fact]
    public void ShouldSendOffsetsAndCommitTransactionButNotStartNewTransactionOnSuspendIfEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process();

        task.suspend();
        Assert.True(producer.sentOffsets());
        Assert.True(producer.transactionCommitted());
        Assert.False(producer.transactionInFlight());
    }

    [Xunit.Fact]
    public void ShouldCommitTransactionOnSuspendEvenIfTransactionIsEmptyIfEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();
        task.suspend();

        Assert.True(producer.transactionCommitted());
        Assert.False(producer.transactionInFlight());
    }

    [Xunit.Fact]
    public void ShouldNotSendOffsetsAndCommitTransactionNorStartNewTransactionOnSuspendIfEosDisabled() {
        task = createStatelessTask(createConfig(false));
        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process();
        task.suspend();

        Assert.False(producer.sentOffsets());
        Assert.False(producer.transactionCommitted());
        Assert.False(producer.transactionInFlight());
    }

    [Xunit.Fact]
    public void ShouldWrapProducerFencedExceptionWithTaskMigragedExceptionInSuspendWhenCommitting() {
        task = createStatelessTask(createConfig(true));
        producer.fenceProducer();

        try {
            task.suspend();
            Assert.True(false, "Should have throws TaskMigratedException");
        } catch (TaskMigratedException expected) {
            Assert.True(expected.getCause() is ProducerFencedException);
        }
        task = null;

        Assert.False(producer.transactionCommitted());
    }

    [Xunit.Fact]
    public void ShouldWrapProducerFencedExceptionWithTaskMigragedExceptionInSuspendWhenClosingProducer() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();

        producer.fenceProducerOnClose();
        try {
            task.suspend();
            Assert.True(false, "Should have throws TaskMigratedException");
        } catch (TaskMigratedException expected) {
            Assert.True(expected.getCause() is ProducerFencedException);
        }

        Assert.True(producer.transactionCommitted());
    }

    [Xunit.Fact]
    public void ShouldStartNewTransactionOnResumeIfEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process();
        task.suspend();

        task.resume();
        task.initializeTopology();
        Assert.True(producer.transactionInFlight());
    }

    [Xunit.Fact]
    public void ShouldNotStartNewTransactionOnResumeIfEosDisabled() {
        task = createStatelessTask(createConfig(false));

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process();
        task.suspend();

        task.resume();
        Assert.False(producer.transactionInFlight());
    }

    [Xunit.Fact]
    public void ShouldStartNewTransactionOnCommitIfEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process();

        task.commit();
        Assert.True(producer.transactionInFlight());
    }

    [Xunit.Fact]
    public void ShouldNotStartNewTransactionOnCommitIfEosDisabled() {
        task = createStatelessTask(createConfig(false));

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 0)));
        task.process();

        task.commit();
        Assert.False(producer.transactionInFlight());
    }

    [Xunit.Fact]
    public void ShouldNotAbortTransactionOnZombieClosedIfEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.close(false, true);
        task = null;

        Assert.False(producer.transactionAborted());
    }

    [Xunit.Fact]
    public void ShouldNotAbortTransactionOnDirtyClosedIfEosDisabled() {
        task = createStatelessTask(createConfig(false));
        task.close(false, false);
        task = null;

        Assert.False(producer.transactionAborted());
    }

    [Xunit.Fact]
    public void ShouldCloseProducerOnCloseWhenEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();
        task.close(true, false);
        task = null;

        Assert.True(producer.closed());
    }

    [Xunit.Fact]
    public void ShouldCloseProducerOnUncleanCloseNotZombieWhenEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();
        task.close(false, false);
        task = null;

        Assert.True(producer.closed());
    }

    [Xunit.Fact]
    public void ShouldCloseProducerOnUncleanCloseIsZombieWhenEosEnabled() {
        task = createStatelessTask(createConfig(true));
        task.initializeTopology();
        task.close(false, true);
        task = null;

        Assert.True(producer.closed());
    }

    [Xunit.Fact]
    public void ShouldNotViolateAtLeastOnceWhenExceptionOccursDuringFlushing() {
        task = createTaskThatThrowsException(false);
        task.initializeStateStores();
        task.initializeTopology();

        try {
            task.commit();
            Assert.True(false, "should have thrown an exception");
        } catch (Exception e) {
            // all good
        }
    }

    [Xunit.Fact]
    public void ShouldNotViolateAtLeastOnceWhenExceptionOccursDuringTaskSuspension() {
        StreamTask task = createTaskThatThrowsException(false);

        task.initializeStateStores();
        task.initializeTopology();
        try {
            task.suspend();
            Assert.True(false, "should have thrown an exception");
        } catch (Exception e) {
            // all good
        }
    }

    [Xunit.Fact]
    public void ShouldCloseStateManagerIfFailureOnTaskClose() {
        task = createStatefulTaskThatThrowsExceptionOnClose();
        task.initializeStateStores();
        task.initializeTopology();

        try {
            task.close(true, false);
            Assert.True(false, "should have thrown an exception");
        } catch (Exception e) {
            // all good
        }

        task = null;
        Assert.False(stateStore.isOpen());
    }

    [Xunit.Fact]
    public void ShouldNotCloseTopologyProcessorNodesIfNotInitialized() {
        StreamTask task = createTaskThatThrowsException(false);
        try {
            task.close(false, false);
        } catch (Exception e) {
            Assert.True(false, "should have not closed non-initialized topology");
        }
    }

    [Xunit.Fact]
    public void ShouldBeInitializedIfChangelogPartitionsIsEmpty() {
        StreamTask task = createStatefulTask(createConfig(false), false);

        Assert.True(task.initializeStateStores());
    }

    [Xunit.Fact]
    public void ShouldNotBeInitializedIfChangelogPartitionsIsNonEmpty() {
        StreamTask task = createStatefulTask(createConfig(false), true);

        Assert.False(task.initializeStateStores());
    }

    [Xunit.Fact]
    public void ShouldReturnOffsetsForRepartitionTopicsForPurging() {
        TopicPartition repartition = new TopicPartition("repartition", 1);

        ProcessorTopology topology = withRepartitionTopics(
            asList(source1, source2),
            mkMap(mkEntry(topic1, source1), mkEntry(repartition.topic(), source2)),
            Collections.singleton(repartition.topic())
        );
        consumer.assign(asList(partition1, repartition));

        task = new StreamTask(
            taskId00,
            Utils.mkSet(partition1, repartition),
            topology,
            consumer,
            changelogReader,
            createConfig(false),
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () => producer = new MockProducer<>(false, bytesSerializer, bytesSerializer));
        task.initializeStateStores();
        task.initializeTopology();

        task.addRecords(partition1, singletonList(getConsumerRecord(partition1, 5L)));
        task.addRecords(repartition, singletonList(getConsumerRecord(repartition, 10L)));

        Assert.True(task.process());
        Assert.True(task.process());

        task.commit();

        Dictionary<TopicPartition, long> map = task.purgableOffsets();

        Assert.Equal(map, (Collections.singletonMap(repartition, 11L)));
    }

    [Xunit.Fact]
    public void ShouldThrowOnCleanCloseTaskWhenEosEnabledIfTransactionInFlight() {
        task = createStatelessTask(createConfig(true));
        try {
            task.close(true, false);
            Assert.True(false, "should have throw IllegalStateException");
        } catch (IllegalStateException expected) {
            // pass
        }
        task = null;

        Assert.True(producer.closed());
    }

    [Xunit.Fact]
    public void ShouldAlwaysCommitIfEosEnabled() {
        task = createStatelessTask(createConfig(true));

        RecordCollectorImpl recordCollector =  new RecordCollectorImpl("StreamTask",
                new LogContext("StreamTaskTest "), new DefaultProductionExceptionHandler(), new Metrics().sensor("skipped-records"));
        recordCollector.init(producer);

        task.initializeStateStores();
        task.initializeTopology();
        task.punctuate(processorSystemTime, 5, PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
            
            public void punctuate(long timestamp) {
                recordCollector.send("result-topic1", 3, 5, null, 0, time.milliseconds(),
                        new IntegerSerializer(),  new IntegerSerializer());
            }
        });
        task.commit();
        Assert.Equal(1, producer.history().Count);
    }

    private StreamTask CreateStatefulTask(StreamsConfig config, bool logged) {
        StateStore stateStore = new MockKeyValueStore(storeName, logged);

        ProcessorTopology topology = ProcessorTopologyFactories.with(
            asList(source1, source2),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2)),
            singletonList(stateStore),
            logged ? Collections.singletonMap(storeName, storeName + "-changelog") : Collections.emptyMap());

        return new StreamTask(
            taskId00,
            partitions,
            topology,
            consumer,
            changelogReader,
            config,
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () => producer = new MockProducer<>(false, bytesSerializer, bytesSerializer));
    }

    private StreamTask CreateStatefulTaskThatThrowsExceptionOnClose() {
        ProcessorTopology topology = ProcessorTopologyFactories.with(
            asList(source1, source3),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source3)),
            singletonList(stateStore),
            Collections.emptyMap());

        return new StreamTask(
            taskId00,
            partitions,
            topology,
            consumer,
            changelogReader,
            createConfig(true),
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () => producer = new MockProducer<>(false, bytesSerializer, bytesSerializer));
    }

    private StreamTask CreateStatelessTask(StreamsConfig streamsConfig) {
        ProcessorTopology topology = withSources(
            asList(source1, source2, processorStreamTime, processorSystemTime),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2))
        );

        source1.addChild(processorStreamTime);
        source2.addChild(processorStreamTime);
        source1.addChild(processorSystemTime);
        source2.addChild(processorSystemTime);

        return new StreamTask(
            taskId00,
            partitions,
            topology,
            consumer,
            changelogReader,
            streamsConfig,
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () => producer = new MockProducer<>(false, bytesSerializer, bytesSerializer));
    }

    // this task will throw exception when processing (on partition2), flushing, suspending and closing
    private StreamTask CreateTaskThatThrowsException(bool enableEos) {
        ProcessorTopology topology = withSources(
            asList(source1, source3, processorStreamTime, processorSystemTime),
            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source3))
        );

        source1.addChild(processorStreamTime);
        source3.addChild(processorStreamTime);
        source1.addChild(processorSystemTime);
        source3.addChild(processorSystemTime);

        return new StreamTask(
            taskId00,
            partitions,
            topology,
            consumer,
            changelogReader,
            createConfig(enableEos),
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () => producer = new MockProducer<>(false, bytesSerializer, bytesSerializer)) {
            
            protected void flushState() {
                throw new RuntimeException("KABOOM!");
            }
        };
    }

    private ConsumeResult<byte[], byte[]> GetConsumerRecord(TopicPartition topicPartition, long offset) {
        return new ConsumeResult<>(
            topicPartition.topic(),
            topicPartition.partition(),
            offset,
            offset, // use the offset as the timestamp
            TimestampType.CreateTime,
            0L,
            0,
            0,
            recordKey,
            recordValue
        );
    }
}
