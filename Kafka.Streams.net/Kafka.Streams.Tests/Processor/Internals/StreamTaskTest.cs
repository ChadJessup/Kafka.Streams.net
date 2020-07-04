//using Confluent.Kafka;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Errors;
//using Kafka.Streams.KStream;
//using Kafka.Streams.Nodes;
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State;
//using Kafka.Streams.State.Interfaces;
//using Kafka.Streams.Tasks;
//using Kafka.Streams.Temporary;
//using Kafka.Streams.Tests.Helpers;
//using Kafka.Streams.Tests.Mocks;
//using System;
//using System.Collections.Generic;
//using System.IO;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    public class StreamTaskTest
//    {
//        private ISerializer<int> intSerializer = Serdes.Int().Serializer;
//        private ISerializer<byte[]> bytesSerializer = Serdes.ByteArray().Serializer;
//        private IDeserializer<int> intDeserializer = Serdes.Int().Deserializer;
//        private readonly string topic1 = "topic1";
//        private readonly string topic2 = "topic2";
//        private TopicPartition partition1 = new TopicPartition(topic1, 1);
//        private TopicPartition partition2 = new TopicPartition(topic2, 1);
//        private HashSet<TopicPartition> partitions = Utils.mkSet(partition1, partition2);

//        private MockSourceNode<int, int> source1 = new MockSourceNode<int, int>(new string[] { topic1 }, intDeserializer, intDeserializer);
//        private MockSourceNode<int, int> source2 = new MockSourceNode<int, int>(new string[] { topic2 }, intDeserializer, intDeserializer);
//        //        private MockSourceNode<int, int> source3 = new MockSourceNode<int, int>(new string[] { topic2 }, intDeserializer, intDeserializer)
//        //        {
//        //
//        //
//        //        public void Process(int key, int value)
//        //        {
//        //            throw new RuntimeException("KABOOM!");
//        //        }
//        //
//        //
//        //        public void Close()
//        //        {
//        //            throw new RuntimeException("KABOOM!");
//        //        }
//        //    };
//        private MockProcessorNode<int, int> processorStreamTime = new MockProcessorNode<int, int>(10L);
//        private MockProcessorNode<int, int> processorSystemTime = new MockProcessorNode<int, int>(10L, PunctuationType.WALL_CLOCK_TIME);

//        private string storeName = "store";
//        private IStateStore stateStore = new MockKeyValueStore(storeName, false);
//        private TopicPartition changelogPartition = new TopicPartition("store-changelog", 0);
//        private long offset = 543L;

//        private ProcessorTopology topology = WithSources(
//            Arrays.asList(source1, source2, processorStreamTime, processorSystemTime),
//            mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2))
//        );

//        private MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(AutoOffsetReset.Earliest);
//        private MockProducer<byte[], byte[]> producer;
//        private MockConsumer<byte[], byte[]> restoreStateConsumer = new MockConsumer<byte[], byte[]>(AutoOffsetReset.Earliest);
//        private IStateRestoreListener stateRestoreListener = new MockStateRestoreListener();
//        //        private StoreChangelogReader changelogReader = new StoreChangelogReader(restoreStateConsumer, TimeSpan.TimeSpan.Zero, stateRestoreListener, new LogContext("stream-task-test "))
//        //        {
//        //
//        //
//        //        public Dictionary<TopicPartition, long> RestoredOffsets()
//        //        {
//        //            return Collections.singletonMap(changelogPartition, offset);
//        //        }
//        //    };
//        private byte[] recordValue = intSerializer.Serialize(null, 10);
//        private byte[] recordKey = intSerializer.Serialize(null, 1);
//        //    private Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG));
//        //    private StreamsMetricsImpl streamsMetrics = new MockStreamsMetrics(metrics);
//        private TaskId taskId00 = new TaskId(0, 0);
//        private MockTime time = new MockTime();
//        private DirectoryInfo baseDir = TestUtils.GetTempDirectory();
//        private StateDirectory stateDirectory;
//        private StreamTask task;
//        private long punctuatedAt;

//        //        private Punctuator punctuator = new Punctuator()
//        //        {
//        //
//        //
//        //        public void Punctuate(long timestamp)
//        //        {
//        //            punctuatedAt = timestamp;
//        //        }
//        //    };

//        static ProcessorTopology WithRepartitionTopics(
//            List<ProcessorNode> processorNodes,
//            Dictionary<string, ISourceNode> sourcesByTopic,
//            HashSet<string> repartitionTopics)
//        {
//            return new ProcessorTopology(
//                processorNodes,
//                sourcesByTopic,
//                Collections.emptyMap<string, ISinkNode>(),
//                Collections.emptyList<IStateStore>(),
//                Collections.emptyList<IStateStore>(),
//                Collections.emptyMap<string, string>(),
//                repartitionTopics);
//        }

//        static ProcessorTopology WithSources(
//            List<ProcessorNode> processorNodes,
//            Dictionary<string, ISourceNode> sourcesByTopic)
//        {
//            return new ProcessorTopology(
//                processorNodes,
//                sourcesByTopic,
//                Collections.emptyMap<string, ISinkNode>(),
//                Collections.emptyList<IStateStore>(),
//                Collections.emptyList<IStateStore>(),
//                Collections.emptyMap<string, string>(),
//                Collections.emptySet<string>());
//        }

//        private StreamsConfig CreateConfig(bool enableEoS)
//        {
//            string canonicalPath;
//            try
//            {
//                canonicalPath = baseDir.FullName;
//            }
//            catch (IOException e)
//            {
//                throw new RuntimeException(e);
//            }
//            return new StreamsConfig(mkProperties(mkMap(
//                mkEntry(StreamsConfig.ApplicationIdConfig, "stream-task-test"),
//                mkEntry(StreamsConfig.BootstrapServersConfig, "localhost:2171"),
//                mkEntry(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3"),
//                mkEntry(StreamsConfig.STATE_DIR_CONFIG, canonicalPath),
//                mkEntry(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.getName()),
//                mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, enableEoS ? StreamsConfig.StreamsConfig.ExactlyOnceConfig : StreamsConfig.AT_LEAST_ONCE),
//                mkEntry(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, "100")
//            )));
//        }


//        public StreamTaskTest()
//        {
//            consumer.Assign(Arrays.asList(partition1, partition2));
//            stateDirectory = new StateDirectory(createConfig(false), new MockTime(), true);
//        }


//        public void Cleanup()
//        { //throws IOException
//            try
//            {
//                if (task != null)
//                {
//                    try
//                    {
//                        task.Close(true, false);
//                    }
//                    catch (Exception e)
//                    {
//                        // swallow
//                    }
//                }
//            }
//            finally
//            {
//                Utils.Delete(baseDir);
//            }
//        }

//        [Fact]
//        public void ShouldHandleInitTransactionsTimeoutExceptionOnCreation()
//        {
//            LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

//            ProcessorTopology topology = WithSources(
//                Arrays.asList(source1, source2, processorStreamTime, processorSystemTime),
//                mkMap(mkEntry(topic1, (SourceNode)source1), mkEntry(topic2, (SourceNode)source2))
//            );

//            source1.addChild(processorStreamTime);
//            source2.addChild(processorStreamTime);
//            source1.addChild(processorSystemTime);
//            source2.addChild(processorSystemTime);

//            // try
//            // {
//            //     new StreamTask(
//            //         taskId00,
//            //         partitions,
//            //         topology,
//            //         consumer,
//            //         changelogReader,
//            //         createConfig(true),
//            //         streamsMetrics,
//            //         stateDirectory,
//            //         null,
//            //         time,
//            //         () => producer = new MockProducer<byte[], byte[]>(false, bytesSerializer, bytesSerializer)
//            //         {
//            // 
//            // 
//            //                 public void initTransactions()
//            //     {
//            //         throw new TimeoutException("test");
//            //     }
//            // },
//            //             null
//            //         );
//            Assert.True(false, "Expected an exception");
//            //}
//            //catch (StreamsException expected)
//            //    {
//            // make sure we log the explanation as an ERROR
//            assertTimeoutErrorLog(appender);

//            // make sure we report the correct message
//            Assert.Equal(expected.Message, ("task [0_0] Failed to initialize task 0_0 due to timeout."));

//            // make sure we preserve the cause
//            Assert.Equal(expected.getCause().GetType(), TimeoutException);
//            Assert.Equal(expected.getCause().Message, ("test"));
//            //}

//            LogCaptureAppender.unregister(appender);
//        }

//        [Fact]
//        public void ShouldHandleInitTransactionsTimeoutExceptionOnResume()
//        {
//            LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

//            ProcessorTopology topology = withSources(
//                Arrays.asList(source1, source2, processorStreamTime, processorSystemTime),
//                mkMap(mkEntry(topic1, (SourceNode)source1), mkEntry(topic2, (SourceNode)source2))
//            );

//            source1.addChild(processorStreamTime);
//            source2.addChild(processorStreamTime);
//            source1.addChild(processorSystemTime);
//            source2.addChild(processorSystemTime);

//            var timeOut = false;

//            //        StreamTask testTask = new StreamTask(
//            //            taskId00,
//            //            partitions,
//            //            topology,
//            //            consumer,
//            //            changelogReader,
//            //            createConfig(true),
//            //            streamsMetrics,
//            //            stateDirectory,
//            //            null,
//            //            time,
//            //            () => producer = new MockProducer<byte[], byte[]>(false, bytesSerializer, bytesSerializer)
//            //            {
//            //    
//            //
//            //                public void initTransactions()
//            //        {
//            //            if (timeOut.Get())
//            //            {
//            //                throw new TimeoutException("test");
//            //            }
//            //            else
//            //            {
//            //                base.initTransactions();
//            //            }
//            //        }
//            //    },
//            //            null
//            //        );
//            testTask.initializeTopology();
//            testTask.Suspend();
//            timeOut.set(true);
//            try
//            {
//                testTask.resume();
//                Assert.True(false, "Expected an exception");
//            }
//            catch (StreamsException expected)
//            {
//                // make sure we log the explanation as an ERROR
//                assertTimeoutErrorLog(appender);

//                // make sure we report the correct message
//                Assert.Equal(expected.Message, ("task [0_0] Failed to initialize task 0_0 due to timeout."));

//                // make sure we preserve the cause
//                Assert.Equal(expected.getCause().GetType(), typeof(TimeoutException));
//                Assert.Equal(expected.getCause().Message, ("test"));
//            }
//            LogCaptureAppender.unregister(appender);
//        }

//        private void AssertTimeoutErrorLog(LogCaptureAppender appender)
//        {

//            string expectedErrorLogMessage =
//                "task [0_0] Timeout exception caught when initializing transactions for task 0_0. " +
//                    "This might happen if the broker is slow to respond, if the network " +
//                    "connection to the broker was interrupted, or if similar circumstances arise. " +
//                    "You can increase producer parameter `max.block.ms` to increase this timeout.";

//            List<string> expectedError = new List<string>();
//            //        appender
//            //            .getEvents()
//            //            .Stream()
//            //            .Filter(@event => @event.Message.Equals(expectedErrorLogMessage))
//            //            .Map(LogCaptureAppender.Event::getLevel)
//            //            .collect(Collectors.toList());
//            Assert.Equal(expectedError, Collections.singletonList("ERROR"));
//        }


//        [Fact]
//        public void TestProcessOrder()
//        {
//            task = createStatelessTask(createConfig(false));

//            task.addRecords(partition1, Arrays.asList(
//                getConsumerRecord(partition1, 10),
//                getConsumerRecord(partition1, 20),
//                getConsumerRecord(partition1, 30)
//            ));

//            task.addRecords(partition2, Arrays.asList(
//                getConsumerRecord(partition2, 25),
//                getConsumerRecord(partition2, 35),
//                getConsumerRecord(partition2, 45)
//            ));

//            Assert.True(task.Process());
//            Assert.Equal(5, task.numBuffered());
//            Assert.Equal(1, source1.numReceived);
//            Assert.Equal(0, source2.numReceived);

//            Assert.True(task.Process());
//            Assert.Equal(4, task.numBuffered());
//            Assert.Equal(2, source1.numReceived);
//            Assert.Equal(0, source2.numReceived);

//            Assert.True(task.Process());
//            Assert.Equal(3, task.numBuffered());
//            Assert.Equal(2, source1.numReceived);
//            Assert.Equal(1, source2.numReceived);

//            Assert.True(task.Process());
//            Assert.Equal(2, task.numBuffered());
//            Assert.Equal(3, source1.numReceived);
//            Assert.Equal(1, source2.numReceived);

//            Assert.True(task.Process());
//            Assert.Equal(1, task.numBuffered());
//            Assert.Equal(3, source1.numReceived);
//            Assert.Equal(2, source2.numReceived);

//            Assert.True(task.Process());
//            Assert.Equal(0, task.numBuffered());
//            Assert.Equal(3, source1.numReceived);
//            Assert.Equal(3, source2.numReceived);
//        }


//        [Fact]
//        public void TestMetrics()
//        {
//            task = createStatelessTask(createConfig(false));

//            Assert.NotNull(getMetric("%s-latency-avg", "The average latency of %s operation.", task.id().ToString()));
//            Assert.NotNull(getMetric("%s-latency-max", "The max latency of %s operation.", task.id().ToString()));
//            Assert.NotNull(getMetric("%s-rate", "The average number of occurrence of %s operation per second.", task.id().ToString()));

//            Assert.NotNull(getMetric("%s-latency-avg", "The average latency of %s operation.", "All"));
//            Assert.NotNull(getMetric("%s-latency-max", "The max latency of %s operation.", "All"));
//            Assert.NotNull(getMetric("%s-rate", "The average number of occurrence of %s operation per second.", "All"));

//            JmxReporter reporter = new JmxReporter("kafka.streams");
//            metrics.addReporter(reporter);
//            Assert.True(reporter.containsMbean(string.format("kafka.streams:type=stream-task-metrics,client-id=test,task-id=%s", task.id.ToString())));
//            Assert.True(reporter.containsMbean("kafka.streams:type=stream-task-metrics,client-id=test,task-id=All"));
//        }

//        private KafkaMetric GetMetric(string nameFormat, string descriptionFormat, string taskId)
//        {
//            return metrics.metrics().Get(metrics.metricName(
//                string.format(nameFormat, "commit"),
//                "stream-task-metrics",
//                string.format(descriptionFormat, "commit"),
//                mkMap(mkEntry("task-id", taskId), mkEntry("client-id", "test"))
//            ));
//        }


//        [Fact]
//        public void TestPauseResume()
//        {
//            task = createStatelessTask(createConfig(false));

//            task.addRecords(partition1, Arrays.asList(
//                getConsumerRecord(partition1, 10),
//                getConsumerRecord(partition1, 20)
//            ));

//            task.addRecords(partition2, Arrays.asList(
//                getConsumerRecord(partition2, 35),
//                getConsumerRecord(partition2, 45),
//                getConsumerRecord(partition2, 55),
//                getConsumerRecord(partition2, 65)
//            ));

//            Assert.True(task.Process());
//            Assert.Equal(1, source1.numReceived);
//            Assert.Equal(0, source2.numReceived);

//            Assert.Equal(1, consumer.paused().Count);
//            Assert.True(consumer.paused().Contains(partition2));

//            task.addRecords(partition1, Arrays.asList(
//                getConsumerRecord(partition1, 30),
//                getConsumerRecord(partition1, 40),
//                getConsumerRecord(partition1, 50)
//            ));

//            Assert.Equal(2, consumer.paused().Count);
//            Assert.True(consumer.paused().Contains(partition1));
//            Assert.True(consumer.paused().Contains(partition2));

//            Assert.True(task.Process());
//            Assert.Equal(2, source1.numReceived);
//            Assert.Equal(0, source2.numReceived);

//            Assert.Equal(1, consumer.paused().Count);
//            Assert.True(consumer.paused().Contains(partition2));

//            Assert.True(task.Process());
//            Assert.Equal(3, source1.numReceived);
//            Assert.Equal(0, source2.numReceived);

//            Assert.Equal(1, consumer.paused().Count);
//            Assert.True(consumer.paused().Contains(partition2));

//            Assert.True(task.Process());
//            Assert.Equal(3, source1.numReceived);
//            Assert.Equal(1, source2.numReceived);

//            Assert.Equal(0, consumer.paused().Count);
//        }


//        [Fact]
//        public void ShouldPunctuateOnceStreamTimeAfterGap()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.InitializeStateStores();
//            task.initializeTopology();

//            task.addRecords(partition1, Arrays.asList(
//                getConsumerRecord(partition1, 20),
//                getConsumerRecord(partition1, 142),
//                getConsumerRecord(partition1, 155),
//                getConsumerRecord(partition1, 160)
//            ));

//            task.addRecords(partition2, Arrays.asList(
//                getConsumerRecord(partition2, 25),
//                getConsumerRecord(partition2, 145),
//                getConsumerRecord(partition2, 159),
//                getConsumerRecord(partition2, 161)
//            ));

//            // st: -1
//            Assert.False(task.maybePunctuateStreamTime()); // punctuate at 20

//            // st: 20
//            Assert.True(task.Process());
//            Assert.Equal(7, task.numBuffered());
//            Assert.Equal(1, source1.numReceived);
//            Assert.Equal(0, source2.numReceived);
//            Assert.True(task.maybePunctuateStreamTime());

//            // st: 25
//            Assert.True(task.Process());
//            Assert.Equal(6, task.numBuffered());
//            Assert.Equal(1, source1.numReceived);
//            Assert.Equal(1, source2.numReceived);
//            Assert.False(task.maybePunctuateStreamTime());

//            // st: 142
//            // punctuate at 142
//            Assert.True(task.Process());
//            Assert.Equal(5, task.numBuffered());
//            Assert.Equal(2, source1.numReceived);
//            Assert.Equal(1, source2.numReceived);
//            Assert.True(task.maybePunctuateStreamTime());

//            // st: 145
//            // only one punctuation after 100ms gap
//            Assert.True(task.Process());
//            Assert.Equal(4, task.numBuffered());
//            Assert.Equal(2, source1.numReceived);
//            Assert.Equal(2, source2.numReceived);
//            Assert.False(task.maybePunctuateStreamTime());

//            // st: 155
//            // punctuate at 155
//            Assert.True(task.Process());
//            Assert.Equal(3, task.numBuffered());
//            Assert.Equal(3, source1.numReceived);
//            Assert.Equal(2, source2.numReceived);
//            Assert.True(task.maybePunctuateStreamTime());

//            // st: 159
//            Assert.True(task.Process());
//            Assert.Equal(2, task.numBuffered());
//            Assert.Equal(3, source1.numReceived);
//            Assert.Equal(3, source2.numReceived);
//            Assert.False(task.maybePunctuateStreamTime());

//            // st: 160, aligned at 0
//            Assert.True(task.Process());
//            Assert.Equal(1, task.numBuffered());
//            Assert.Equal(4, source1.numReceived);
//            Assert.Equal(3, source2.numReceived);
//            Assert.True(task.maybePunctuateStreamTime());

//            // st: 161
//            Assert.True(task.Process());
//            Assert.Equal(0, task.numBuffered());
//            Assert.Equal(4, source1.numReceived);
//            Assert.Equal(4, source2.numReceived);
//            Assert.False(task.maybePunctuateStreamTime());

//            processorStreamTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.STREAM_TIME, 20L, 142L, 155L, 160L);
//        }

//        [Fact]
//        public void ShouldRespectPunctuateCancellationStreamTime()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.InitializeStateStores();
//            task.initializeTopology();

//            task.addRecords(partition1, Arrays.asList(
//                getConsumerRecord(partition1, 20),
//                getConsumerRecord(partition1, 30),
//                getConsumerRecord(partition1, 40)
//            ));

//            task.addRecords(partition2, Arrays.asList(
//                getConsumerRecord(partition2, 25),
//                getConsumerRecord(partition2, 35),
//                getConsumerRecord(partition2, 45)
//            ));

//            Assert.False(task.maybePunctuateStreamTime());

//            // st is now 20
//            Assert.True(task.Process());

//            Assert.True(task.maybePunctuateStreamTime());

//            // st is now 25
//            Assert.True(task.Process());

//            Assert.False(task.maybePunctuateStreamTime());

//            // st is now 30
//            Assert.True(task.Process());

//            processorStreamTime.mockProcessor.scheduleCancellable.cancel();

//            Assert.False(task.maybePunctuateStreamTime());

//            processorStreamTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.STREAM_TIME, 20L);
//        }

//        [Fact]
//        public void ShouldRespectPunctuateCancellationSystemTime()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.InitializeStateStores();
//            task.initializeTopology();
//            long now = time.NowAsEpochMilliseconds;
//            time.Sleep(10);
//            Assert.True(task.maybePunctuateSystemTime());
//            processorSystemTime.mockProcessor.scheduleCancellable.cancel();
//            time.Sleep(10);
//            Assert.False(task.maybePunctuateSystemTime());
//            processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 10);
//        }

//        [Fact]
//        public void ShouldRespectCommitNeeded()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.InitializeStateStores();
//            task.initializeTopology();

//            Assert.False(task.commitNeeded());

//            task.addRecords(partition1, Collections.singletonList(getConsumerRecord(partition1, 0)));
//            Assert.True(task.Process());
//            Assert.True(task.commitNeeded());

//            task.Commit();
//            Assert.False(task.commitNeeded());

//            Assert.True(task.maybePunctuateStreamTime());
//            Assert.True(task.commitNeeded());

//            task.Commit();
//            Assert.False(task.commitNeeded());

//            time.Sleep(10);
//            Assert.True(task.maybePunctuateSystemTime());
//            Assert.True(task.commitNeeded());

//            task.Commit();
//            Assert.False(task.commitNeeded());
//        }

//        [Fact]
//        public void ShouldRespectCommitRequested()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.InitializeStateStores();
//            task.initializeTopology();

//            task.requestCommit();
//            Assert.True(task.commitRequested());
//        }

//        [Fact]
//        public void ShouldBeProcessableIfAllPartitionsBuffered()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.InitializeStateStores();
//            task.initializeTopology();

//            Assert.False(task.isProcessable(0L));

//            byte[] bytes = new ByteBuffer().Allocate(4).putInt(1).array();

//            task.addRecords(partition1, Collections.singleton(new ConsumeResult<>(topic1, 1, 0, bytes, bytes)));

//            Assert.False(task.isProcessable(0L));

//            task.addRecords(partition2, Collections.singleton(new ConsumeResult<>(topic2, 1, 0, bytes, bytes)));

//            Assert.True(task.isProcessable(0L));
//        }

//        [Fact]
//        public void ShouldBeProcessableIfWaitedForTooLong()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.InitializeStateStores();
//            task.initializeTopology();

//            MetricName enforcedProcessMetric = metrics.metricName("enforced-processing-total", "stream-task-metrics", mkMap(mkEntry("client-id", "test"), mkEntry("task-id", taskId00.ToString())));

//            Assert.False(task.isProcessable(0L));
//            Assert.Equal(0.0, metrics.metric(enforcedProcessMetric).metricValue());

//            byte[] bytes = new ByteBuffer().Allocate(4).putInt(1).array();

//            task.addRecords(partition1, Collections.singleton(new ConsumeResult<>(topic1, 1, 0, bytes, bytes)));

//            Assert.False(task.isProcessable(time.NowAsEpochMilliseconds));

//            Assert.False(task.isProcessable(time.NowAsEpochMilliseconds +50L));

//            Assert.True(task.isProcessable(time.NowAsEpochMilliseconds +100L));
//            Assert.Equal(1.0, metrics.metric(enforcedProcessMetric).metricValue());

//            // once decided to enforce, continue doing that
//            Assert.True(task.isProcessable(time.NowAsEpochMilliseconds +101L));
//            Assert.Equal(2.0, metrics.metric(enforcedProcessMetric).metricValue());

//            task.addRecords(partition2, Collections.singleton(new ConsumeResult<>(topic2, 1, 0, bytes, bytes)));

//            Assert.True(task.isProcessable(time.NowAsEpochMilliseconds +130L));
//            Assert.Equal(2.0, metrics.metric(enforcedProcessMetric).metricValue());

//            // one resumed to normal processing, the timer should be reset
//            task.Process();

//            Assert.False(task.isProcessable(time.NowAsEpochMilliseconds +150L));
//            Assert.Equal(2.0, metrics.metric(enforcedProcessMetric).metricValue());

//            Assert.False(task.isProcessable(time.NowAsEpochMilliseconds +249L));
//            Assert.Equal(2.0, metrics.metric(enforcedProcessMetric).metricValue());

//            Assert.True(task.isProcessable(time.NowAsEpochMilliseconds +250L));
//            Assert.Equal(3.0, metrics.metric(enforcedProcessMetric).metricValue());
//        }


//        [Fact]
//        public void ShouldPunctuateSystemTimeWhenIntervalElapsed()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.InitializeStateStores();
//            task.initializeTopology();
//            long now = time.NowAsEpochMilliseconds;
//            time.Sleep(10);
//            Assert.True(task.maybePunctuateSystemTime());
//            time.Sleep(10);
//            Assert.True(task.maybePunctuateSystemTime());
//            time.Sleep(9);
//            Assert.False(task.maybePunctuateSystemTime());
//            time.Sleep(1);
//            Assert.True(task.maybePunctuateSystemTime());
//            time.Sleep(20);
//            Assert.True(task.maybePunctuateSystemTime());
//            Assert.False(task.maybePunctuateSystemTime());
//            processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 10, now + 20, now + 30, now + 50);
//        }

//        [Fact]
//        public void ShouldNotPunctuateSystemTimeWhenIntervalNotElapsed()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.InitializeStateStores();
//            task.initializeTopology();
//            Assert.False(task.maybePunctuateSystemTime());
//            time.Sleep(9);
//            Assert.False(task.maybePunctuateSystemTime());
//            processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME);
//        }

//        [Fact]
//        public void ShouldPunctuateOnceSystemTimeAfterGap()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.InitializeStateStores();
//            task.initializeTopology();
//            long now = time.NowAsEpochMilliseconds;
//            time.Sleep(100);
//            Assert.True(task.maybePunctuateSystemTime());
//            Assert.False(task.maybePunctuateSystemTime());
//            time.Sleep(10);
//            Assert.True(task.maybePunctuateSystemTime());
//            time.Sleep(12);
//            Assert.True(task.maybePunctuateSystemTime());
//            time.Sleep(7);
//            Assert.False(task.maybePunctuateSystemTime());
//            time.Sleep(1); // punctuate at now + 130
//            Assert.True(task.maybePunctuateSystemTime());
//            time.Sleep(105); // punctuate at now + 235
//            Assert.True(task.maybePunctuateSystemTime());
//            Assert.False(task.maybePunctuateSystemTime());
//            time.Sleep(5); // punctuate at now + 240, still aligned on the initial punctuation
//            Assert.True(task.maybePunctuateSystemTime());
//            Assert.False(task.maybePunctuateSystemTime());
//            processorSystemTime.mockProcessor.checkAndClearPunctuateResult(PunctuationType.WALL_CLOCK_TIME, now + 100, now + 110, now + 122, now + 130, now + 235, now + 240);
//        }

//        [Fact]
//        public void ShouldWrapKafkaExceptionsWithStreamsExceptionAndAddContext()
//        {
//            task = createTaskThatThrowsException(false);
//            task.InitializeStateStores();
//            task.initializeTopology();
//            task.addRecords(partition2, Collections.singletonList(getConsumerRecord(partition2, 0)));

//            try
//            {
//                task.Process();
//                Assert.True(false, "Should've thrown StreamsException");
//            }
//            catch (Exception e)
//            {
//                Assert.Equal(task.processorContext.currentNode(), nullValue());
//            }
//        }

//        [Fact]
//        public void ShouldWrapKafkaExceptionsWithStreamsExceptionAndAddContextWhenPunctuatingStreamTime()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.InitializeStateStores();
//            task.initializeTopology();

//            //    try
//            //    {
//            //        task.punctuate(processorStreamTime, 1, PunctuationType.STREAM_TIME, new Punctuator()
//            //        {
//            //
//            //
//            //                public void punctuate(long timestamp)
//            //        {
//            //            throw new KafkaException("KABOOM!");
//            //        }
//            //    });
//            //Assert.True(false, "Should've thrown StreamsException");
//            //} catch (StreamsException e) {
//            //            string message = e.ToString();
//            //Assert.True("message=" + message + " should contain processor", message.Contains("processor '" + processorStreamTime.Name() + "'"));
//            //            Assert.Equal(task.processorContext.currentNode(), nullValue());
//            //        }
//        }

//        [Fact]
//        public void ShouldWrapKafkaExceptionsWithStreamsExceptionAndAddContextWhenPunctuatingWallClockTimeTime()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.InitializeStateStores();
//            task.initializeTopology();

//            //try
//            //{
//            //    task.punctuate(processorSystemTime, 1, PunctuationType.WALL_CLOCK_TIME, new Punctuator()
//            //    {
//            //
//            //
//            //            public void punctuate(long timestamp)
//            //    {
//            //        throw new KafkaException("KABOOM!");
//            //    }
//            //});
//            Assert.True(false, "Should've thrown StreamsException");
//            // } catch (StreamsException e) {
//            //     string message = e.ToString();
//            // Assert.True("message=" + message + " should contain processor", message.Contains("processor '" + processorSystemTime.Name() + "'"));
//            //     Assert.Equal(task.processorContext.currentNode(), nullValue());
//            // }
//        }

//        [Fact]
//        public void ShouldFlushRecordCollectorOnFlushState()
//        {
//            bool flushed = new bool(false);
//            // StreamsMetricsImpl streamsMetrics = new MockStreamsMetrics(new Metrics());
//            //    StreamTask streamTask = new StreamTask(
//            //        taskId00,
//            //        partitions,
//            //        topology,
//            //        consumer,
//            //        changelogReader,
//            //        createConfig(false),
//            //        streamsMetrics,
//            //        stateDirectory,
//            //        null,
//            //        time,
//            //        () => producer = new MockProducer<>(false, bytesSerializer, bytesSerializer),
//            //        new NoOpRecordCollector()
//            //        {
//            //
//            //
//            //                public void Flush()
//            //    {
//            //        flushed.set(true);
//            //    }
//            //});
//            streamTask.flushState();
//            Assert.True(flushed.Get());
//        }

//        [Fact]
//        public void ShouldCheckpointOffsetsOnCommit()
//        { //throws IOException
//            task = createStatefulTask(createConfig(false), true);
//            task.InitializeStateStores();
//            task.initializeTopology();
//            task.Commit();
//            OffsetCheckpoint checkpoint = new OffsetCheckpoint(
//                new FileInfo(stateDirectory.DirectoryForTask(taskId00), StateManagerUtil.CHECKPOINT_FILE_NAME)
//            );

//            Assert.Equal(checkpoint.read(), Collections.singletonMap(changelogPartition, offset));
//        }

//        [Fact]
//        public void ShouldNotCheckpointOffsetsOnCommitIfEosIsEnabled()
//        {
//            task = createStatefulTask(createConfig(true), true);
//            task.InitializeStateStores();
//            task.initializeTopology();
//            task.Commit();
//            File checkpointFile = new FileInfo(
//                stateDirectory.DirectoryForTask(taskId00),
//                StateManagerUtil.CHECKPOINT_FILE_NAME
//            );

//            Assert.False(checkpointFile.Exists);
//        }

//        [Fact]
//        public void ShouldThrowIllegalStateExceptionIfCurrentNodeIsNotNullWhenPunctuateCalled()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.InitializeStateStores();
//            task.initializeTopology();
//            task.processorContext.setCurrentNode(processorStreamTime);
//            try
//            {
//                task.punctuate(processorStreamTime, 10, PunctuationType.STREAM_TIME, punctuator);
//                Assert.True(false, "Should throw illegal state exception as current node is not null");
//            }
//            catch (IllegalStateException e)
//            {
//                // pass
//            }
//        }

//        [Fact]
//        public void ShouldCallPunctuateOnPassedInProcessorNode()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.InitializeStateStores();
//            task.initializeTopology();
//            task.punctuate(processorStreamTime, 5, PunctuationType.STREAM_TIME, punctuator);
//            Assert.Equal(5L, punctuatedAt);
//            task.punctuate(processorStreamTime, 10, PunctuationType.STREAM_TIME, punctuator);
//            Assert.Equal(10L, punctuatedAt);
//        }

//        [Fact]
//        public void ShouldSetProcessorNodeOnContextBackToNullAfterSuccessfulPunctuate()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.InitializeStateStores();
//            task.initializeTopology();
//            task.punctuate(processorStreamTime, 5, PunctuationType.STREAM_TIME, punctuator);
//            Assert.Equal(((ProcessorContextImpl)task.context).currentNode(), nullValue());
//        }

//        [Fact]// (expected = IllegalStateException)
//        public void ShouldThrowIllegalStateExceptionOnScheduleIfCurrentNodeIsNull()
//        {
//            task = createStatelessTask(createConfig(false));
//            //            task.schedule(1, PunctuationType.STREAM_TIME, new Punctuator()
//            //            {
//            //
//            //
//            //            public void punctuate(long timestamp)
//            //            {
//            //                // no-op
//            //            }
//            //        });
//        }

//        [Fact]
//        public void ShouldNotThrowExceptionOnScheduleIfCurrentNodeIsNotNull()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.processorContext.setCurrentNode(processorStreamTime);
//            task.schedule(1, PunctuationType.STREAM_TIME, new Punctuator());
//            //    {
//            //
//            //
//            //    public void punctuate(long timestamp)
//            //    {
//            //        // no-op
//            //    }
//            //});
//        }

//        [Fact]
//        public void ShouldNotCloseProducerOnCleanCloseWithEosDisabled()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.Close(true, false);
//            task = null;

//            Assert.False(producer.closed());
//        }

//        [Fact]
//        public void ShouldNotCloseProducerOnUncleanCloseWithEosDisabled()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.Close(false, false);
//            task = null;

//            Assert.False(producer.closed());
//        }

//        [Fact]
//        public void ShouldNotCloseProducerOnErrorDuringCleanCloseWithEosDisabled()
//        {
//            task = createTaskThatThrowsException(false);

//            try
//            {
//                task.Close(true, false);
//                Assert.True(false, "should have thrown runtime exception");
//            }
//            catch (RuntimeException expected)
//            {
//                task = null;
//            }

//            Assert.False(producer.closed());
//        }

//        [Fact]
//        public void ShouldNotCloseProducerOnErrorDuringUncleanCloseWithEosDisabled()
//        {
//            task = createTaskThatThrowsException(false);

//            task.Close(false, false);
//            task = null;

//            Assert.False(producer.closed());
//        }

//        [Fact]
//        public void ShouldCommitTransactionAndCloseProducerOnCleanCloseWithEosEnabled()
//        {
//            task = createStatelessTask(createConfig(true));
//            task.initializeTopology();

//            task.Close(true, false);
//            task = null;

//            Assert.True(producer.transactionCommitted());
//            Assert.False(producer.transactionInFlight());
//            Assert.True(producer.closed());
//        }

//        [Fact]
//        public void ShouldNotAbortTransactionAndNotCloseProducerOnErrorDuringCleanCloseWithEosEnabled()
//        {
//            task = createTaskThatThrowsException(true);
//            task.initializeTopology();

//            try
//            {
//                task.Close(true, false);
//                Assert.True(false, "should have thrown runtime exception");
//            }
//            catch (RuntimeException expected)
//            {
//                task = null;
//            }

//            Assert.True(producer.transactionInFlight());
//            Assert.False(producer.closed());
//        }

//        [Fact]
//        public void ShouldOnlyCloseProducerIfFencedOnCommitDuringCleanCloseWithEosEnabled()
//        {
//            task = createStatelessTask(createConfig(true));
//            task.initializeTopology();
//            producer.fenceProducer();

//            try
//            {
//                task.Close(true, false);
//                Assert.True(false, "should have thrown TaskMigratedException");
//            }
//            catch (TaskMigratedException expected)
//            {
//                task = null;
//                Assert.True(expected.getCause() is ProducerFencedException);
//            }

//            Assert.False(producer.transactionCommitted());
//            Assert.True(producer.transactionInFlight());
//            Assert.False(producer.transactionAborted());
//            Assert.False(producer.transactionCommitted());
//            Assert.True(producer.closed());
//        }

//        [Fact]
//        public void ShouldNotCloseProducerIfFencedOnCloseDuringCleanCloseWithEosEnabled()
//        {
//            task = createStatelessTask(createConfig(true));
//            task.initializeTopology();
//            producer.fenceProducerOnClose();

//            try
//            {
//                task.Close(true, false);
//                Assert.True(false, "should have thrown TaskMigratedException");
//            }
//            catch (TaskMigratedException expected)
//            {
//                task = null;
//                Assert.True(expected.getCause() is ProducerFencedException);
//            }

//            Assert.True(producer.transactionCommitted());
//            Assert.False(producer.transactionInFlight());
//            Assert.False(producer.closed());
//        }

//        [Fact]
//        public void ShouldAbortTransactionAndCloseProducerOnUncleanCloseWithEosEnabled()
//        {
//            task = createStatelessTask(createConfig(true));
//            task.initializeTopology();

//            task.Close(false, false);
//            task = null;

//            Assert.True(producer.transactionAborted());
//            Assert.False(producer.transactionInFlight());
//            Assert.True(producer.closed());
//        }

//        [Fact]
//        public void ShouldAbortTransactionAndCloseProducerOnErrorDuringUncleanCloseWithEosEnabled()
//        {
//            task = createTaskThatThrowsException(true);
//            task.initializeTopology();

//            task.Close(false, false);

//            Assert.True(producer.transactionAborted());
//            Assert.True(producer.closed());
//        }

//        [Fact]
//        public void ShouldOnlyCloseProducerIfFencedOnAbortDuringUncleanCloseWithEosEnabled()
//        {
//            task = createStatelessTask(createConfig(true));
//            task.initializeTopology();
//            producer.fenceProducer();

//            task.Close(false, false);
//            task = null;

//            Assert.True(producer.transactionInFlight());
//            Assert.False(producer.transactionAborted());
//            Assert.False(producer.transactionCommitted());
//            Assert.True(producer.closed());
//        }

//        [Fact]
//        public void ShouldOnlyCloseFencedProducerOnUncleanClosedWithEosEnabled()
//        {
//            task = createStatelessTask(createConfig(true));
//            task.initializeTopology();
//            producer.fenceProducer();

//            task.Close(false, true);
//            task = null;

//            Assert.False(producer.transactionAborted());
//            Assert.True(producer.closed());
//        }

//        [Fact]
//        public void ShouldAbortTransactionButNotCloseProducerIfFencedOnCloseDuringUncleanCloseWithEosEnabled()
//        {
//            task = createStatelessTask(createConfig(true));
//            task.initializeTopology();
//            producer.fenceProducerOnClose();

//            task.Close(false, false);
//            task = null;

//            Assert.True(producer.transactionAborted());
//            Assert.False(producer.closed());
//        }

//        [Fact]
//        public void ShouldThrowExceptionIfAnyExceptionsRaisedDuringCloseButStillCloseAllProcessorNodesTopology()
//        {
//            task = createTaskThatThrowsException(false);
//            task.InitializeStateStores();
//            task.initializeTopology();
//            try
//            {
//                task.Close(true, false);
//                Assert.True(false, "should have thrown runtime exception");
//            }
//            catch (RuntimeException expected)
//            {
//                task = null;
//            }
//            Assert.True(processorSystemTime.closed);
//            Assert.True(processorStreamTime.closed);
//            Assert.True(source1.closed);
//        }

//        [Fact]
//        public void ShouldInitAndBeginTransactionOnCreateIfEosEnabled()
//        {
//            task = createStatelessTask(createConfig(true));
//            task.initializeTopology();

//            Assert.True(producer.transactionInitialized());
//            Assert.True(producer.transactionInFlight());
//        }

//        [Fact]
//        public void ShouldWrapProducerFencedExceptionWithTaskMigratedExceptionForBeginTransaction()
//        {
//            task = createStatelessTask(createConfig(true));
//            producer.fenceProducer();

//            try
//            {
//                task.initializeTopology();
//                Assert.True(false, "Should have throws TaskMigratedException");
//            }
//            catch (TaskMigratedException expected)
//            {
//                Assert.True(expected.getCause() is ProducerFencedException);
//            }
//        }

//        [Fact]
//        public void ShouldNotThrowOnCloseIfTaskWasNotInitializedWithEosEnabled()
//        {
//            task = createStatelessTask(createConfig(true));

//            Assert.False(producer.transactionInFlight());
//            task.Close(false, false);
//        }

//        [Fact]
//        public void ShouldNotInitOrBeginTransactionOnCreateIfEosDisabled()
//        {
//            task = createStatelessTask(createConfig(false));

//            Assert.False(producer.transactionInitialized());
//            Assert.False(producer.transactionInFlight());
//        }

//        [Fact]
//        public void ShouldSendOffsetsAndCommitTransactionButNotStartNewTransactionOnSuspendIfEosEnabled()
//        {
//            task = createStatelessTask(createConfig(true));
//            task.initializeTopology();

//            task.addRecords(partition1, Collections.singletonList(getConsumerRecord(partition1, 0)));
//            task.Process();

//            task.Suspend();
//            Assert.True(producer.sentOffsets());
//            Assert.True(producer.transactionCommitted());
//            Assert.False(producer.transactionInFlight());
//        }

//        [Fact]
//        public void ShouldCommitTransactionOnSuspendEvenIfTransactionIsEmptyIfEosEnabled()
//        {
//            task = createStatelessTask(createConfig(true));
//            task.initializeTopology();
//            task.Suspend();

//            Assert.True(producer.transactionCommitted());
//            Assert.False(producer.transactionInFlight());
//        }

//        [Fact]
//        public void ShouldNotSendOffsetsAndCommitTransactionNorStartNewTransactionOnSuspendIfEosDisabled()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.addRecords(partition1, Collections.singletonList(getConsumerRecord(partition1, 0)));
//            task.Process();
//            task.Suspend();

//            Assert.False(producer.sentOffsets());
//            Assert.False(producer.transactionCommitted());
//            Assert.False(producer.transactionInFlight());
//        }

//        [Fact]
//        public void ShouldWrapProducerFencedExceptionWithTaskMigragedExceptionInSuspendWhenCommitting()
//        {
//            task = createStatelessTask(createConfig(true));
//            producer.fenceProducer();

//            try
//            {
//                task.Suspend();
//                Assert.True(false, "Should have throws TaskMigratedException");
//            }
//            catch (TaskMigratedException expected)
//            {
//                Assert.True(expected.getCause() is ProducerFencedException);
//            }
//            task = null;

//            Assert.False(producer.transactionCommitted());
//        }

//        [Fact]
//        public void ShouldWrapProducerFencedExceptionWithTaskMigragedExceptionInSuspendWhenClosingProducer()
//        {
//            task = createStatelessTask(createConfig(true));
//            task.initializeTopology();

//            producer.fenceProducerOnClose();
//            try
//            {
//                task.Suspend();
//                Assert.True(false, "Should have throws TaskMigratedException");
//            }
//            catch (TaskMigratedException expected)
//            {
//                Assert.True(expected.getCause() is ProducerFencedException);
//            }

//            Assert.True(producer.transactionCommitted());
//        }

//        [Fact]
//        public void ShouldStartNewTransactionOnResumeIfEosEnabled()
//        {
//            task = createStatelessTask(createConfig(true));
//            task.initializeTopology();

//            task.addRecords(partition1, Collections.singletonList(getConsumerRecord(partition1, 0)));
//            task.Process();
//            task.Suspend();

//            task.resume();
//            task.initializeTopology();
//            Assert.True(producer.transactionInFlight());
//        }

//        [Fact]
//        public void ShouldNotStartNewTransactionOnResumeIfEosDisabled()
//        {
//            task = createStatelessTask(createConfig(false));

//            task.addRecords(partition1, Collections.singletonList(getConsumerRecord(partition1, 0)));
//            task.Process();
//            task.Suspend();

//            task.resume();
//            Assert.False(producer.transactionInFlight());
//        }

//        [Fact]
//        public void ShouldStartNewTransactionOnCommitIfEosEnabled()
//        {
//            task = createStatelessTask(createConfig(true));
//            task.initializeTopology();

//            task.addRecords(partition1, Collections.singletonList(getConsumerRecord(partition1, 0)));
//            task.Process();

//            task.Commit();
//            Assert.True(producer.transactionInFlight());
//        }

//        [Fact]
//        public void ShouldNotStartNewTransactionOnCommitIfEosDisabled()
//        {
//            task = createStatelessTask(createConfig(false));

//            task.addRecords(partition1, Collections.singletonList(getConsumerRecord(partition1, 0)));
//            task.Process();

//            task.Commit();
//            Assert.False(producer.transactionInFlight());
//        }

//        [Fact]
//        public void ShouldNotAbortTransactionOnZombieClosedIfEosEnabled()
//        {
//            task = createStatelessTask(createConfig(true));
//            task.Close(false, true);
//            task = null;

//            Assert.False(producer.transactionAborted());
//        }

//        [Fact]
//        public void ShouldNotAbortTransactionOnDirtyClosedIfEosDisabled()
//        {
//            task = createStatelessTask(createConfig(false));
//            task.Close(false, false);
//            task = null;

//            Assert.False(producer.transactionAborted());
//        }

//        [Fact]
//        public void ShouldCloseProducerOnCloseWhenEosEnabled()
//        {
//            task = createStatelessTask(createConfig(true));
//            task.initializeTopology();
//            task.Close(true, false);
//            task = null;

//            Assert.True(producer.closed());
//        }

//        [Fact]
//        public void ShouldCloseProducerOnUncleanCloseNotZombieWhenEosEnabled()
//        {
//            task = createStatelessTask(createConfig(true));
//            task.initializeTopology();
//            task.Close(false, false);
//            task = null;

//            Assert.True(producer.closed());
//        }

//        [Fact]
//        public void ShouldCloseProducerOnUncleanCloseIsZombieWhenEosEnabled()
//        {
//            task = createStatelessTask(createConfig(true));
//            task.initializeTopology();
//            task.Close(false, true);
//            task = null;

//            Assert.True(producer.closed());
//        }

//        [Fact]
//        public void ShouldNotViolateAtLeastOnceWhenExceptionOccursDuringFlushing()
//        {
//            task = createTaskThatThrowsException(false);
//            task.InitializeStateStores();
//            task.initializeTopology();

//            try
//            {
//                task.Commit();
//                Assert.True(false, "should have thrown an exception");
//            }
//            catch (Exception e)
//            {
//                // All good
//            }
//        }

//        [Fact]
//        public void ShouldNotViolateAtLeastOnceWhenExceptionOccursDuringTaskSuspension()
//        {
//            StreamTask task = createTaskThatThrowsException(false);

//            task.InitializeStateStores();
//            task.initializeTopology();
//            try
//            {
//                task.Suspend();
//                Assert.True(false, "should have thrown an exception");
//            }
//            catch (Exception e)
//            {
//                // All good
//            }
//        }

//        [Fact]
//        public void ShouldCloseStateManagerIfFailureOnTaskClose()
//        {
//            task = createStatefulTaskThatThrowsExceptionOnClose();
//            task.InitializeStateStores();
//            task.initializeTopology();

//            try
//            {
//                task.Close(true, false);
//                Assert.True(false, "should have thrown an exception");
//            }
//            catch (Exception e)
//            {
//                // All good
//            }

//            task = null;
//            Assert.False(stateStore.IsOpen());
//        }

//        [Fact]
//        public void ShouldNotCloseTopologyProcessorNodesIfNotInitialized()
//        {
//            StreamTask task = createTaskThatThrowsException(false);
//            try
//            {
//                task.Close(false, false);
//            }
//            catch (Exception e)
//            {
//                Assert.True(false, "should have not closed non-initialized topology");
//            }
//        }

//        [Fact]
//        public void ShouldBeInitializedIfChangelogPartitionsIsEmpty()
//        {
//            StreamTask task = createStatefulTask(createConfig(false), false);

//            Assert.True(task.InitializeStateStores());
//        }

//        [Fact]
//        public void ShouldNotBeInitializedIfChangelogPartitionsIsNonEmpty()
//        {
//            StreamTask task = createStatefulTask(createConfig(false), true);

//            Assert.False(task.InitializeStateStores());
//        }

//        [Fact]
//        public void ShouldReturnOffsetsForRepartitionTopicsForPurging()
//        {
//            TopicPartition repartition = new TopicPartition("repartition", 1);

//            ProcessorTopology topology = withRepartitionTopics(
//                Arrays.asList(source1, source2),
//                mkMap(mkEntry(topic1, source1), mkEntry(repartition.Topic, source2)),
//                Collections.singleton(repartition.Topic)
//            );
//            consumer.Assign(asList(partition1, repartition));

//            task = new StreamTask(
//                taskId00,
//                Utils.mkSet(partition1, repartition),
//                topology,
//                consumer,
//                changelogReader,
//                createConfig(false),
//                streamsMetrics,
//                stateDirectory,
//                null,
//                time,
//                () => producer = new MockProducer<>(false, bytesSerializer, bytesSerializer));
//            task.InitializeStateStores();
//            task.initializeTopology();

//            task.addRecords(partition1, Collections.singletonList(getConsumerRecord(partition1, 5L)));
//            task.addRecords(repartition, Collections.singletonList(getConsumerRecord(repartition, 10L)));

//            Assert.True(task.Process());
//            Assert.True(task.Process());

//            task.Commit();

//            Dictionary<TopicPartition, long> map = task.purgableOffsets();

//            Assert.Equal(map, Collections.singletonMap(repartition, 11L));
//        }

//        [Fact]
//        public void ShouldThrowOnCleanCloseTaskWhenEosEnabledIfTransactionInFlight()
//        {
//            task = createStatelessTask(createConfig(true));
//            try
//            {
//                task.Close(true, false);
//                Assert.True(false, "should have throw IllegalStateException");
//            }
//            catch (IllegalStateException expected)
//            {
//                // pass
//            }
//            task = null;

//            Assert.True(producer.closed());
//        }

//        [Fact]
//        public void ShouldAlwaysCommitIfEosEnabled()
//        {
//            task = CreateStatelessTask(CreateConfig(true));

//            RecordCollector recordCollector = new RecordCollector("StreamTask",
//                    new DefaultProductionExceptionHandler());

//            recordCollector.Init(producer);

//            task.InitializeStateStores();
//            task.InitializeTopology();
//            task.Punctuate(processorSystemTime, 5, PunctuationType.WALL_CLOCK_TIME, new Punctuator());
//            //            {
//            //
//            //
//            //            public void punctuate(long timestamp)
//            //            {
//            //                recordCollector.send("result-topic1", 3, 5, null, 0, time.NowAsEpochMilliseconds,
//            //                    new Serdes.Int().Serializer(), new Serdes.Int().Serializer());
//            //        }
//            //    });
//            task.Commit();
//            Assert.Equal(1, producer.history().Count);
//        }

//        private StreamTask CreateStatefulTask(StreamsConfig config, bool logged)
//        {
//            IStateStore stateStore = new MockKeyValueStore(storeName, logged);

//            ProcessorTopology topology = ProcessorTopologyFactories.With(
//                Arrays.asList(source1, source2),
//                mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2)),
//                Collections.singletonList(stateStore),
//                logged ? Collections.singletonMap(storeName, storeName + "-changelog") : Collections.emptyMap());

//            return new StreamTask(
//                taskId00,
//                partitions,
//                topology,
//                consumer,
//                changelogReader,
//                config,
//                streamsMetrics,
//                stateDirectory,
//                null,
//                time,
//                () => producer = new MockProducer<>(false, bytesSerializer, bytesSerializer));
//        }

//        private StreamTask CreateStatefulTaskThatThrowsExceptionOnClose()
//        {
//            ProcessorTopology topology = ProcessorTopologyFactories.With(
//                Arrays.asList(source1, source3),
//                mkMap(mkEntry(topic1, source1), mkEntry(topic2, source3)),
//                Collections.singletonList(stateStore),
//                Collections.emptyMap());

//            return new StreamTask(
//                taskId00,
//                partitions,
//                topology,
//                consumer,
//                changelogReader,
//                createConfig(true),
//                streamsMetrics,
//                stateDirectory,
//                null,
//                time,
//                () => producer = new MockProducer<>(false, bytesSerializer, bytesSerializer));
//        }

//        private StreamTask CreateStatelessTask(StreamsConfig streamsConfig)
//        {
//            ProcessorTopology topology = withSources(
//                Arrays.asList(source1, source2, processorStreamTime, processorSystemTime),
//                mkMap(mkEntry(topic1, source1), mkEntry(topic2, source2))
//            );

//            source1.addChild(processorStreamTime);
//            source2.addChild(processorStreamTime);
//            source1.addChild(processorSystemTime);
//            source2.addChild(processorSystemTime);

//            return new StreamTask(
//                taskId00,
//                partitions,
//                topology,
//                consumer,
//                changelogReader,
//                streamsConfig,
//                streamsMetrics,
//                stateDirectory,
//                null,
//                time,
//                () => producer = new MockProducer<>(false, bytesSerializer, bytesSerializer));
//        }

//        // this task will throw exception when processing (on partition2), flushing, suspending and closing
//        private StreamTask CreateTaskThatThrowsException(bool enableEos)
//        {
//            ProcessorTopology topology = withSources(
//                Arrays.asList(source1, source3, processorStreamTime, processorSystemTime),
//                mkMap(mkEntry(topic1, source1), mkEntry(topic2, source3))
//            );

//            source1.addChild(processorStreamTime);
//            source3.addChild(processorStreamTime);
//            source1.addChild(processorSystemTime);
//            source3.addChild(processorSystemTime);

//            return null;
//            //    return new StreamTask(
//            //        taskId00,
//            //        partitions,
//            //        topology,
//            //        consumer,
//            //        changelogReader,
//            //        createConfig(enableEos),
//            //        streamsMetrics,
//            //        stateDirectory,
//            //        null,
//            //        time,
//            //        () => producer = new MockProducer<>(false, bytesSerializer, bytesSerializer))
//            //    {
//            //
//            //
//            //    protected void flushState()
//            //    {
//            //        throw new RuntimeException("KABOOM!");
//            //    }
//            //};
//        }

//        private ConsumeResult<byte[], byte[]> GetConsumerRecord(TopicPartition topicPartition, long offset)
//        {
//            return new ConsumeResult<>(
//                topicPartition.Topic,
//                topicPartition.Partition,
//                offset,
//                offset, // use the offset as the timestamp
//                TimestampType.CreateTime,
//                0L,
//                0,
//                0,
//                recordKey,
//                recordValue
//            );
//        }
//    }
//}
