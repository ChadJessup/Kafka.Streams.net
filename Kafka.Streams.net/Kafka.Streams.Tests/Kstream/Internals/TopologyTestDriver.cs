using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Clients;
using Kafka.Streams.Clients.Consumers;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.KStream;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.ReadOnly;
using Kafka.Streams.State.Sessions;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.State.Windowed;
using Kafka.Streams.Tasks;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;
using Kafka.Streams.Threads.Stream;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace Kafka.Streams.Tests
{
    /**
    * This class makes it easier to write tests to verify the behavior of topologies created with {@link Topology} or
    * {@link StreamsBuilder}.
    * You can test simple topologies that have a single processor, or very complex topologies that have multiple sources,
    * processors, sinks, or sub-topologies.
    * Best of All, the class works without a real Kafka broker, so the tests execute very quickly with very little overhead.
    * <p>
    * Using the {@code TopologyTestDriver} in tests is easy: simply instantiate the driver and provide a {@link Topology}
    * (cf. {@link StreamsBuilder#build()}) and {@link StreamsConfig configs}, {@link #createInputTopic(string, Serializer, Serializer) Create}
    * and use a {@link TestInputTopic} to supply an input records to the topology,
    * and then {@link #createOutputTopic(string, Deserializer, Deserializer) Create} and use a {@link TestOutputTopic} to read and
    * verify any output records by the topology.
    * <p>
    * Although the driver doesn't use a real Kafka broker, it does simulate Kafka {@link Consumer consumers} and
    * {@link Producer producers} that read and write raw {@code byte[]} messages.
    * You can let {@link TestInputTopic} and {@link TestOutputTopic} to handle conversion
    * form regular Java objects to raw bytes.
    *
    * <h2>Driver setup</h2>
    * In order to Create a {@code TopologyTestDriver} instance, you need a {@link Topology} and a {@link StreamsConfig config}.
    * The configuration needs to be representative of what you'd supply to the real topology, so that means including
    * several key properties (cf. {@link StreamsConfig}).
    * For example, the following code fragment creates a configuration that specifies a local Kafka broker list (which is
    * needed but not used), a timestamp extractor, and default serializers and deserializers for string keys and values:
    *
    * <pre>{@code
    * StreamsConfig props = new StreamsConfig();
    * props.Set(StreamsConfig.BootstrapServersConfig, "localhost:9091");
    * props.Set(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimestampExtractor.getName());
    * props.Set(StreamsConfig.DefaultKeySerdeClassConfig, Serdes.String().GetType().FullName);
    * props.Set(StreamsConfig.DefaultValueSerdeClassConfig, Serdes.String().GetType().FullName);
    * Topology topology = ...
    * TopologyTestDriver driver = new TopologyTestDriver(topology, props);
    * }</pre>
    *
    * <h2>Processing messages</h2>
    * <p>
    * Your test can supply new input records on any of the topics that the topology's sources consume.
    * This test driver simulates single-partitioned input topics.
    * Here's an example of an input message on the topic named {@code input-topic}:
    *
    * <pre>{@code
    * TestInputTopic<string, string> inputTopic = driver.createInputTopic("input-topic", stringSerdeSerializer, stringSerializer);
    * inputTopic.PipeInput("key1", "value1");
    * }</pre>
    *
    * When {@link TestInputTopic#PipeInput(object, object)} is called, the driver passes the input message through to the appropriate source that
    * consumes the named topic, and will invoke the processor(s) downstream of the source.
    * If your topology's processors forward messages to sinks, your test can then consume these output messages to verify
    * they match the expected outcome.
    * For example, if our topology should have generated 2 messages on {@code output-topic-1} and 1 message on
    * {@code output-topic-2}, then our test can obtain these messages using the
    * {@link TestOutputTopic#readKeyValue()}  method:
    *
    * <pre>{@code
    * TestOutputTopic<string, string> outputTopic1 = driver.createOutputTopic("output-topic-1", stringDeserializer, stringDeserializer);
    * TestOutputTopic<string, string> outputTopic2 = driver.createOutputTopic("output-topic-2", stringDeserializer, stringDeserializer);
    *
    * KeyValuePair<string, string> record1 = outputTopic1.readKeyValue();
    * KeyValuePair<string, string> record2 = outputTopic2.readKeyValue();
    * KeyValuePair<string, string> record3 = outputTopic1.readKeyValue();
    * }</pre>
    *
    * Again, our example topology generates messages with string keys and values, so we supply our string deserializer
    * instance for use on both the keys and values. Your test logic can then verify whether these output records are
    * correct.
    * <p>
    * Note, that calling {@code PipeInput()} will also trigger {@link PunctuationType#STREAM_TIME event-time} base
    * {@link ProcessorContext#schedule(TimeSpan, PunctuationType, Punctuator) punctuation} callbacks.
    * However, you won't trigger {@link PunctuationType#WALL_CLOCK_TIME wall-clock} type punctuations that you must
    * trigger manually via {@link #advanceWallClockTime(long)}.
    * <p>
    * Finally, when completed, make sure your tests {@link #Close()} the driver to release All resources and
    * {@link org.apache.kafka.streams.processor.Processor processors}.
    *
    * <h2>Processor state</h2>
    * <p>
    * Some processors use Kafka {@link IStateStore state storage}, so this driver class provides the generic
    * {@link #getStateStore(string)} as well as store-type specific methods so that your tests can check the underlying
    * state store(s) used by your topology's processors.
    * In our previous example, after we supplied a single input message and checked the three output messages, our test
    * could also check the key value store to verify the processor correctly added, removed, or updated internal state.
    * Or, our test might have pre-populated some state <em>before</em> submitting the input message, and verified afterward
    * that the processor(s) correctly updated the state.
    *
    * @see TestInputTopic
    * @see TestOutputTopic
    */
    public class TopologyTestDriver
    {
        private readonly IClock mockWallClockTime;
        private readonly KafkaStreamsContext context;

        private const int PARTITION_ID = 0;
        private static readonly TaskId TASK_ID = new TaskId(0, PARTITION_ID);
        private StreamTask? task;
        private GlobalStateUpdateTask? globalStateTask;
        private GlobalStateManager? globalStateManager;

        private StateDirectory stateDirectory;

        // private Metrics metrics;
        private ProcessorTopology processorTopology;
        private ProcessorTopology globalTopology;

        private readonly MockProducer<byte[], byte[]> producer;

        private readonly Dictionary<string, TopicPartition> partitionsByInputTopic = new Dictionary<string, TopicPartition>();
        private readonly Dictionary<string, TopicPartition> globalPartitionsByInputTopic = new Dictionary<string, TopicPartition>();
        private readonly Dictionary<TopicPartition, long> offsetsByTopicOrPatternPartition = new Dictionary<TopicPartition, long>();

        private readonly Dictionary<string, Queue<Message<byte[], byte[]>>> outputRecordsByTopic = new Dictionary<string, Queue<Message<byte[], byte[]>>>();
        private readonly bool eosEnabled;

        private readonly IStateRestoreListener stateRestoreListener = new NoOpStateRestoreListener();

        /**
         * Create a new test diver instance.
         * Initialized the internally mocked wall-clock time with {@link System#currentTimeMillis() current system time}.
         *
         * @param topology the topology to be tested
         * @param config   the configuration for the topology
         */
        public TopologyTestDriver(
            KafkaStreamsContext context,
            Topology topology,
            StreamsConfig config)
            : this(context, topology, config, null)
        {
        }

        /**
         * Create a new test diver instance.
         *
         * @param topology               the topology to be tested
         * @param config                 the configuration for the topology
         * @param initialWallClockTime   the initial value of internally mocked wall-clock time
         */
        public TopologyTestDriver(
            KafkaStreamsContext context,
            Topology topology,
            StreamsConfig config,
            DateTime? initialWallClockTime)
            : this(
                context,
                config,
                initialWallClockTime == null
                  ? DateTimeOffset.Now.ToUnixTimeMilliseconds()
                  : Timestamp.DateTimeToUnixTimestampMs(initialWallClockTime.Value))
        {
        }

        /**
         * Create a new test diver instance.
         *
         * @param builder builder for the topology to be tested
         * @param config the configuration for the topology
         * @param initialWallClockTimeMs the initial value of internally mocked wall-clock time
         */
        private TopologyTestDriver(
            KafkaStreamsContext context,
            StreamsConfig streamsConfig,
            long? initialWallClockTimeMs)
        {
            this.context = context;

            //StreamsConfig streamsConfig = new QuietStreamsConfig(config);
            this.LogIfTaskIdleEnabled(streamsConfig);

            //new LogContext("topology-test-driver ");
            this.mockWallClockTime = new MockTime(initialWallClockTimeMs.GetValueOrDefault());
            this.eosEnabled = StreamsConfig.ExactlyOnceConfig.Equals(streamsConfig.GetString(StreamsConfig.ProcessingGuaranteeConfig));

            //StreamsMetricsImpl streamsMetrics = setupMetrics(streamsConfig);
            this.SetupTopology(streamsConfig);

            var cache = new ThreadCache(
                context.CreateLogger<ThreadCache>(),
                Math.Max(0, streamsConfig.CacheMaxBytesBuffering));

            ISerializer<byte[]> bytesSerializer = Serdes.ByteArray().Serializer;
            this.producer = new MockProducer<byte[], byte[]>(true, bytesSerializer, bytesSerializer);
            //{
            //
            //    public List<PartitionInfo> partitionsFor(string topic)
            //    {
            //        return Collections.singletonList(new PartitionInfo(topic, PARTITION_ID, null, null, null));
            //    }
            //};

            this.SetupGlobalTask(streamsConfig, cache);
            this.SetupTask(streamsConfig, cache);
        }

        private void LogIfTaskIdleEnabled(StreamsConfig streamsConfig)
        {
            var taskIdleTime = streamsConfig.GetLong(StreamsConfig.MAX_TASK_IDLE_MS_CONFIGConfig);
            if (taskIdleTime > 0)
            {
                //  this.logger.Information("Detected {} config in use with TopologyTestDriver (set to {}ms)." +
                //               " This means you might need to use TopologyTestDriver#advanceWallClockTime()" +
                //               " or enqueue records on All partitions to allow Steams to make progress." +
                //               " TopologyTestDriver will this.logger a message each time it cannot process enqueued" +
                //               " records due to {}.",
                //           StreamsConfig.MAX_TASK_IDLE_MS_CONFIG,
                //           taskIdleTime,
                //           StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
            }
        }

        // private StreamsMetricsImpl setupMetrics(StreamsConfig streamsConfig)
        // {
        //     string threadId = Thread.currentThread().getName();
        //
        //     //MetricConfig metricConfig = new MetricConfig()
        //     //    .samples(streamsConfig.GetInt(StreamsConfig.METRICS_NUM_SAMPLES_CONFIG))
        //     //    .recordLevel(Sensor.RecordingLevel.forName(streamsConfig.getString(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG)))
        //     //    .timeWindow(streamsConfig.GetLong(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS);
        //     //metrics = new Metrics(metricConfig, mockWallClockTime);
        //
        //     var streamsMetrics = new StreamsMetricsImpl(
        //         metrics,
        //         "test-client",
        //         streamsConfig.getString(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG)
        //     );
        //     streamsMetrics.setRocksDBMetricsRecordingTrigger(new RocksDBMetricsRecordingTrigger());
        //     TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor(threadId, TASK_ID.ToString(), streamsMetrics);
        //
        //     return streamsMetrics;
        // }

        private void SetupTopology(StreamsConfig streamsConfig)
        {
            this.context.InternalTopologyBuilder.RewriteTopology(streamsConfig);

            this.processorTopology = this.context.InternalTopologyBuilder.Build();
            this.globalTopology = this.context.InternalTopologyBuilder.BuildGlobalStateTopology();

            foreach (string topic in this.processorTopology.SourceTopics)
            {
                var tp = new TopicPartition(topic, PARTITION_ID);
                this.partitionsByInputTopic.Add(topic, tp);
                this.offsetsByTopicOrPatternPartition.Add(tp, 0);
            }

            var createStateDirectory = this.processorTopology.HasPersistentLocalStore() ||
                (this.globalTopology != null && this.globalTopology.HasPersistentGlobalStore());

            this.stateDirectory = new StateDirectory(this.context.CreateLogger<StateDirectory>(), this.context.StreamsConfig);//, createStateDirectory);
        }

        private void SetupGlobalTask(
            StreamsConfig streamsConfig,
            ThreadCache cache)
        {
            if (this.globalTopology != null)
            {
                var globalConsumer = new MockConsumer<byte[], byte[]>(Mock.Of<IConsumer<byte[], byte[]>>());// AutoOffsetReset.Latest);
                foreach (string topicName in this.globalTopology.SourceTopics)
                {
                    var partition = new TopicPartition(topicName, 0);
                    this.globalPartitionsByInputTopic.Add(topicName, partition);
                    this.offsetsByTopicOrPatternPartition.Add(partition, 0);

                    globalConsumer.UpdatePartitions(topicName, new List<TopicPartitionOffset> { new TopicPartitionOffset(partition, 0) });
                    globalConsumer.UpdateBeginningOffsets(new Dictionary<TopicPartition, long> { { partition, 0L } });
                    globalConsumer.UpdateEndOffsets(new List<TopicPartitionOffset> { new TopicPartitionOffset(partition, 0L) });
                }

                this.globalStateManager = new GlobalStateManager(
                    Mock.Of<ILogger<GlobalStateManager>>(),
                    this.globalTopology,
                    Mock.Of<IKafkaClientSupplier>(),
                    globalConsumer,
                    this.stateDirectory,
                    Mock.Of<IStateRestoreListener>(),
                    streamsConfig);

                var globalProcessorContext =
                    new GlobalProcessorContext(
                        this.context,
                        streamsConfig,
                        this.globalStateManager,
                        cache);

                this.globalStateManager.SetGlobalProcessorContext(globalProcessorContext);

                this.globalStateTask = new GlobalStateUpdateTask(
                    this.globalTopology,
                    globalProcessorContext,
                    this.globalStateManager,
                    new LogAndContinueExceptionHandler(Mock.Of<ILogger<LogAndContinueExceptionHandler>>()));

                this.globalStateTask.Initialize();
                globalProcessorContext.SetRecordContext(new ProcessorRecordContext(
                    DateTime.MinValue,
                    -1L,
                    -1,
                    ProcessorContext.NONEXIST_TOPIC,
                    new Headers()));
            }
            else
            {
                this.globalStateManager = null;
                this.globalStateTask = null;
            }
        }

        private void SetupTask(StreamsConfig streamsConfig, ThreadCache cache)
        {
            if (this.partitionsByInputTopic.Any())
            {
                var consumer = new MockConsumer<byte[], byte[]>(Mock.Of<IConsumer<byte[], byte[]>>(c =>
                    c.Committed(It.IsAny<IEnumerable<TopicPartition>>(), It.IsAny<TimeSpan>()) == new List<TopicPartitionOffset>()));

                consumer.Assign(this.partitionsByInputTopic.Values);
                var startOffsets = new Dictionary<TopicPartition, long>();
                foreach (TopicPartition topicPartition in this.partitionsByInputTopic.Values)
                {
                    startOffsets.Add(topicPartition, 0L);
                }

                consumer.UpdateBeginningOffsets(startOffsets);

                var restoreConsumerMock = new RestoreConsumer(
                    Mock.Of<ILogger<RestoreConsumer>>(),
                    streamsConfig.GetRestoreConsumerConfigs());

                var storeChangelogReader = new StoreChangelogReader(
                    this.context,
                    restoreConsumerMock,//processorTopology.StoreToChangelogTopic),
                    this.stateRestoreListener);

                var stateManager = new ProcessorStateManager(
                    this.context,
                    TASK_ID,
                    TaskType.ACTIVE,
                    false,
                    this.stateDirectory,
                    new MockChangelogReader(),
                    this.processorTopology.StoreToChangelogTopic,
                    this.partitionsByInputTopic.Values.ToList());
                //StreamsConfig.ExactlyOnceConfig.Equals(streamsConfig.GetString(StreamsConfig.ProcessingGuaranteeConfig)));

                var streamsProducer = new StreamsProducer(
                    streamsConfig,
                    "threadId",
                    Mock.Of<IKafkaClientSupplier>(),
                    TASK_ID,
                    Guid.NewGuid());

                var recordCollector = new RecordCollector(
                    TASK_ID,
                    //consumer,
                    streamsProducer,
                    streamsConfig.GetDefaultProductionExceptionHandler(this.context.Services).GetType());
                //eosEnabled);

                this.task = new StreamTask(
                    this.context,
                    TASK_ID,
                    new HashSet<TopicPartition>(this.partitionsByInputTopic.Values),
                    this.processorTopology,
                    consumer,
                    streamsConfig,
                    this.stateDirectory,
                    cache,
                    stateManager,
                    recordCollector);

                this.task.InitializeIfNeeded();
                this.task.CompleteRestoration();

                ((IInternalProcessorContext)this.task.ProcessorContext()).SetRecordContext(new ProcessorRecordContext(
                    DateTime.MinValue,
                    -1L,
                    -1,
                    ProcessorContext.NONEXIST_TOPIC,
                    new Headers()));
            }
            else
            {
                this.task = null;
            }
        }

        /**
         * Get read-only handle on global metrics registry.
         *
         * @return Map of All metrics.
         */
        // public Dictionary<MetricName, ? : Metric> metrics()
        // {
        //     return Collections.unmodifiableMap(metrics.metrics());
        // }

        /**
         * Send an input message with the given key, value, and timestamp on the specified topic to the topology and then
         * commit the messages.
         *
         * @deprecated Since 2.4 use methods of {@link TestInputTopic} instead
         *
         * @param consumerRecord the record to be processed
         */
        [Obsolete]
        public void PipeInput(ConsumeResult<byte[], byte[]> consumerRecord)
        {
            this.PipeRecord(
                consumerRecord.Topic,
                consumerRecord.Timestamp.UnixTimestampMs,
                consumerRecord.Key,
                consumerRecord.Value,
                consumerRecord.Headers);
        }

        private void PipeRecord(
            string topicName,
            long timestamp,
            byte[] key,
            byte[] value,
            Headers? headers)
        {
            TopicPartition inputTopicOrPatternPartition = this.GetInputTopicOrPatternPartition(topicName);

            if (this.globalPartitionsByInputTopic.TryGetValue(topicName, out var globalInputTopicPartition)
                && inputTopicOrPatternPartition == null)
            {
                throw new ArgumentException("Unknown topic: " + topicName);
            }

            if (inputTopicOrPatternPartition != null)
            {
                this.EnqueueTaskRecord(topicName, inputTopicOrPatternPartition, timestamp, key, value, headers);
                this.CompleteAllProcessableWork();
            }

            if (globalInputTopicPartition != null)
            {
                this.ProcessGlobalRecord(globalInputTopicPartition, timestamp, key, value, headers);
            }
        }

        private void EnqueueTaskRecord(
            string inputTopic,
            TopicPartition topicOrPatternPartition,
            long timestamp,
            byte[] key,
            byte[] value,
            Headers? headers)
        {
            this.task.AddRecords(
                topicOrPatternPartition,
                new[]
                {
                    new ConsumeResult<byte[], byte[]>
                    {
                          Message = new Message<byte[], byte[]>
                          {
                               Key = key,
                               Value = value,
                               Timestamp = new Timestamp(timestamp, TimestampType.CreateTime),
                               Headers = headers,
                          },
                          Offset = ++offsetsByTopicOrPatternPartition[topicOrPatternPartition] - 1,
                          Partition = topicOrPatternPartition.Partition,
                          TopicPartitionOffset = new TopicPartitionOffset(
                          inputTopic,
                          topicOrPatternPartition.Partition,
                          ++offsetsByTopicOrPatternPartition[topicOrPatternPartition] - 1),
                    }
                });
        }

        private void CompleteAllProcessableWork()
        {
            // for internally triggered processing (like wall-clock punctuations),
            // we might have buffered some records to internal topics that need to
            // be piped back in to kick-start the processing loop. This is idempotent
            // and therefore harmless in the case where All we've done is enqueued an
            // input record from the user.
            this.CaptureOutputsAndReEnqueueInternalResults();

            // If the topology only has global tasks, then `task` would be null.
            // For this method, it just means there's nothing to do.
            if (this.task != null)
            {
                while (task.HasRecordsQueued() && task.IsProcessable(mockWallClockTime.UtcNow))
                {
                    // Process the record ...
                    task.Process(mockWallClockTime.UtcNow);
                    this.task.MaybePunctuateStreamTime();
                    // this.Commit(task.PrepareCommit());
                    task.PostCommit();
                    this.CaptureOutputsAndReEnqueueInternalResults();
                }

                if (task.HasRecordsQueued())
                {
                    //this.logger.info("Due to the {} configuration, there are currently some records" +
                    //             " that cannot be processed. Advancing wall-clock time or" +
                    //             " enqueuing records on the empty topics will allow" +
                    //             " Streams to process more.",
                    //         StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
                }
            }
        }

        private void Commit(Dictionary<TopicPartition, OffsetAndMetadata> offsets)
        {
            // if (processingMode == ProcessingMode.EXACTLY_ONCE_ALPHA || processingMode == ProcessingMode.EXACTLY_ONCE_BETA)
            // {
            //     testDriverProducer.commitTransaction(offsets, new ConsumerGroupMetadata("dummy-app-id"));
            // }
            // else
            // {
            //     consumer.commitSync(offsets);
            // }
        }

        private void ProcessGlobalRecord(
            TopicPartition globalInputTopicPartition,
            long timestamp,
            byte[] key,
            byte[] value,
            Headers headers)
        {
            //globalStateTask.Update(new ConsumeResult<byte[], byte[]>(
            //    globalInputTopicPartition.Topic,
            //    globalInputTopicPartition.Partition,
            //    ++offsetsByTopicOrPatternPartition[globalInputTopicPartition] - 1,
            //    timestamp,
            //    TimestampType.CreateTime,
            //    (long)ConsumeResult.NULL_CHECKSUM,
            //    key == null ? ConsumeResult.NULL_SIZE : key.Length,
            //    value == null ? ConsumeResult.NULL_SIZE : value.Length,
            //    key,
            //    value,
            //    headers));
            this.globalStateTask.FlushState();
        }

        private void ValidateSourceTopicNameRegexPattern(string inputRecordTopic)
        {
            foreach (var sourceTopicName in this.context.InternalTopologyBuilder.GetSourceTopicNames())
            {
                if (!sourceTopicName.Equals(inputRecordTopic) && new Regex(sourceTopicName, RegexOptions.Compiled).Matches(inputRecordTopic).Any())
                {
                    throw new TopologyException("Topology add source of type string for topic: " + sourceTopicName +
                                                    " cannot contain regex pattern for input record topic: " + inputRecordTopic +
                                                    " and hence cannot process the message.");
                }
            }
        }

        private TopicPartition GetInputTopicOrPatternPartition(string topicName)
        {
            if (this.context.InternalTopologyBuilder.GetSourceTopicNames().Any())
            {
                this.ValidateSourceTopicNameRegexPattern(topicName);
            }

            TopicPartition topicPartition = this.partitionsByInputTopic[topicName];
            if (topicPartition == null)
            {
                foreach (var entry in this.partitionsByInputTopic)
                {
                    if (Regex.IsMatch(entry.Key, topicName))
                    {
                        return entry.Value;
                    }
                }
            }

            return topicPartition;
        }

        private void CaptureOutputsAndReEnqueueInternalResults()
        {
            // Capture All the records sent to the producer ...
            List<Message<byte[], byte[]>> output = producer.History();
            // producer.Clear();
            //
            // foreach (Message<byte[], byte[]> record in output)
            // {
            //     outputRecordsByTopic.computeIfAbsent(record.Topic, k => new LinkedList<>()).Add(record);
            //
            //     // Forward back into the topology if the produced record is to an internal or a source topic ...
            //     string outputTopicName = record.Topic;
            //
            //     TopicPartition inputTopicOrPatternPartition = getInputTopicOrPatternPartition(outputTopicName);
            //     TopicPartition globalInputTopicPartition = globalPartitionsByInputTopic.Get(outputTopicName);
            //
            //     if (inputTopicOrPatternPartition != null)
            //     {
            //         enqueueTaskRecord(
            //             outputTopicName,
            //             inputTopicOrPatternPartition,
            //             record.Timestamp,
            //             record.Key,
            //             record.Value,
            //             record.Headers);
            //     }
            //
            //     if (globalInputTopicPartition != null)
            //     {
            //         processGlobalRecord(
            //             globalInputTopicPartition,
            //             record.Timestamp,
            //             record.Key,
            //             record.Value,
            //             record.Headers);
            //     }
            // }
        }

        /**
         * Send input messages to the topology and then commit each message individually.
         *
         * @deprecated Since 2.4 use methods of {@link TestInputTopic} instead
         *
         * @param records a list of records to be processed
         */
        [Obsolete]
        public void PipeInput(List<ConsumeResult<byte[], byte[]>> records)
        {
            foreach (ConsumeResult<byte[], byte[]> record in records)
            {
                this.PipeInput(record);
            }
        }

        /**
         * Advances the internally mocked wall-clock time.
         * This might trigger a {@link PunctuationType#WALL_CLOCK_TIME wall-clock} type
         * {@link ProcessorContext#schedule(TimeSpan, PunctuationType, Punctuator) punctuations}.
         *
         * @deprecated Since 2.4 use {@link #advanceWallClockTime(TimeSpan)} instead
         *
         * @param advanceMs the amount of time to advance wall-clock time in milliseconds
         */
        [Obsolete]
        public void AdvanceWallClockTime(long advanceMs)
        {
            this.AdvanceWallClockTime(TimeSpan.FromMilliseconds(advanceMs));
        }

        /**
         * Advances the internally mocked wall-clock time.
         * This might trigger a {@link PunctuationType#WALL_CLOCK_TIME wall-clock} type
         * {@link ProcessorContext#schedule(TimeSpan, PunctuationType, Punctuator) punctuations}.
         *
         * @param advance the amount of time to advance wall-clock time
         */
        public void AdvanceWallClockTime(TimeSpan advance)
        {
            //mockWallClockTime.Sleep(advance);
            if (this.task != null)
            {
                this.task.MaybePunctuateSystemTime();
                // this.task.Commit();
            }
            this.CompleteAllProcessableWork();
        }

        /**
         * Read the next record from the given topic.
         * These records were output by the topology during the previous calls to {@link #PipeInput(ConsumeResult)}.
         *
         * @deprecated Since 2.4 use methods of {@link TestOutputTopic} instead
         *
         * @param topic the Name of the topic
         * @return the next record on that topic, or {@code null} if there is no record available
         */
        // [Obsolete]
        // public Message<byte[], byte[]> readOutput(string topic)
        // {
        //     Queue<Message<byte[], byte[]>> outputRecords = outputRecordsByTopic.Get(topic);
        //     if (outputRecords == null)
        //     {
        //         return null;
        //     }
        //     return outputRecords.poll();
        // }

        /**
         * Read the next record from the given topic.
         * These records were output by the topology during the previous calls to {@link #PipeInput(ConsumeResult)}.
         *
         * @deprecated Since 2.4 use methods of {@link TestOutputTopic} instead
         *
         * @param topic             the Name of the topic
         * @param keyDeserializer   the deserializer for the key type
         * @param valueDeserializer the deserializer for the value type
         * @return the next record on that topic, or {@code null} if there is no record available
         */
        // [Obsolete]
        // public Message<K, V> readOutput<K, V>(string topic,
        //                                               IDeserializer<K> keyDeserializer,
        //                                               IDeserializer<V> valueDeserializer)
        // {
        //     Message<byte[], byte[]> record = readOutput(topic);
        //     if (record == null)
        //     {
        //         return null;
        //     }
        //     K key = keyDeserializer.Deserialize(record.Topic, record.Key);
        //     V value = valueDeserializer.Deserialize(record.Topic, record.Value);
        //     return new Message<>(record.Topic, record.Partition, record.Timestamp, key, value, record.Headers);
        // }
        //
        // private Queue<Message<byte[], byte[]>> getRecordsQueue(string topicName)
        // {
        //     Queue<Message<byte[], byte[]>> outputRecords = outputRecordsByTopic[topicName];
        //     if (outputRecords == null)
        //     {
        //         if (!processorTopology.SinkTopics.SinkTopics.Contains(topicName))
        //         {
        //             throw new ArgumentException("Unknown topic: " + topicName);
        //         }
        //     }
        //     return outputRecords;
        // }

        /**
         * Create {@link TestInputTopic} to be used for piping records to topic
         * Uses current system time as start timestamp for records.
         * Auto-advance is disabled.
         *
         * @param topicName             the Name of the topic
         * @param keySerializer   the Serializer for the key type
         * @param valueSerializer the Serializer for the value type
         * @param <K> the key type
         * @param <V> the value type
         * @return {@link TestInputTopic} object
         */
        // public TestInputTopic<K, V> createInputTopic<K, V>(string topicName,
        //                                                           ISerializer<K> keySerializer,
        //                                                           ISerializer<V> valueSerializer)
        // {
        //     return new TestInputTopic<K, V>(
        //         this,
        //         topicName,
        //         keySerializer,
        //         valueSerializer,
        //         SystemClock.Instance.GetCurrentInstant(),
        //         TimeSpan.Zero);
        // }

        /**
         * Create {@link TestInputTopic} to be used for piping records to topic
         * Uses provided start timestamp and autoAdvance parameter for records
         *
         * @param topicName             the Name of the topic
         * @param keySerializer   the Serializer for the key type
         * @param valueSerializer the Serializer for the value type
         * @param startTimestamp Start timestamp for auto-generated record time
         * @param autoAdvance autoAdvance duration for auto-generated record time
         * @param <K> the key type
         * @param <V> the value type
         * @return {@link TestInputTopic} object
         */
        //public TestInputTopic<K, V> createInputTopic<K, V>(string topicName,
        //                                                          ISerializer<K> keySerializer,
        //                                                          ISerializer<V> valueSerializer,
        //                                                          Instant startTimestamp,
        //                                                          TimeSpan autoAdvance)
        //{
        //    return new TestInputTopic<>(this, topicName, keySerializer, valueSerializer, startTimestamp, autoAdvance);
        //}

        /**
         * Create {@link TestOutputTopic} to be used for reading records from topic
         *
         * @param topicName             the Name of the topic
         * @param keyDeserializer   the Deserializer for the key type
         * @param valueDeserializer the Deserializer for the value type
         * @param <K> the key type
         * @param <V> the value type
         * @return {@link TestOutputTopic} object
         */
        //public TestOutputTopic<K, V> createOutputTopic<K, V>(string topicName,
        //                                                            IDeserializer<K> keyDeserializer,
        //                                                            IDeserializer<V> valueDeserializer)
        //{
        //    return new TestOutputTopic<>(this, topicName, keyDeserializer, valueDeserializer);
        //}

        // Message<byte[], byte[]> readRecord(string topic)
        // {
        //     Queue<Message<byte[], byte[]>> outputRecords = getRecordsQueue(topic);
        //     if (outputRecords == null)
        //     {
        //         return null;
        //     }
        //
        //     return outputRecords.poll();
        // }

        // TestRecord<K, V> readRecord<K, V>(string topic,
        //                                   IDeserializer<K> keyDeserializer,
        //                                   IDeserializer<V> valueDeserializer)
        // {
        //     var outputRecords = getRecordsQueue(topic);
        //     if (outputRecords == null)
        //     {
        //         throw new Exception("Uninitialized topic: " + topic);
        //     }
        //     Message<byte[], byte[]> record = outputRecords.Poll();
        //     if (record == null)
        //     {
        //         throw new Exception("Empty topic: " + topic);
        //     }
        //     K key = keyDeserializer.Deserialize(record.Topic, record.Key);
        //     V value = valueDeserializer.Deserialize(record.Topic, record.Value);
        //     return new TestRecord<>(key, value, record.Headers, record.Timestamp);
        // }

        public void PipeRecord<K, V>(
            string topic,
            TestRecord<K, V> record,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer,
            DateTime time)
        {
            var serializedKey = keySerializer.Serialize(record.Key, new SerializationContext(MessageComponentType.Key, topic));
            var serializedValue = valueSerializer.Serialize(record.Value, new SerializationContext(MessageComponentType.Value, topic));

            long timestamp;
            if (time != null)
            {
                timestamp = Timestamp.DateTimeToUnixTimestampMs(time);
            }
            else if (record.Timestamp() != null)
            {
                timestamp = record.Timestamp()!.Value;
            }
            else
            {
                throw new InvalidOperationException("Provided `TestRecord` does not have a timestamp and no timestamp overwrite was provided via `time` parameter.");
            }

            this.PipeRecord(topic, timestamp, serializedKey, serializedValue, record.Headers);
        }

        private long GetQueueSize(string topic)
        {
            //Queue<Message<byte[], byte[]>> queue = getRecordsQueue(topic);
            //if (queue == null)
            //{
            //    //Return 0 if not initialized, getRecordsQueue throw exception if non existing topic
            //    return 0;
            //}
            //
            //return queue.size();
            return 0;
        }

        private bool IsEmpty(string topic)
        {
            return this.GetQueueSize(topic) == 0;
        }

        /**
         * Get All {@link IStateStore StateStores} from the topology.
         * The stores can be a "regular" or global stores.
         * <p>
         * This is often useful in test cases to pre-populate the store before the test case instructs the topology to
         * {@link #PipeInput(ConsumeResult) process an input message}, and/or to check the store afterward.
         * <p>
         * Note, that {@code IStateStore} might be {@code null} if a store is added but not connected to any processor.
         * <p>
         * <strong>Caution:</strong> Using this method to access stores that are added by the DSL is unsafe as the store
         * types may change. Stores added by the DSL should only be accessed via the corresponding typed methods
         * like {@link #GetKeyValueStore(string)} etc.
         *
         * @return All stores my Name
         * @see #getStateStore(string)
         * @see #GetKeyValueStore(string)
         * @see #getTimestampedKeyValueStore(string)
         * @see #getWindowStore(string)
         * @see #getTimestampedWindowStore(string)
         * @see #getSessionStore(string)
         */
        public Dictionary<string, IStateStore?> GetAllStateStores()
        {
            var allStores = new Dictionary<string, IStateStore?>();

            foreach (var storeName in this.context.InternalTopologyBuilder.AllStateStoreName())
            {
                allStores.Add(storeName, this.GetStateStore(storeName, false));
            }

            return allStores;
        }

        /**
         * Get the {@link IStateStore} with the given Name.
         * The store can be a "regular" or global store.
         * <p>
         * Should be used for custom stores only.
         * For built-in stores, the corresponding typed methods like {@link #GetKeyValueStore(string)} should be used.
         * <p>
         * This is often useful in test cases to pre-populate the store before the test case instructs the topology to
         * {@link #PipeInput(ConsumeResult) process an input message}, and/or to check the store afterward.
         *
         * @param Name the Name of the store
         * @return the state store, or {@code null} if no store has been registered with the given Name
         * @throws ArgumentException if the store is a built-in store like {@link IKeyValueStore},
         * {@link IWindowStore}, or {@link ISessionStore}
         *
         * @see #getAllStateStores()
         * @see #GetKeyValueStore(string)
         * @see #getTimestampedKeyValueStore(string)
         * @see #getWindowStore(string)
         * @see #getTimestampedWindowStore(string)
         * @see #getSessionStore(string)
         */
        public IStateStore? GetStateStore(string Name)
        {
            return this.GetStateStore(Name, true);
        }

        private IStateStore? GetStateStore(string Name, bool throwForBuiltInStores)
        {
            if (this.task != null)
            {
                IStateStore stateStore = task.ProcessorContext().GetStateStore(Name);
                if (stateStore != null)
                {
                    if (throwForBuiltInStores)
                    {
                        ThrowIfBuiltInStore(stateStore);
                    }

                    return stateStore;
                }
            }

            if (this.globalStateManager != null)
            {
                IStateStore? stateStore = this.globalStateManager.GetStore(Name);
                if (stateStore != null)
                {
                    if (throwForBuiltInStores)
                    {
                        this.ThrowIfBuiltInStore(stateStore);
                    }

                    return stateStore;
                }

            }

            return null;
        }

        private void ThrowIfBuiltInStore(IStateStore stateStore)
        {
            if (stateStore is ITimestampedKeyValueStore)
            {
                throw new ArgumentException("Store " + stateStore.Name
                                                       + " is a timestamped key-value store and should be accessed via `getTimestampedKeyValueStore()`");
            }
            if (stateStore is IReadOnlyKeyValueStore)
            {
                throw new ArgumentException("Store " + stateStore.Name
                                                       + " is a key-value store and should be accessed via `GetKeyValueStore()`");
            }
            if (stateStore is ITimestampedWindowStore)
            {
                throw new ArgumentException("Store " + stateStore.Name
                                                       + " is a timestamped window store and should be accessed via `getTimestampedWindowStore()`");
            }
            if (stateStore is IReadOnlyWindowStore)
            {
                throw new ArgumentException("Store " + stateStore.Name
                                                       + " is a window store and should be accessed via `getWindowStore()`");
            }
            if (stateStore is IReadOnlySessionStore)
            {
                throw new ArgumentException("Store " + stateStore.Name
                                                       + " is a session store and should be accessed via `getSessionStore()`");
            }
        }

        /**
         * Get the {@link IKeyValueStore} or {@link ITimestampedKeyValueStore} with the given Name.
         * The store can be a "regular" or global store.
         * <p>
         * If the registered store is a {@link ITimestampedKeyValueStore} this method will return a value-only query
         * interface. <strong>It is highly recommended to update the code for this case to avoid bugs and to use
         * {@link #getTimestampedKeyValueStore(string)} for full store access instead.</strong>
         * <p>
         * This is often useful in test cases to pre-populate the store before the test case instructs the topology to
         * {@link #PipeInput(ConsumeResult) process an input message}, and/or to check the store afterward.
         *
         * @param Name the Name of the store
         * @return the key value store, or {@code null} if no {@link IKeyValueStore} or {@link ITimestampedKeyValueStore}
         * has been registered with the given Name
         * @see #getAllStateStores()
         * @see #getStateStore(string)
         * @see #getTimestampedKeyValueStore(string)
         * @see #getWindowStore(string)
         * @see #getTimestampedWindowStore(string)
         * @see #getSessionStore(string)
         */
        public IKeyValueStore<K, V>? GetKeyValueStore<K, V>(string Name)
        {
            IStateStore store = this.GetStateStore(Name, false);
            // if (store is ITimestampedKeyValueStore<K, V>)
            // {
            //     this.logger.info("Method #getTimestampedKeyValueStore() should be used to access a ITimestampedKeyValueStore.");
            //     return new KeyValueStoreFacade<>((ITimestampedKeyValueStore<K, V>)store);
            // }

            return store is IKeyValueStore
                ? (IKeyValueStore<K, V>)store
                : null;
        }

        /**
         * Get the {@link ITimestampedKeyValueStore} with the given Name.
         * The store can be a "regular" or global store.
         * <p>
         * This is often useful in test cases to pre-populate the store before the test case instructs the topology to
         * {@link #PipeInput(ConsumeResult) process an input message}, and/or to check the store afterward.
         *
         * @param Name the Name of the store
         * @return the key value store, or {@code null} if no {@link ITimestampedKeyValueStore} has been registered with the given Name
         * @see #getAllStateStores()
         * @see #getStateStore(string)
         * @see #GetKeyValueStore(string)
         * @see #getWindowStore(string)
         * @see #getTimestampedWindowStore(string)
         * @see #getSessionStore(string)
         */
        public IKeyValueStore<K, IValueAndTimestamp<V>>? GetTimestampedKeyValueStore<K, V>(string Name)
        {
            IStateStore store = this.GetStateStore(Name, false);
            return store is ITimestampedKeyValueStore
                ? (ITimestampedKeyValueStore<K, V>)store
                : null;
        }

        /**
         * Get the {@link IWindowStore} or {@link ITimestampedWindowStore} with the given Name.
         * The store can be a "regular" or global store.
         * <p>
         * If the registered store is a {@link ITimestampedWindowStore} this method will return a value-only query
         * interface. <strong>It is highly recommended to update the code for this case to avoid bugs and to use
         * {@link #getTimestampedWindowStore(string)} for full store access instead.</strong>
         * <p>
         * This is often useful in test cases to pre-populate the store before the test case instructs the topology to
         * {@link #PipeInput(ConsumeResult) process an input message}, and/or to check the store afterward.
         *
         * @param Name the Name of the store
         * @return the key value store, or {@code null} if no {@link IWindowStore} or {@link ITimestampedWindowStore}
         * has been registered with the given Name
         * @see #getAllStateStores()
         * @see #getStateStore(string)
         * @see #GetKeyValueStore(string)
         * @see #getTimestampedKeyValueStore(string)
         * @see #getTimestampedWindowStore(string)
         * @see #getSessionStore(string)
         */
        public IWindowStore<K, V>? GetWindowStore<K, V>(string Name)
        {
            IStateStore store = this.GetStateStore(Name, false);
            if (store is ITimestampedWindowStore)
            {
                //       this.logger.info("Method #getTimestampedWindowStore() should be used to access a ITimestampedWindowStore.");
                //       return new WindowStoreFacade<>((ITimestampedWindowStore<K, V>)store);
            }
            return store is IWindowStore
                ? (IWindowStore<K, V>)store
                : null;
        }

        /**
         * Get the {@link ITimestampedWindowStore} with the given Name.
         * The store can be a "regular" or global store.
         * <p>
         * This is often useful in test cases to pre-populate the store before the test case instructs the topology to
         * {@link #PipeInput(ConsumeResult) process an input message}, and/or to check the store afterward.
         *
         * @param Name the Name of the store
         * @return the key value store, or {@code null} if no {@link ITimestampedWindowStore} has been registered with the given Name
         * @see #getAllStateStores()
         * @see #getStateStore(string)
         * @see #GetKeyValueStore(string)
         * @see #getTimestampedKeyValueStore(string)
         * @see #getWindowStore(string)
         * @see #getSessionStore(string)
         */
        public IWindowStore<K, IValueAndTimestamp<V>>? GetTimestampedWindowStore<K, V>(string Name)
        {
            var store = this.GetStateStore(Name, false);
            return store is ITimestampedWindowStore<K, V>
                ? (ITimestampedWindowStore<K, V>)store
                : null;
        }

        /**
         * Get the {@link ISessionStore} with the given Name.
         * The store can be a "regular" or global store.
         * <p>
         * This is often useful in test cases to pre-populate the store before the test case instructs the topology to
         * {@link #PipeInput(ConsumeResult) process an input message}, and/or to check the store afterward.
         *
         * @param Name the Name of the store
         * @return the key value store, or {@code null} if no {@link ISessionStore} has been registered with the given Name
         * @see #getAllStateStores()
         * @see #getStateStore(string)
         * @see #GetKeyValueStore(string)
         * @see #getTimestampedKeyValueStore(string)
         * @see #getWindowStore(string)
         * @see #getTimestampedWindowStore(string)
         */
        public ISessionStore<K, V>? GetSessionStore<K, V>(string Name)
            where V : class
        {
            IStateStore store = this.GetStateStore(Name, false);
            return store is ISessionStore<K, V>
                ? (ISessionStore<K, V>)store
                : null;
        }

        /**
         * Close the driver, its topology, and All processors.
         */
        public void Close()
        {
            if (this.task != null)
            {
                // task.closeClean();
            }

            if (this.globalStateTask != null)
            {
                try
                {
                    this.globalStateTask.Close();
                }
                catch (IOException e)
                {
                    // ignore
                }
            }

            this.CompleteAllProcessableWork();
            //if (task != null && task.hasRecordsQueued())
            //{
            //    this.logger.Warning("Found some records that cannot be processed due to the" +
            //                 " {} configuration during TopologyTestDriver#Close().",
            //             StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
            //}
            if (!this.eosEnabled)
            {
                //  producer.Close();
            }

            this.stateDirectory.Clean(this.context);
        }

        // private MockConsumer<byte[], byte[]> createRestoreConsumer(Dictionary<string, string> storeToChangelogTopic)
        // {
        //     var consumer = new MockConsumer<byte[], byte[]>(AutoOffsetReset.Latest);
        //     //    {
        //     //    public void seekToEnd(Collection<TopicPartition> partitions) { }
        //     //
        //     //    public void seekToBeginning(Collection<TopicPartition> partitions) { }
        //     //
        //     //    public long position(TopicPartition partition)
        //     //    {
        //     //        return 0L;
        //     //    }
        //     //};
        //
        //     // for each store
        //     foreach (var storeAndTopic in storeToChangelogTopic)
        //     {
        //         var topicName = storeAndTopic.Value;
        //         // Set up the restore-state topic ...
        //         // consumer.subscribe(new TopicPartition(topicName, 0));
        //         // Set up the partition that matches the ID (which is what ProcessorStateManager expects) ...
        //         var partitionInfos = new List<PartitionInfo>();
        //         partitionInfos.Add(new PartitionInfo(topicName, PARTITION_ID, null, null, null));
        //         consumer.UpdatePartitions(topicName, partitionInfos);
        //         consumer.UpdateEndOffsets(new List<TopicPartition> { new TopicPartition(topicName, PARTITION_ID), 0L })};
        // }
        //
        //     return consumer;
        // }
    }
}
