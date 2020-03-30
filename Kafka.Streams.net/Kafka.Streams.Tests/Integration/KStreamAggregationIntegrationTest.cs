namespace Kafka.Streams.Tests.Integration
{
}
//using Confluent.Kafka;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Integration
//{
//    public class KStreamAggregationIntegrationTest
//    {
//        private static int NUM_BROKERS = 1;


//        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

//        private static volatile AtomicInteger testNo = new AtomicInteger(0);
//        private MockTime mockTime = CLUSTER.time;
//        private StreamsBuilder builder;
//        private Properties streamsConfiguration;
//        private KafkaStreams kafkaStreams;
//        private string streamOneInput;
//        private string userSessionsStream = "user-sessions";
//        private string outputTopic;
//        private KGroupedStream<string, string> groupedStream;
//        private Reducer<string> reducer;
//        private Initializer<int> initializer;
//        private Aggregator<string, string, int> aggregator;
//        private KStream<int, string> stream;


//        public void before()
//        {// throws InterruptedException
//            builder = new StreamsBuilder();
//            createTopics();
//            streamsConfiguration = new Properties();
//            string applicationId = "kgrouped-stream-test-" + testNo.incrementAndGet();
//            streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
//            streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//            streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
//            streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//            streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
//            streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//            streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Int().getClass());

//            KeyValueMapper<int, string, string> mapper = MockMapper.selectValueMapper();
//            stream = builder.stream(streamOneInput, Consumed.with(Serdes.Int(), Serdes.String()));
//            groupedStream = stream.groupBy(mapper, Grouped.with(Serdes.String(), Serdes.String()));

//            reducer = (value1, value2) => value1 + ":" + value2;
//            initializer = () => 0;
//            aggregator = (aggKey, value, aggregate) => aggregate + value.Length();
//        }


//        public void whenShuttingDown()
//        { //throws IOException
//            if (kafkaStreams != null)
//            {
//                kafkaStreams.close();
//            }
//            IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
//        }

//        [Xunit.Fact]
//        public void shouldReduce()
//        {// throws Exception
//            produceMessages(mockTime.milliseconds());
//            groupedStream
//                .reduce(reducer, Materialized.As("reduce-by-key"))
//                .toStream()
//                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

//            startStreams();

//            produceMessages(mockTime.milliseconds());

//            List<KeyValueTimestamp<string, string>> results = receiveMessages(
//                new StringDeserializer(),
//                new StringDeserializer(),
//                10);

//            results.sort(KStreamAggregationIntegrationTest::compare);

//            Assert.Equal(results, (Array.asList(
//                new KeyValueTimestamp("A", "A", mockTime.milliseconds()),
//                new KeyValueTimestamp("A", "A:A", mockTime.milliseconds()),
//                new KeyValueTimestamp("B", "B", mockTime.milliseconds()),
//                new KeyValueTimestamp("B", "B:B", mockTime.milliseconds()),
//                new KeyValueTimestamp("C", "C", mockTime.milliseconds()),
//                new KeyValueTimestamp("C", "C:C", mockTime.milliseconds()),
//                new KeyValueTimestamp("D", "D", mockTime.milliseconds()),
//                new KeyValueTimestamp("D", "D:D", mockTime.milliseconds()),
//                new KeyValueTimestamp("E", "E", mockTime.milliseconds()),
//                new KeyValueTimestamp("E", "E:E", mockTime.milliseconds()))));
//        }

//        private static int compare<K, V>(KeyValueTimestamp<K, V> o1,
//                                                                                KeyValueTimestamp<K, V> o2)
//        {
//            int keyComparison = o1.Key.compareTo(o2.Key);
//            if (keyComparison == 0)
//            {
//                int valueComparison = o1.Value.compareTo(o2.Value);
//                if (valueComparison == 0)
//                {
//                    return long.compare(o1.Timestamp, o2.Timestamp);
//                }
//                return valueComparison;
//            }
//            return keyComparison;
//        }

//        [Xunit.Fact]
//        public void shouldReduceWindowed()
//        {// throws Exception
//            long firstBatchTimestamp = mockTime.milliseconds();
//            mockTime.sleep(1000);
//            produceMessages(firstBatchTimestamp);
//            long secondBatchTimestamp = mockTime.milliseconds();
//            produceMessages(secondBatchTimestamp);
//            produceMessages(secondBatchTimestamp);

//            Serde<Windowed<string>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(string);
//            groupedStream
//                    .windowedBy(TimeWindows.of(ofMillis(500L)))
//                    .reduce(reducer)
//                    .toStream()
//                    .to(outputTopic, Produced.with(windowedSerde, Serdes.String()));

//            startStreams();

//            List<KeyValueTimestamp<Windowed<string>, string>> windowedOutput = receiveMessages(
//                new TimeWindowedDeserializer<>(),
//                new StringDeserializer(),
//                string,
//                15);

//            // read from ConsoleConsumer
//            string resultFromConsoleConsumer = readWindowedKeyedMessagesViaConsoleConsumer(
//                new TimeWindowedDeserializer<string>(),
//                new StringDeserializer(),
//                string,
//                15,
//                true);

//            Comparator<KeyValueTimestamp<Windowed<string>, string>> comparator =
//                Comparator.comparing((KeyValueTimestamp<Windowed<string>, string> o) => o.Key.Key)
//                    .thenComparing(KeyValueTimestamp::value);

//            windowedOutput.sort(comparator);
//            long firstBatchWindow = firstBatchTimestamp / 500 * 500;
//            long secondBatchWindow = secondBatchTimestamp / 500 * 500;

//            List<KeyValueTimestamp<Windowed<string>, string>> expectResult = Array.asList(
//                    new KeyValueTimestamp(new Windowed("A", new TimeWindow(firstBatchWindow, long.MaxValue)), "A", firstBatchTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(secondBatchWindow, long.MaxValue)), "A", secondBatchTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(secondBatchWindow, long.MaxValue)), "A:A", secondBatchTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(firstBatchWindow, long.MaxValue)), "B", firstBatchTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(secondBatchWindow, long.MaxValue)), "B", secondBatchTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(secondBatchWindow, long.MaxValue)), "B:B", secondBatchTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(firstBatchWindow, long.MaxValue)), "C", firstBatchTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(secondBatchWindow, long.MaxValue)), "C", secondBatchTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(secondBatchWindow, long.MaxValue)), "C:C", secondBatchTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(firstBatchWindow, long.MaxValue)), "D", firstBatchTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(secondBatchWindow, long.MaxValue)), "D", secondBatchTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(secondBatchWindow, long.MaxValue)), "D:D", secondBatchTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(firstBatchWindow, long.MaxValue)), "E", firstBatchTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(secondBatchWindow, long.MaxValue)), "E", secondBatchTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(secondBatchWindow, long.MaxValue)), "E:E", secondBatchTimestamp)
//            );
//            Assert.Equal(windowedOutput, (expectResult));

//            HashSet<string> expectResultString = new HashSet<>(expectResult.Count);
//            foreach (KeyValueTimestamp<Windowed<string>, string> eachRecord in expectResult)
//            {
//                expectResultString.add("CreateTime:" + eachRecord.Timestamp + ", "
//                    + eachRecord.Key + ", " + eachRecord.Value);
//            }

//            // check every message is contained in the expect result
//            string[] allRecords = resultFromConsoleConsumer.split("\n");
//            foreach (string record in allRecords)
//            {
//                Assert.True(expectResultString.Contains(record));
//            }
//        }

//        [Xunit.Fact]
//        public void shouldAggregate()
//        {// throws Exception
//            produceMessages(mockTime.milliseconds());
//            groupedStream.aggregate(
//                initializer,
//                aggregator,
//                Materialized.As("aggregate-by-selected-key"))
//                .toStream()
//                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Int()));

//            startStreams();

//            produceMessages(mockTime.milliseconds());

//            List<KeyValueTimestamp<string, int>> results = receiveMessages(
//                new StringDeserializer(),
//                Serializers.Int32,
//                10);

//            results.sort(KStreamAggregationIntegrationTest::compare);

//            Assert.Equal(results, (Array.asList(
//                new KeyValueTimestamp("A", 1, mockTime.milliseconds()),
//                new KeyValueTimestamp("A", 2, mockTime.milliseconds()),
//                new KeyValueTimestamp("B", 1, mockTime.milliseconds()),
//                new KeyValueTimestamp("B", 2, mockTime.milliseconds()),
//                new KeyValueTimestamp("C", 1, mockTime.milliseconds()),
//                new KeyValueTimestamp("C", 2, mockTime.milliseconds()),
//                new KeyValueTimestamp("D", 1, mockTime.milliseconds()),
//                new KeyValueTimestamp("D", 2, mockTime.milliseconds()),
//                new KeyValueTimestamp("E", 1, mockTime.milliseconds()),
//                new KeyValueTimestamp("E", 2, mockTime.milliseconds())
//            )));
//        }

//        [Xunit.Fact]
//        public void shouldAggregateWindowed()
//        {// throws Exception
//            long firstTimestamp = mockTime.milliseconds();
//            mockTime.sleep(1000);
//            produceMessages(firstTimestamp);
//            long secondTimestamp = mockTime.milliseconds();
//            produceMessages(secondTimestamp);
//            produceMessages(secondTimestamp);

//            Serde<Windowed<string>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(string);
//            groupedStream.windowedBy(TimeWindows.of(ofMillis(500L)))
//                    .aggregate(
//                            initializer,
//                            aggregator,
//                            Materialized.with(null, Serdes.Int())
//                    )
//                    .toStream()
//                    .to(outputTopic, Produced.with(windowedSerde, Serdes.Int()));

//            startStreams();

//            List<KeyValueTimestamp<Windowed<string>, int>> windowedMessages = receiveMessagesWithTimestamp(
//                new TimeWindowedDeserializer<>(),
//                Serializers.Int32,
//                string,
//                15);

//            // read from ConsoleConsumer
//            string resultFromConsoleConsumer = readWindowedKeyedMessagesViaConsoleConsumer(
//                new TimeWindowedDeserializer<string>(),
//                Serializers.Int32,
//                string,
//                15,
//                true);

//            Comparator<KeyValueTimestamp<Windowed<string>, int>> comparator =
//                Comparator.comparing((KeyValueTimestamp<Windowed<string>, int> o) => o.Key.Key)
//                    .thenComparingInt(KeyValueTimestamp::value);
//            windowedMessages.sort(comparator);

//            long firstWindow = firstTimestamp / 500 * 500;
//            long secondWindow = secondTimestamp / 500 * 500;

//            List<KeyValueTimestamp<Windowed<string>, int>> expectResult = Array.asList(
//                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(firstWindow, long.MaxValue)), 1, firstTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(secondWindow, long.MaxValue)), 1, secondTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(secondWindow, long.MaxValue)), 2, secondTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(firstWindow, long.MaxValue)), 1, firstTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(secondWindow, long.MaxValue)), 1, secondTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(secondWindow, long.MaxValue)), 2, secondTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(firstWindow, long.MaxValue)), 1, firstTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(secondWindow, long.MaxValue)), 1, secondTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(secondWindow, long.MaxValue)), 2, secondTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(firstWindow, long.MaxValue)), 1, firstTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(secondWindow, long.MaxValue)), 1, secondTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(secondWindow, long.MaxValue)), 2, secondTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(firstWindow, long.MaxValue)), 1, firstTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(secondWindow, long.MaxValue)), 1, secondTimestamp),
//                    new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(secondWindow, long.MaxValue)), 2, secondTimestamp));

//            Assert.Equal(windowedMessages, (expectResult));

//            HashSet<string> expectResultString = new HashSet<>(expectResult.Count);
//            foreach (KeyValueTimestamp<Windowed<string>, int> eachRecord in expectResult)
//            {
//                expectResultString.add("CreateTime:" + eachRecord.Timestamp + ", " + eachRecord.Key + ", " + eachRecord.Value);
//            }

//            // check every message is contained in the expect result
//            string[] allRecords = resultFromConsoleConsumer.split("\n");
//            foreach (string record in allRecords)
//            {
//                Assert.True(expectResultString.Contains(record));
//            }

//        }

//        private void shouldCountHelper()
//        {// throws Exception
//            startStreams();

//            produceMessages(mockTime.milliseconds());

//            List<KeyValueTimestamp<string, long>> results = receiveMessages(
//                new StringDeserializer(),
//                new LongDeserializer(),
//                10);
//            results.sort(KStreamAggregationIntegrationTest::compare);

//            Assert.Equal(results, (Array.asList(
//                new KeyValueTimestamp("A", 1L, mockTime.milliseconds()),
//                new KeyValueTimestamp("A", 2L, mockTime.milliseconds()),
//                new KeyValueTimestamp("B", 1L, mockTime.milliseconds()),
//                new KeyValueTimestamp("B", 2L, mockTime.milliseconds()),
//                new KeyValueTimestamp("C", 1L, mockTime.milliseconds()),
//                new KeyValueTimestamp("C", 2L, mockTime.milliseconds()),
//                new KeyValueTimestamp("D", 1L, mockTime.milliseconds()),
//                new KeyValueTimestamp("D", 2L, mockTime.milliseconds()),
//                new KeyValueTimestamp("E", 1L, mockTime.milliseconds()),
//                new KeyValueTimestamp("E", 2L, mockTime.milliseconds())
//            )));
//        }

//        [Xunit.Fact]
//        public void shouldCount()
//        {// throws Exception
//            produceMessages(mockTime.milliseconds());

//            groupedStream.count(Materialized.As("count-by-key"))
//                    .toStream()
//                    .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

//            shouldCountHelper();
//        }

//        [Xunit.Fact]
//        public void shouldCountWithInternalStore()
//        {// throws Exception
//            produceMessages(mockTime.milliseconds());

//            groupedStream.count()
//                    .toStream()
//                    .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

//            shouldCountHelper();
//        }

//        [Xunit.Fact]
//        public void shouldGroupByKey()
//        {// throws Exception
//            long timestamp = mockTime.milliseconds();
//            produceMessages(timestamp);
//            produceMessages(timestamp);

//            stream.groupByKey(Grouped.with(Serdes.Int(), Serdes.String()))
//                    .windowedBy(TimeWindows.of(ofMillis(500L)))
//                    .count()
//                    .toStream((windowedKey, value) => windowedKey.Key + "@" + windowedKey.window().start()).to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

//            startStreams();

//            List<KeyValueTimestamp<string, long>> results = receiveMessages(
//                new StringDeserializer(),
//                new LongDeserializer(),
//                10);
//            results.sort(KStreamAggregationIntegrationTest::compare);

//            long window = timestamp / 500 * 500;
//            Assert.Equal(results, (Array.asList(
//                new KeyValueTimestamp<string, long>("1@" + window, 1L, timestamp),
//                new KeyValueTimestamp<string, long>("1@" + window, 2L, timestamp),
//                new KeyValueTimestamp<string, long>("2@" + window, 1L, timestamp),
//                new KeyValueTimestamp<string, long>("2@" + window, 2L, timestamp),
//                new KeyValueTimestamp<string, long>("3@" + window, 1L, timestamp),
//                new KeyValueTimestamp<string, long>("3@" + window, 2L, timestamp),
//                new KeyValueTimestamp<string, long>("4@" + window, 1L, timestamp),
//                new KeyValueTimestamp<string, long>("4@" + window, 2L, timestamp),
//                new KeyValueTimestamp<string, long>("5@" + window, 1L, timestamp),
//                new KeyValueTimestamp<string, long>("5@" + window, 2L, timestamp)
//            )));
//        }

//        [Xunit.Fact]
//        public void shouldCountSessionWindows()
//        {// throws Exception
//            long sessionGap = 5 * 60 * 1000L;
//            List<KeyValuePair<string, string>> t1Messages = Array.asList(new KeyValuePair<>("bob", "start"),
//                                                                            new KeyValuePair<>("penny", "start"),
//                                                                            new KeyValuePair<>("jo", "pause"),
//                                                                            new KeyValuePair<>("emily", "pause"));

//            long t1 = mockTime.milliseconds() - TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
//            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
//                    userSessionsStream,
//                    t1Messages,
//                    TestUtils.producerConfig(
//                            CLUSTER.bootstrapServers(),
//                            StringSerializer,
//                            StringSerializer,
//                            new Properties()),
//                    t1);
//            long t2 = t1 + (sessionGap / 2);
//            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
//                    userSessionsStream,
//                    Collections.singletonList(
//                            new KeyValuePair<>("emily", "resume")
//                    ),
//                    TestUtils.producerConfig(
//                            CLUSTER.bootstrapServers(),
//                            StringSerializer,
//                            StringSerializer,
//                            new Properties()),
//                    t2);
//            long t3 = t1 + sessionGap + 1;
//            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
//                    userSessionsStream,
//                    Array.asList(
//                            new KeyValuePair<>("bob", "pause"),
//                            new KeyValuePair<>("penny", "stop")
//                    ),
//                    TestUtils.producerConfig(
//                            CLUSTER.bootstrapServers(),
//                            StringSerializer,
//                            StringSerializer,
//                            new Properties()),
//                    t3);
//            long t4 = t3 + (sessionGap / 2);
//            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
//                    userSessionsStream,
//                    Array.asList(
//                            new KeyValuePair<>("bob", "resume"), // bobs session continues
//                            new KeyValuePair<>("jo", "resume")   // jo's starts new session
//                    ),
//                    TestUtils.producerConfig(
//                            CLUSTER.bootstrapServers(),
//                            StringSerializer,
//                            StringSerializer,
//                            new Properties()),
//                    t4);
//            long t5 = t4 - 1;
//            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
//                userSessionsStream,
//                Collections.singletonList(
//                    new KeyValuePair<>("jo", "late")   // jo has late arrival
//                ),
//                TestUtils.producerConfig(
//                    CLUSTER.bootstrapServers(),
//                    StringSerializer,
//                    StringSerializer,
//                    new Properties()),
//                t5);

//            Dictionary<Windowed<string>, KeyValuePair<long, long>> results = new HashMap<>();
//            CountDownLatch latch = new CountDownLatch(13);

//            builder.stream(userSessionsStream, Consumed.with(Serdes.String(), Serdes.String()))
//                    .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
//                    .windowedBy(SessionWindows.with(ofMillis(sessionGap)))
//                    .count()
//                    .toStream()
//                    .transform(() => new Transformer<Windowed<string>, long, KeyValuePair<object, object>>()
//                    {
//                                private ProcessorContext context;


//        public void Init(IProcessorContext context)
//        {
//            this.context = context;
//        }


//        public KeyValuePair<object, object> transform(Windowed<string> key, long value)
//        {
//            results.put(key, KeyValuePair.Create(value, context.Timestamp));
//            latch.countDown();
//            return null;
//        }


//        public void close() { }
//    });

//        startStreams();
//    latch.await(30, TimeUnit.SECONDS);

//        Assert.Equal(results.get(new Windowed<>("bob", new SessionWindow(t1, t1))), (KeyValuePair.Create(1L, t1)));
//        Assert.Equal(results.get(new Windowed<>("penny", new SessionWindow(t1, t1))), (KeyValuePair.Create(1L, t1)));
//        Assert.Equal(results.get(new Windowed<>("jo", new SessionWindow(t1, t1))), (KeyValuePair.Create(1L, t1)));
//        Assert.Equal(results.get(new Windowed<>("jo", new SessionWindow(t5, t4))), (KeyValuePair.Create(2L, t4)));
//        Assert.Equal(results.get(new Windowed<>("emily", new SessionWindow(t1, t2))), (KeyValuePair.Create(2L, t2)));
//        Assert.Equal(results.get(new Windowed<>("bob", new SessionWindow(t3, t4))), (KeyValuePair.Create(2L, t4)));
//        Assert.Equal(results.get(new Windowed<>("penny", new SessionWindow(t3, t3))), (KeyValuePair.Create(1L, t3)));
//    }

//[Xunit.Fact]
//public void shouldReduceSessionWindows()
//{// throws Exception
//    long sessionGap = 1000L; // something to do with time
//    List<KeyValuePair<string, string>> t1Messages = Array.asList(new KeyValuePair<>("bob", "start"),
//                                                                    new KeyValuePair<>("penny", "start"),
//                                                                    new KeyValuePair<>("jo", "pause"),
//                                                                    new KeyValuePair<>("emily", "pause"));

//    long t1 = mockTime.milliseconds();
//    IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
//            userSessionsStream,
//            t1Messages,
//            TestUtils.producerConfig(
//                    CLUSTER.bootstrapServers(),
//                    StringSerializer,
//                    StringSerializer,
//                    new Properties()),
//            t1);
//    long t2 = t1 + (sessionGap / 2);
//    IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
//            userSessionsStream,
//            Collections.singletonList(
//                    new KeyValuePair<>("emily", "resume")
//            ),
//            TestUtils.producerConfig(
//                    CLUSTER.bootstrapServers(),
//                    StringSerializer,
//                    StringSerializer,
//                    new Properties()),
//            t2);
//    long t3 = t1 + sessionGap + 1;
//    IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
//            userSessionsStream,
//            Array.asList(
//                    new KeyValuePair<>("bob", "pause"),
//                    new KeyValuePair<>("penny", "stop")
//            ),
//            TestUtils.producerConfig(
//                    CLUSTER.bootstrapServers(),
//                    StringSerializer,
//                    StringSerializer,
//                    new Properties()),
//            t3);
//    long t4 = t3 + (sessionGap / 2);
//    IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
//            userSessionsStream,
//            Array.asList(
//                    new KeyValuePair<>("bob", "resume"), // bobs session continues
//                    new KeyValuePair<>("jo", "resume")   // jo's starts new session
//            ),
//            TestUtils.producerConfig(
//                    CLUSTER.bootstrapServers(),
//                    StringSerializer,
//                    StringSerializer,
//                    new Properties()),
//            t4);
//    long t5 = t4 - 1;
//    IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
//        userSessionsStream,
//        Collections.singletonList(
//            new KeyValuePair<>("jo", "late")   // jo has late arrival
//        ),
//        TestUtils.producerConfig(
//            CLUSTER.bootstrapServers(),
//            StringSerializer,
//            StringSerializer,
//            new Properties()),
//        t5);

//    Dictionary<Windowed<string>, KeyValuePair<string, long>> results = new HashMap<>();
//    CountDownLatch latch = new CountDownLatch(13);
//    string userSessionsStore = "UserSessionsStore";
//    builder.stream(userSessionsStream, Consumed.with(Serdes.String(), Serdes.String()))
//            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
//            .windowedBy(SessionWindows.with(ofMillis(sessionGap)))
//            .reduce((value1, value2) => value1 + ":" + value2, Materialized.As(userSessionsStore))
//            .toStream()
//        .transform(() => new Transformer<Windowed<string>, string, KeyValuePair<object, object>>()
//        {
//                private ProcessorContext context;


//public void init(ProcessorContext context)
//{
//    this.context = context;
//}


//public KeyValuePair<object, object> transform(Windowed<string> key, string value)
//{
//    results.put(key, KeyValuePair.Create(value, context.Timestamp));
//    latch.countDown();
//    return null;
//}


//public void close() { }
//            });

//        startStreams();
//latch.await(30, TimeUnit.SECONDS);

//        // verify correct data received
//        Assert.Equal(results.get(new Windowed<>("bob", new SessionWindow(t1, t1))), (KeyValuePair.Create("start", t1)));
//        Assert.Equal(results.get(new Windowed<>("penny", new SessionWindow(t1, t1))), (KeyValuePair.Create("start", t1)));
//        Assert.Equal(results.get(new Windowed<>("jo", new SessionWindow(t1, t1))), (KeyValuePair.Create("pause", t1)));
//        Assert.Equal(results.get(new Windowed<>("jo", new SessionWindow(t5, t4))), (KeyValuePair.Create("resume:late", t4)));
//        Assert.Equal(results.get(new Windowed<>("emily", new SessionWindow(t1, t2))), (KeyValuePair.Create("pause:resume", t2)));
//        Assert.Equal(results.get(new Windowed<>("bob", new SessionWindow(t3, t4))), (KeyValuePair.Create("pause:resume", t4)));
//        Assert.Equal(results.get(new Windowed<>("penny", new SessionWindow(t3, t3))), (KeyValuePair.Create("stop", t3)));

//        // verify can query data via IQ
//        ReadOnlySessionStore<string, string> sessionStore =
//            kafkaStreams.store(userSessionsStore, QueryableStoreTypes.sessionStore());
//KeyValueIterator<Windowed<string>, string> bob = sessionStore.fetch("bob");
//Assert.Equal(bob.next(), (KeyValuePair.Create(new Windowed<>("bob", new SessionWindow(t1, t1)), "start")));
//        Assert.Equal(bob.next(), (KeyValuePair.Create(new Windowed<>("bob", new SessionWindow(t3, t4)), "pause:resume")));
//        Assert.False(bob.hasNext());
//    }

//    [Xunit.Fact]
//public void shouldCountUnlimitedWindows()
//{// throws Exception
//    long startTime = mockTime.milliseconds() - TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS) + 1;
//    long incrementTime = Duration.ofDays(1).toMillis();

//    long t1 = mockTime.milliseconds() - TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
//    List<KeyValuePair<string, string>> t1Messages = Array.asList(new KeyValuePair<>("bob", "start"),
//                                                                    new KeyValuePair<>("penny", "start"),
//                                                                    new KeyValuePair<>("jo", "pause"),
//                                                                    new KeyValuePair<>("emily", "pause"));

//    Properties producerConfig = TestUtils.producerConfig(
//        CLUSTER.bootstrapServers(),
//        StringSerializer,
//        StringSerializer,
//        new Properties()
//    );

//    IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
//        userSessionsStream,
//        t1Messages,
//        producerConfig,
//        t1);

//    long t2 = t1 + incrementTime;
//    IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
//        userSessionsStream,
//        Collections.singletonList(
//            new KeyValuePair<>("emily", "resume")
//        ),
//        producerConfig,
//        t2);
//    long t3 = t2 + incrementTime;
//    IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
//        userSessionsStream,
//        Array.asList(
//            new KeyValuePair<>("bob", "pause"),
//            new KeyValuePair<>("penny", "stop")
//        ),
//        producerConfig,
//        t3);

//    long t4 = t3 + incrementTime;
//    IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
//        userSessionsStream,
//        Array.asList(
//            new KeyValuePair<>("bob", "resume"), // bobs session continues
//            new KeyValuePair<>("jo", "resume")   // jo's starts new session
//        ),
//        producerConfig,
//        t4);

//    Dictionary<Windowed<string>, KeyValuePair<long, long>> results = new HashMap<>();
//    CountDownLatch latch = new CountDownLatch(5);

//    builder.stream(userSessionsStream, Consumed.with(Serdes.String(), Serdes.String()))
//           .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
//           .windowedBy(UnlimitedWindows.of().startOn(ofEpochMilli(startTime)))
//           .count()
//           .toStream()
//           .transform(() => new Transformer<Windowed<string>, long, KeyValuePair<object, object>>()
//           {
//                   private ProcessorContext context;


//public void init(ProcessorContext context)
//{
//    this.context = context;
//}


//public KeyValuePair<object, object> transform(Windowed<string> key, long value)
//{
//    results.put(key, KeyValuePair.Create(value, context.Timestamp));
//    latch.countDown();
//    return null;
//}


//public void close() { }
//               });
//        startStreams();
//Assert.True(latch.await(30, TimeUnit.SECONDS));

//        Assert.Equal(results.get(new Windowed<>("bob", new UnlimitedWindow(startTime))), (KeyValuePair.Create(2L, t4)));
//        Assert.Equal(results.get(new Windowed<>("penny", new UnlimitedWindow(startTime))), (KeyValuePair.Create(1L, t3)));
//        Assert.Equal(results.get(new Windowed<>("jo", new UnlimitedWindow(startTime))), (KeyValuePair.Create(1L, t4)));
//        Assert.Equal(results.get(new Windowed<>("emily", new UnlimitedWindow(startTime))), (KeyValuePair.Create(1L, t2)));
//    }


//    private void produceMessages(long timestamp)
//{// throws Exception
//    IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
//        streamOneInput,
//        Array.asList(
//            new KeyValuePair<>(1, "A"),
//            new KeyValuePair<>(2, "B"),
//            new KeyValuePair<>(3, "C"),
//            new KeyValuePair<>(4, "D"),
//            new KeyValuePair<>(5, "E")),
//        TestUtils.producerConfig(
//            CLUSTER.bootstrapServers(),
//            IntegerSerializer,
//            StringSerializer,
//            new Properties()),
//        timestamp);
//}


//private void createTopics()
//{// throws InterruptedException
//    streamOneInput = "stream-one-" + testNo;
//    outputTopic = "output-" + testNo;
//    userSessionsStream = userSessionsStream + "-" + testNo;
//    CLUSTER.createTopic(streamOneInput, 3, 1);
//    CLUSTER.createTopics(userSessionsStream, outputTopic);
//}

//private void startStreams()
//{
//    kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
//    kafkaStreams.start();
//}

//private List<KeyValueTimestamp<K, V>> receiveMessages<K, V>(Deserializer<K> keyDeserializer,
//                                                             Deserializer<V> valueDeserializer,
//                                                             int numMessages)
//        //throws InterruptedException {
//        return receiveMessages(keyDeserializer, valueDeserializer, null, numMessages);
//    }

//    private List<KeyValueTimestamp<K, V>> receiveMessages<K, V>(Deserializer<K> keyDeserializer,
//                                                                 Deserializer<V> valueDeserializer,
//                                                                 Class innerClass,
//                                                                 int numMessages)
//{// throws InterruptedException
//    Properties consumerProperties = new Properties();
//    consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//    consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kgroupedstream-test-" + testNo);
//    consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//    consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass().getName());
//    consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass().getName());
//    if (keyDeserializer is TimeWindowedDeserializer || keyDeserializer is SessionWindowedDeserializer)
//    {
//        consumerProperties.setProperty(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS,
//                Serdes.serdeFrom(innerClass).getClass().getName());
//    }
//    return IntegrationTestUtils.waitUntilMinKeyValueWithTimestampRecordsReceived(
//            consumerProperties,
//            outputTopic,
//            numMessages,
//            60 * 1000);
//}

//private List<KeyValueTimestamp<K, V>> receiveMessagesWithTimestamp<K, V>(Deserializer<K> keyDeserializer,
//                                                                                          Deserializer<V> valueDeserializer,
//                                                                                          Class innerClass,
//                                                                                          int numMessages)
//{// throws InterruptedException
//    Properties consumerProperties = new Properties();
//    consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//    consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kgroupedstream-test-" + testNo);
//    consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//    consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass().getName());
//    consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass().getName());
//    if (keyDeserializer is TimeWindowedDeserializer || keyDeserializer is SessionWindowedDeserializer)
//    {
//        consumerProperties.setProperty(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS,
//            Serdes.serdeFrom(innerClass).getClass().getName());
//    }
//    return IntegrationTestUtils.waitUntilMinKeyValueWithTimestampRecordsReceived(
//        consumerProperties,
//        outputTopic,
//        numMessages,
//        60 * 1000);
//}

//private string readWindowedKeyedMessagesViaConsoleConsumer<K, V>(IDeserializer<K> keyDeserializer,
//                                                                  IDeserializer<V> valueDeserializer,
//                                                                  Type innerClass,
//                                                                  int numMessages,
//                                                                  bool printTimestamp)
//{
//    ByteArrayOutputStream newConsole = new ByteArrayOutputStream();
//    PrintStream originalStream = System.Console.Out;
//    try
//    {
//        (PrintStream newStream = new PrintStream(newConsole));
//        System.setOut(newStream);

//        string keySeparator = ", ";
//        // manually construct the console consumer argument array
//        string[] args = new string[] {
//                "--bootstrap-server", CLUSTER.bootstrapServers(),
//                "--from-beginning",
//                "--property", "print.key=true",
//                "--property", "print.timestamp=" + printTimestamp,
//                "--topic", outputTopic,
//                "--max-messages", string.valueOf(numMessages),
//                "--property", "key.deserializer=" + keyDeserializer.getClass().getName(),
//                "--property", "value.deserializer=" + valueDeserializer.getClass().getName(),
//                "--property", "key.separator=" + keySeparator,
//                "--property", "key.deserializer." + StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS + "=" + Serdes.serdeFrom(innerClass).getClass().getName()
//            };

//        ConsoleConsumer.messageCount_$eq(0); //reset the message count
//        ConsoleConsumer.run(new ConsoleConsumer.ConsumerConfig(args));
//        newStream.flush();
//        System.setOut(originalStream);
//        return newConsole.toString();
//    }
//    }
//}
