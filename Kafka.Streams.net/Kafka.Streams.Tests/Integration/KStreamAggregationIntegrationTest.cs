//using Confluent.Kafka;
//using Kafka.Common;
//using Kafka.Streams;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.State.Queryable;
//using Kafka.Streams.State.ReadOnly;
//using Kafka.Streams.Temporary;
//using Kafka.Streams.Tests.Helpers;
//using Kafka.Streams.Tests.Mocks;
//using Kafka.Streams.Threads.KafkaStreams;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Integration
//{
//    public class KStreamAggregationIntegrationTest
//    {
//        private static int NUM_BROKERS = 1;

//        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

//        private static volatile int testNo = 0;
//        private MockTime mockTime = CLUSTER.time;
//        private StreamsBuilder builder;
//        private StreamsConfig streamsConfiguration;
//        private IKafkaStreamsThread kafkaStreams;
//        private string streamOneInput;
//        private string userSessionsStream = "user-sessions";
//        private string outputTopic;
//        private IKGroupedStream<string, string> groupedStream;
//        private IReducer<string> reducer;
//        private IInitializer<int> initializer;
//        private IAggregator<string, string, int> aggregator;
//        private IKStream<int, string> stream;

//        public KStreamAggregationIntegrationTest()
//        {// throws InterruptedException
//            builder = new StreamsBuilder();
//            createTopics();
//            streamsConfiguration = new StreamsConfig();
//            string applicationId = "kgrouped-stream-test-" + ++testNo;
//            streamsConfiguration.Set(StreamsConfig.ApplicationIdConfig, applicationId);
//            streamsConfiguration.Set(StreamsConfig.BootstrapServersConfig, CLUSTER.bootstrapServers());
//            streamsConfiguration.Set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIGConfig, "earliest");
//            streamsConfiguration.Set(StreamsConfig.STATE_DIR_CONFIGConfig, TestUtils.GetTempDirectory().FullName);
//            streamsConfiguration.Set(StreamsConfig.CacheMaxBytesBufferingConfig, 0.ToString());
//            streamsConfiguration.Set(StreamsConfig.COMMIT_INTERVAL_MS_CONFIGConfig, 100.ToString());
//            streamsConfiguration.Set(StreamsConfig.DefaultKeySerdeClassConfig, Serdes.String().GetType().AssemblyQualifiedName);
//            streamsConfiguration.Set(StreamsConfig.DefaultValueSerdeClassConfig, Serdes.Int().GetType().AssemblyQualifiedName);

//            var mapper = MockMapper.GetSelectValueKeyValueMapper<int, string>();
//            stream = builder.Stream(streamOneInput, Consumed.With(Serdes.Int(), Serdes.String()));
//            groupedStream = stream.GroupBy<string>(mapper, Grouped.With(Serdes.String(), Serdes.String()));

//            reducer = (value1, value2) => value1 + ":" + value2;
//            initializer = () => 0;
//            aggregator = (aggKey, value, aggregate) => aggregate + value.Length();
//        }

//        public void WhenShuttingDown()
//        { //throws IOException
//            if (kafkaStreams != null)
//            {
//                kafkaStreams.Close();
//            }

//            IntegrationTestUtils.PurgeLocalStreamsState(streamsConfiguration);
//        }

//        [Fact]
//        public void shouldReduce()
//        {// throws Exception
//            produceMessages(mockTime.NowAsEpochMilliseconds);
//            groupedStream
//                .Reduce(reducer, Materialized.As("reduce-by-key"))
//                .ToStream()
//                .To(outputTopic, Produced.With(Serdes.String(), Serdes.String()));

//            StartStreams();

//            produceMessages(mockTime.NowAsEpochMilliseconds);

//            List<KeyValueTimestamp<string, string>> results = receiveMessages(
//                Serdes.String().Deserializer,
//                Serdes.String().Deserializer,
//                10);

//            results.Sort();// KStreamAggregationIntegrationTest);

//            Assert.Equal(
//                results,
//                Arrays.asList(
//                    new KeyValueTimestamp<string, string>("A", "A", mockTime.NowAsEpochMilliseconds),
//                    new KeyValueTimestamp<string, string>("A", "A:A", mockTime.NowAsEpochMilliseconds),
//                    new KeyValueTimestamp<string, string>("B", "B", mockTime.NowAsEpochMilliseconds),
//                    new KeyValueTimestamp<string, string>("B", "B:B", mockTime.NowAsEpochMilliseconds),
//                    new KeyValueTimestamp<string, string>("C", "C", mockTime.NowAsEpochMilliseconds),
//                    new KeyValueTimestamp<string, string>("C", "C:C", mockTime.NowAsEpochMilliseconds),
//                    new KeyValueTimestamp<string, string>("D", "D", mockTime.NowAsEpochMilliseconds),
//                    new KeyValueTimestamp<string, string>("D", "D:D", mockTime.NowAsEpochMilliseconds),
//                    new KeyValueTimestamp<string, string>("E", "E", mockTime.NowAsEpochMilliseconds),
//                    new KeyValueTimestamp<string, string>("E", "E:E", mockTime.NowAsEpochMilliseconds)));
//        }

//        private static int Compare<K, V>(KeyValueTimestamp<K, V> o1, KeyValueTimestamp<K, V> o2)
//        {
//            int keyComparison = o1.Key.CompareTo(o2.Key);
//            if (keyComparison == 0)
//            {
//                int valueComparison = o1.Value.CompareTo(o2.Value);
//                if (valueComparison == 0)
//                {
//                    return long.Compare(o1.Timestamp, o2.Timestamp);
//                }
//                return valueComparison;
//            }
//            return keyComparison;
//        }

//        [Fact]
//        public void shouldReduceWindowed()
//        {// throws Exception
//            long firstBatchTimestamp = mockTime.NowAsEpochMilliseconds;
//            mockTime.Sleep(1000);
//            produceMessages(firstBatchTimestamp);
//            long secondBatchTimestamp = mockTime.NowAsEpochMilliseconds;
//            produceMessages(secondBatchTimestamp);
//            produceMessages(secondBatchTimestamp);

//            ISerde<IWindowed<string>> windowedSerde = WindowedSerdes.TimeWindowedSerdeFrom<string>();
//            groupedStream
//                    .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(500L)))
//                    .Reduce(reducer)
//                    .ToStream()
//                    .To(outputTopic, Produced.With(windowedSerde, Serdes.String()));

//            StartStreams();

//            List<KeyValueTimestamp<IWindowed<string>, string>> windowedOutput = receiveMessages(
//                new TimeWindowedDeserializer<>(),
//                Serdes.String().Deserializer,
//                Serdes.String().Deserializer,
//                15);

//            // read from ConsoleConsumer
//            string resultFromConsoleConsumer = readWindowedKeyedMessagesViaConsoleConsumer(
//                new TimeWindowedDeserializer<string>(),
//                Serdes.String().Deserializer,
//                Serdes.String().Deserializer,
//                15,
//                true);

//            Comparator<KeyValueTimestamp<IWindowed<string>, string>> comparator =
//                Comparator.comparing((KeyValueTimestamp<IWindowed<string>, string> o) => o.Key.Key)
//                    .thenComparing(KeyValueTimestamp::value);

//            windowedOutput.Sort(comparator);
//            long firstBatchWindow = firstBatchTimestamp / 500 * 500;
//            long secondBatchWindow = secondBatchTimestamp / 500 * 500;

//            List<KeyValueTimestamp<IWindowed<string>, string>> expectResult = Arrays.asList(
//                    new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(firstBatchWindow, long.MaxValue)), "A", firstBatchTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(secondBatchWindow, long.MaxValue)), "A", secondBatchTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(secondBatchWindow, long.MaxValue)), "A:A", secondBatchTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(firstBatchWindow, long.MaxValue)), "B", firstBatchTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(secondBatchWindow, long.MaxValue)), "B", secondBatchTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(secondBatchWindow, long.MaxValue)), "B:B", secondBatchTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("C", new TimeWindow(firstBatchWindow, long.MaxValue)), "C", firstBatchTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("C", new TimeWindow(secondBatchWindow, long.MaxValue)), "C", secondBatchTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("C", new TimeWindow(secondBatchWindow, long.MaxValue)), "C:C", secondBatchTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("D", new TimeWindow(firstBatchWindow, long.MaxValue)), "D", firstBatchTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("D", new TimeWindow(secondBatchWindow, long.MaxValue)), "D", secondBatchTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("D", new TimeWindow(secondBatchWindow, long.MaxValue)), "D:D", secondBatchTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("E", new TimeWindow(firstBatchWindow, long.MaxValue)), "E", firstBatchTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("E", new TimeWindow(secondBatchWindow, long.MaxValue)), "E", secondBatchTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("E", new TimeWindow(secondBatchWindow, long.MaxValue)), "E:E", secondBatchTimestamp));

//            Assert.Equal(windowedOutput, expectResult);

//            HashSet<string> expectResultString = new HashSet<string>(expectResult.Count);
//            foreach (KeyValueTimestamp<IWindowed<string>, string> eachRecord in expectResult)
//            {
//                expectResultString.Add("CreateTime:" + eachRecord.Timestamp + ", "
//                    + eachRecord.Key + ", " + eachRecord.Value);
//            }

//            // check every message is contained in the expect result
//            string[] allRecords = resultFromConsoleConsumer.Split("\n");
//            foreach (string record in allRecords)
//            {
//                Assert.Contains(record, expectResultString);
//            }
//        }

//        [Fact]
//        public void shouldAggregate()
//        {// throws Exception
//            produceMessages(mockTime.NowAsEpochMilliseconds);
//            groupedStream.Aggregate(
//                initializer,
//                aggregator,
//                Materialized.As("aggregate-by-selected-key"))
//                .ToStream()
//                .To(outputTopic, Produced.With(Serdes.String(), Serdes.Int()));

//            StartStreams();

//            produceMessages(mockTime.NowAsEpochMilliseconds);

//            List<KeyValueTimestamp<string, int>> results = receiveMessages(
//                Serdes.String().Deserializer,
//                Serializers.Int32,
//                10);

//            results.Sort();//KStreamAggregationIntegrationTest);

//            Assert.Equal(results, (Arrays.asList(
//                new KeyValueTimestamp<string, int>("A", 1, mockTime.UtcNow),
//                new KeyValueTimestamp<string, int>("A", 2, mockTime.UtcNow),
//                new KeyValueTimestamp<string, int>("B", 1, mockTime.UtcNow),
//                new KeyValueTimestamp<string, int>("B", 2, mockTime.UtcNow),
//                new KeyValueTimestamp<string, int>("C", 1, mockTime.UtcNow),
//                new KeyValueTimestamp<string, int>("C", 2, mockTime.UtcNow),
//                new KeyValueTimestamp<string, int>("D", 1, mockTime.UtcNow),
//                new KeyValueTimestamp<string, int>("D", 2, mockTime.UtcNow),
//                new KeyValueTimestamp<string, int>("E", 1, mockTime.UtcNow),
//                new KeyValueTimestamp<string, int>("E", 2, mockTime.UtcNow)
//            )));
//        }

//        [Fact]
//        public void shouldAggregateWindowed()
//        {// throws Exception
//            long firstTimestamp = mockTime.NowAsEpochMilliseconds;
//            mockTime.Sleep(1000);
//            produceMessages(firstTimestamp);
//            long secondTimestamp = mockTime.NowAsEpochMilliseconds;
//            produceMessages(secondTimestamp);
//            produceMessages(secondTimestamp);

//            ISerde<IWindowed<string>> windowedSerde = WindowedSerdes.TimeWindowedSerdeFrom<string>();
//            groupedStream.WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(500L)))
//                    .Aggregate(
//                            initializer,
//                            aggregator,
//                            Materialized.With(null, Serdes.Int())
//                    )
//                    .ToStream()
//                    .To(outputTopic, Produced.With(windowedSerde, Serdes.Int()));

//            StartStreams();

//            List<KeyValueTimestamp<IWindowed<string>, int>> windowedMessages = ReceiveMessagesWithTimestamp(
//                new TimeWindowedDeserializer<>(),
//                Serializers.Int32,
//                Serializers.Utf8,
//                15);

//            // read from ConsoleConsumer
//            string resultFromConsoleConsumer = readWindowedKeyedMessagesViaConsoleConsumer(
//                new TimeWindowedDeserializer<string>(),
//                Serializers.Int32,
//                Serializers.Utf8,
//                15,
//                true);

//            Comparator<KeyValueTimestamp<IWindowed<string>, int>> comparator =
//                Comparator.comparing((KeyValueTimestamp<IWindowed<string>, int> o) => o.Key.Key)
//                    .ThenComparingInt(KeyValueTimestamp);
//            windowedMessages.sort(comparator);

//            long firstWindow = firstTimestamp / 500 * 500;
//            long secondWindow = secondTimestamp / 500 * 500;

//            List<KeyValueTimestamp<IWindowed<string>, int>> expectResult = Arrays.asList(
//                    new KeyValueTimestamp<IWindowed<string>, int>(new Windowed<string>("A", new TimeWindow(firstWindow, long.MaxValue)), 1, firstTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, int>(new Windowed<string>("A", new TimeWindow(secondWindow, long.MaxValue)), 1, secondTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, int>(new Windowed<string>("A", new TimeWindow(secondWindow, long.MaxValue)), 2, secondTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, int>(new Windowed<string>("B", new TimeWindow(firstWindow, long.MaxValue)), 1, firstTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, int>(new Windowed<string>("B", new TimeWindow(secondWindow, long.MaxValue)), 1, secondTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, int>(new Windowed<string>("B", new TimeWindow(secondWindow, long.MaxValue)), 2, secondTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, int>(new Windowed<string>("C", new TimeWindow(firstWindow, long.MaxValue)), 1, firstTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, int>(new Windowed<string>("C", new TimeWindow(secondWindow, long.MaxValue)), 1, secondTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, int>(new Windowed<string>("C", new TimeWindow(secondWindow, long.MaxValue)), 2, secondTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, int>(new Windowed<string>("D", new TimeWindow(firstWindow, long.MaxValue)), 1, firstTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, int>(new Windowed<string>("D", new TimeWindow(secondWindow, long.MaxValue)), 1, secondTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, int>(new Windowed<string>("D", new TimeWindow(secondWindow, long.MaxValue)), 2, secondTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, int>(new Windowed<string>("E", new TimeWindow(firstWindow, long.MaxValue)), 1, firstTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, int>(new Windowed<string>("E", new TimeWindow(secondWindow, long.MaxValue)), 1, secondTimestamp),
//                    new KeyValueTimestamp<IWindowed<string>, int>(new Windowed<string>("E", new TimeWindow(secondWindow, long.MaxValue)), 2, secondTimestamp)
//                );

//            Assert.Equal(windowedMessages, expectResult);

//            HashSet<string> expectResultString = new HashSet<string>(expectResult.Count);
//            foreach (KeyValueTimestamp<IWindowed<string>, int> eachRecord in expectResult)
//            {
//                expectResultString.Add("CreateTime:" + eachRecord.Timestamp + ", " + eachRecord.Key + ", " + eachRecord.Value);
//            }

//            // check every message is contained in the expect result
//            string[] allRecords = resultFromConsoleConsumer.Split("\n");
//            foreach (string record in allRecords)
//            {
//                Assert.Contains(record, expectResultString);
//            }

//        }

//        private void shouldCountHelper()
//        {// throws Exception
//            StartStreams();

//            produceMessages(mockTime.NowAsEpochMilliseconds);

//            List<KeyValueTimestamp<string, long>> results = receiveMessages(
//                Serdes.String().Deserializer,
//                Serdes.Long().Deserializer,
//                10);

//            results.Sort(KStreamAggregationIntegrationTest.Compare);

//            Assert.Equal(results, (Arrays.asList(
//                new KeyValueTimestamp<string, long>("A", 1L, mockTime.NowAsEpochMilliseconds),
//                new KeyValueTimestamp<string, long>("A", 2L, mockTime.NowAsEpochMilliseconds),
//                new KeyValueTimestamp<string, long>("B", 1L, mockTime.NowAsEpochMilliseconds),
//                new KeyValueTimestamp<string, long>("B", 2L, mockTime.NowAsEpochMilliseconds),
//                new KeyValueTimestamp<string, long>("C", 1L, mockTime.NowAsEpochMilliseconds),
//                new KeyValueTimestamp<string, long>("C", 2L, mockTime.NowAsEpochMilliseconds),
//                new KeyValueTimestamp<string, long>("D", 1L, mockTime.NowAsEpochMilliseconds),
//                new KeyValueTimestamp<string, long>("D", 2L, mockTime.NowAsEpochMilliseconds),
//                new KeyValueTimestamp<string, long>("E", 1L, mockTime.NowAsEpochMilliseconds),
//                new KeyValueTimestamp<string, long>("E", 2L, mockTime.NowAsEpochMilliseconds)
//            )));
//        }

//        [Fact]
//        public void shouldCount()
//        {// throws Exception
//            produceMessages(mockTime.NowAsEpochMilliseconds);

//            groupedStream.Count(Materialized.As("count-by-key"))
//                    .ToStream()
//                    .To(outputTopic, Produced.With(Serdes.String(), Serdes.Long()));

//            shouldCountHelper();
//        }

//        [Fact]
//        public void shouldCountWithInternalStore()
//        {// throws Exception
//            produceMessages(mockTime.NowAsEpochMilliseconds);

//            groupedStream.Count()
//                    .ToStream()
//                    .To(outputTopic, Produced.With(Serdes.String(), Serdes.Long()));

//            shouldCountHelper();
//        }

//        [Fact]
//        public void shouldGroupByKey()
//        {// throws Exception
//            long timestamp = mockTime.NowAsEpochMilliseconds;
//            produceMessages(timestamp);
//            produceMessages(timestamp);

//            stream.GroupByKey(Grouped.With(Serdes.Int(), Serdes.String()))
//                    .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(500L)))
//                    .Count()
//                    .ToStream((windowedKey, value) => windowedKey.Key + "@" + windowedKey.window().Start()).To(outputTopic, Produced.With(Serdes.String(), Serdes.Long()));

//            StartStreams();

//            List<KeyValueTimestamp<string, long>> results = receiveMessages(
//                Serdes.String().Deserializer,
//                Serdes.Long().Deserializer,
//                10);

//            results.Sort(KStreamAggregationIntegrationTest.Compare);

//            long window = timestamp / 500 * 500;
//            Assert.Equal(results, Arrays.asList(
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
//            ));
//        }

//        [Fact]
//        public void shouldCountSessionWindows()
//        {// throws Exception
//            long sessionGap = 5 * 60 * 1000L;
//            List<KeyValuePair<string, string>> t1Messages = Arrays.asList(
//                KeyValuePair.Create("bob", "start"),
//                KeyValuePair.Create("penny", "start"),
//                KeyValuePair.Create("jo", "pause"),
//                KeyValuePair.Create("emily", "pause"));

//            long t1 = mockTime.NowAsEpochMilliseconds - TimeUnit.MILLISECONDS.Convert(1, TimeUnit.HOURS);
//            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
//                    userSessionsStream,
//                    t1Messages,
//                    TestUtils.ProducerConfig(
//                            CLUSTER.bootstrapServers(),
//                            Serdes.String().Serializer,
//                            Serdes.String().Serializer,
//                            new StreamsConfig()),
//                    t1);
//            long t2 = t1 + (sessionGap / 2);
//            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
//                    userSessionsStream,
//                    Collections.singletonList(
//                            KeyValuePair.Create("emily", "resume")),
//                    TestUtils.ProducerConfig(
//                            CLUSTER.bootstrapServers(),
//                            Serdes.String().Serializer,
//                            Serdes.String().Serializer,
//                            new StreamsConfig()),
//                    t2);
//            long t3 = t1 + sessionGap + 1;
//            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
//                    userSessionsStream,
//                    Arrays.asList(
//                            KeyValuePair.Create("bob", "pause"),
//                            KeyValuePair.Create("penny", "stop")
//                    ),
//                    TestUtils.ProducerConfig(
//                            CLUSTER.bootstrapServers(),
//                            Serdes.String().Serializer,
//                            Serdes.String().Serializer,
//                            new StreamsConfig()),
//                    t3);
//            long t4 = t3 + (sessionGap / 2);
//            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
//                    userSessionsStream,
//                    Arrays.asList(
//                            KeyValuePair.Create("bob", "resume"), // bobs session continues
//                            KeyValuePair.Create("jo", "resume")   // jo's starts new session
//                    ),
//                    TestUtils.ProducerConfig(
//                            CLUSTER.bootstrapServers(),
//                            Serdes.String().Serializer,
//                            Serdes.String().Serializer,
//                            new StreamsConfig()),
//                    t4);
//            long t5 = t4 - 1;
//            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
//                userSessionsStream,
//                Collections.singletonList(
//                    KeyValuePair.Create("jo", "late")   // jo has late arrival
//                ),
//                TestUtils.ProducerConfig(
//                    CLUSTER.bootstrapServers(),
//                    Serdes.String().Serializer,
//                    Serdes.String().Serializer,
//                    new StreamsConfig()),
//                t5);

//            Dictionary<IWindowed<string>, KeyValuePair<long, long>> results = new Dictionary<IWindowed<string>, KeyValuePair<long, long>>();
//            // CountDownLatch latch = new CountDownLatch(13);

//            builder.Stream(userSessionsStream, Consumed.With(Serdes.String(), Serdes.String()))
//                .GroupByKey(Grouped.With(Serdes.String(), Serdes.String()))
//                .WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(sessionGap)))
//                .Count()
//                .ToStream()
//                .Transform(() => new Transformer<IWindowed<string>, long, KeyValuePair<object, object>>());
//            //                    {
//            //                                private ProcessorContext context;
//            //
//            //
//            //        public void Init(IProcessorContext context)
//            //        {
//            //            this.context = context;
//            //        }
//            //
//            //
//            //        public KeyValuePair<object, object> transform(IWindowed<string> key, long value)
//            //        {
//            //            results.Put(key, KeyValuePair.Create(value, context.Timestamp));
//            //            latch.countDown();
//            //            return null;
//            //        }
//            //
//            //
//            //        public void Close() { }
//            //    });
//            //
//            //        startStreams();
//            //    latch.await(30, TimeUnit.SECONDS);

//            Assert.Equal(results[new Windowed<string>("bob", new SessionWindow(t1, t1))], (KeyValuePair.Create(1L, t1)));
//            Assert.Equal(results[new Windowed<string>("penny", new SessionWindow(t1, t1))], (KeyValuePair.Create(1L, t1)));
//            Assert.Equal(results[new Windowed<string>("jo", new SessionWindow(t1, t1))], (KeyValuePair.Create(1L, t1)));
//            Assert.Equal(results[new Windowed<string>("jo", new SessionWindow(t5, t4))], (KeyValuePair.Create(2L, t4)));
//            Assert.Equal(results[new Windowed<string>("emily", new SessionWindow(t1, t2))], (KeyValuePair.Create(2L, t2)));
//            Assert.Equal(results[new Windowed<string>("bob", new SessionWindow(t3, t4))], (KeyValuePair.Create(2L, t4)));
//            Assert.Equal(results[new Windowed<string>("penny", new SessionWindow(t3, t3))], (KeyValuePair.Create(1L, t3)));
//        }

//        [Fact]
//        public void shouldReduceSessionWindows()
//        {// throws Exception
//            long sessionGap = 1000L; // something to do with time
//            List<KeyValuePair<string, string>> t1Messages = Arrays.asList(
//                KeyValuePair.Create("bob", "start"),
//                KeyValuePair.Create("penny", "start"),
//                KeyValuePair.Create("jo", "pause"),
//                KeyValuePair.Create("emily", "pause"));

//            long t1 = mockTime.NowAsEpochMilliseconds;
//            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
//                    userSessionsStream,
//                    t1Messages,
//                    TestUtils.ProducerConfig(
//                            CLUSTER.bootstrapServers(),
//                            Serdes.String().Serializer,
//                            Serdes.String().Serializer,
//                            new StreamsConfig()),
//                    t1);
//            long t2 = t1 + (sessionGap / 2);
//            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
//                    userSessionsStream,
//                    Collections.singletonList(
//                            KeyValuePair.Create("emily", "resume")
//                    ),
//                    TestUtils.ProducerConfig(
//                            CLUSTER.bootstrapServers(),
//                            Serdes.String().Serializer,
//                            Serdes.String().Serializer,
//                            new StreamsConfig()),
//                    t2);
//            long t3 = t1 + sessionGap + 1;
//            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
//                    userSessionsStream,
//                    Arrays.asList(
//                            KeyValuePair.Create("bob", "pause"),
//                            KeyValuePair.Create("penny", "stop")
//                    ),
//                    TestUtils.ProducerConfig(
//                            CLUSTER.bootstrapServers(),
//                            Serdes.String().Serializer,
//                            Serdes.String().Serializer,
//                            new StreamsConfig()),
//                    t3);
//            long t4 = t3 + (sessionGap / 2);
//            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
//                    userSessionsStream,
//                    Arrays.asList(
//                            KeyValuePair.Create("bob", "resume"), // bobs session continues
//                            KeyValuePair.Create("jo", "resume")   // jo's starts new session
//                    ),
//                    TestUtils.ProducerConfig(
//                            CLUSTER.bootstrapServers(),
//                            Serdes.String().Serializer,
//                            Serdes.String().Serializer,
//                            new StreamsConfig()),
//                    t4);
//            long t5 = t4 - 1;
//            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
//                userSessionsStream,
//                Collections.singletonList(
//                    KeyValuePair.Create("jo", "late")   // jo has late arrival
//                ),
//                TestUtils.ProducerConfig(
//                    CLUSTER.bootstrapServers(),
//                    Serdes.String().Serializer,
//                    Serdes.String().Serializer,
//                    new StreamsConfig()),
//                t5);

//            Dictionary<IWindowed<string>, KeyValuePair<string, long>> results = new Dictionary<IWindowed<string>, KeyValuePair<string, long>>();
//            // CountDownLatch latch = new CountDownLatch(13);
//            string userSessionsStore = "UserSessionsStore";
//            builder.Stream(userSessionsStream, Consumed.With(Serdes.String(), Serdes.String()))
//                    .GroupByKey(Grouped.With(Serdes.String(), Serdes.String()))
//                    .WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(sessionGap)))
//                    .Reduce((value1, value2) => value1 + ":" + value2, Materialized.As(userSessionsStore))
//                    .ToStream()
//                .Transform(() => new Transformer<IWindowed<string>, string, KeyValuePair<object, object>>());
//            //        {
//            //                private ProcessorContext context;
//            //
//            //
//            //public void Init(IProcessorContext context)
//            //{
//            //    this.context = context;
//            //}
//            //
//            //
//            //public KeyValuePair<object, object> transform(IWindowed<string> key, string value)
//            //{
//            //    results.Put(key, KeyValuePair.Create(value, context.Timestamp));
//            //    latch.countDown();
//            //    return null;
//            //}
//            //
//            //
//            //public void Close() { }
//            //            });
//            //
//            //        startStreams();
//            //latch.await(30, TimeUnit.SECONDS);

//            // verify correct data received
//            Assert.Equal(results[new Windowed<string>("bob", new SessionWindow(t1, t1))], (KeyValuePair.Create("start", t1)));
//            Assert.Equal(results[new Windowed<string>("penny", new SessionWindow(t1, t1))], (KeyValuePair.Create("start", t1)));
//            Assert.Equal(results[new Windowed<string>("jo", new SessionWindow(t1, t1))], (KeyValuePair.Create("pause", t1)));
//            Assert.Equal(results[new Windowed<string>("jo", new SessionWindow(t5, t4))], (KeyValuePair.Create("resume:late", t4)));
//            Assert.Equal(results[new Windowed<string>("emily", new SessionWindow(t1, t2))], (KeyValuePair.Create("pause:resume", t2)));
//            Assert.Equal(results[new Windowed<string>("bob", new SessionWindow(t3, t4))], (KeyValuePair.Create("pause:resume", t4)));
//            Assert.Equal(results[new Windowed<string>("penny", new SessionWindow(t3, t3))], (KeyValuePair.Create("stop", t3)));

//            // verify can query data via IQ
//            IReadOnlySessionStore<string, string> sessionStore =
//                kafkaStreams.Store(userSessionsStore, QueryableStoreTypes.SessionStore);
//            IKeyValueIterator<IWindowed<string>, string> bob = sessionStore.Fetch("bob");
//            Assert.Equal(bob.Current, (KeyValuePair.Create(new Windowed<string>("bob", new SessionWindow(t1, t1)), "start")));
//            Assert.Equal(bob.Current, (KeyValuePair.Create(new Windowed<string>("bob", new SessionWindow(t3, t4)), "pause:resume")));
//            Assert.False(bob.MoveNext());
//        }

//        [Fact]
//        public void shouldCountUnlimitedWindows()
//        {// throws Exception
//            long startTime = mockTime.NowAsEpochMilliseconds - TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS) + 1;
//            long incrementTime = (long)TimeSpan.FromDays(1).TotalMilliseconds;

//            long t1 = mockTime.NowAsEpochMilliseconds - TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
//            List<KeyValuePair<string, string>> t1Messages = Arrays.asList(
//                KeyValuePair.Create("bob", "start"),
//                KeyValuePair.Create("penny", "start"),
//                KeyValuePair.Create("jo", "pause"),
//                KeyValuePair.Create("emily", "pause"));

//            StreamsConfig ProducerConfig = TestUtils.ProducerConfig(
//                CLUSTER.bootstrapServers(),
//                Serdes.String().Serializer,
//                Serdes.String().Serializer,
//                new StreamsConfig()
//            );

//            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
//                userSessionsStream,
//                t1Messages,
//                ProducerConfig,
//                t1);

//            long t2 = t1 + incrementTime;
//            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
//                userSessionsStream,
//                Collections.singletonList(
//                    KeyValuePair.Create("emily", "resume")
//                ),
//                ProducerConfig,
//                t2);
//            long t3 = t2 + incrementTime;
//            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
//                userSessionsStream,
//                Arrays.asList(
//                    KeyValuePair.Create("bob", "pause"),
//                    KeyValuePair.Create("penny", "stop")
//                ),
//                ProducerConfig,
//                t3);

//            long t4 = t3 + incrementTime;
//            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
//                userSessionsStream,
//                Arrays.asList(
//                    KeyValuePair.Create("bob", "resume"), // bobs session continues
//                    KeyValuePair.Create("jo", "resume")   // jo's starts new session
//                ),
//                ProducerConfig,
//                t4);

//            Dictionary<IWindowed<string>, KeyValuePair<long, long>> results = new Dictionary<IWindowed<string>, KeyValuePair<long, long>>();
//            CountDownLatch latch = new CountDownLatch(5);

//            builder.Stream(userSessionsStream, Consumed.With(Serdes.String(), Serdes.String()))
//                   .GroupByKey(Grouped.With(Serdes.String(), Serdes.String()))
//                   .WindowedBy(UnlimitedWindows.Of().startOn(Timestamp.UnixTimestampMsToDateTime(startTime)))
//                   .Count()
//                   .ToStream()
//                   .transform(() => new Transformer<IWindowed<string>, long, KeyValuePair<object, object>>());
//            //                   {
//            //                           private ProcessorContext context;
//            //
//            //
//            //        public void Init(IProcessorContext context)
//            //        {
//            //            this.context = context;
//            //        }
//            //
//            //
//            //        public KeyValuePair<object, object> transform(IWindowed<string> key, long value)
//            //        {
//            //            results.Put(key, KeyValuePair.Create(value, context.Timestamp));
//            //            latch.countDown();
//            //            return null;
//            //        }
//            //
//            //
//            //        public void Close() { }
//            //    });
//            StartStreams();
//            Assert.True(latch.wait(30, TimeUnit.SECONDS));

//            Assert.Equal(results[new Windowed<string>("bob", new UnlimitedWindow(startTime))], (KeyValuePair.Create(2L, t4)));
//            Assert.Equal(results[new Windowed<string>("penny", new UnlimitedWindow(startTime))], (KeyValuePair.Create(1L, t3)));
//            Assert.Equal(results[new Windowed<string>("jo", new UnlimitedWindow(startTime))], (KeyValuePair.Create(1L, t4)));
//            Assert.Equal(results[new Windowed<string>("emily", new UnlimitedWindow(startTime))], (KeyValuePair.Create(1L, t2)));
//        }


//        private void produceMessages(long timestamp)
//        {// throws Exception
//            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
//                streamOneInput,
//                Arrays.asList(
//                    KeyValuePair.Create(1, "A"),
//                    KeyValuePair.Create(2, "B"),
//                    KeyValuePair.Create(3, "C"),
//                    KeyValuePair.Create(4, "D"),
//                    KeyValuePair.Create(5, "E")),
//                TestUtils.ProducerConfig(
//                    CLUSTER.bootstrapServers(),
//                    Serdes.Int().Serializer,
//                    Serdes.String().Serializer,
//                    new StreamsConfig()),
//                timestamp);
//        }


//        private void createTopics()
//        {// throws InterruptedException
//            streamOneInput = "stream-one-" + testNo;
//            outputTopic = "output-" + testNo;
//            userSessionsStream = userSessionsStream + "-" + testNo;
//            CLUSTER.CreateTopic(streamOneInput, 3, 1);
//            CLUSTER.CreateTopics(userSessionsStream, outputTopic);
//        }

//        private void StartStreams()
//        {
//            kafkaStreams = null; // new KafkaStreamsThread(builder.Build(), streamsConfiguration);
//            kafkaStreams.Start();
//        }

//        private List<KeyValueTimestamp<K, V>> receiveMessages<K, V>(
//            IDeserializer<K> keyDeserializer,
//            IDeserializer<V> valueDeserializer,
//            int numMessages)
//        {    //throws InterruptedException {
//            return receiveMessages(keyDeserializer, valueDeserializer, null, numMessages);
//        }

//        private List<KeyValueTimestamp<K, V>> receiveMessages<K, V>(
//            IDeserializer<K> keyDeserializer,
//            IDeserializer<V> valueDeserializer,
//            Type innerClass,
//            int numMessages)
//        {// throws InterruptedException
//            StreamsConfig consumerProperties = new StreamsConfig();
//            consumerProperties.Set(ConsumerConfig.BootstrapServersConfig, CLUSTER.bootstrapServers());
//            consumerProperties.Set(ConsumerConfig.GROUP_ID_CONFIG, "kgroupedstream-test-" + testNo);
//            consumerProperties.Set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            consumerProperties.Set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.GetType().FullName);
//            consumerProperties.Set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.GetType().FullName);

//            if (keyDeserializer is TimeWindowedDeserializer<K> || keyDeserializer is SessionWindowedDeserializer<K>)
//            {
//                consumerProperties.Set(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASSConfig,
//                        Serdes.SerdeFrom(innerClass).GetType().FullName);
//            }

//            return IntegrationTestUtils.WaitUntilMinKeyValueWithTimestampRecordsReceived<K, V>(
//                    consumerProperties,
//                    outputTopic,
//                    numMessages,
//                    60 * 1000);
//        }

//        private List<KeyValueTimestamp<K, V>> ReceiveMessagesWithTimestamp<K, V>(
//            IDeserializer<K> keyDeserializer,
//            IDeserializer<V> valueDeserializer,
//            Type innerClass,
//            int numMessages)
//        {// throws InterruptedException
//            StreamsConfig consumerProperties = new StreamsConfig();
//            consumerProperties.Set(ConsumerConfig.BootstrapServersConfig, CLUSTER.bootstrapServers());
//            consumerProperties.Set(ConsumerConfig.GROUP_ID_CONFIG, "kgroupedstream-test-" + testNo);
//            consumerProperties.Set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            consumerProperties.Set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.GetType().FullName);
//            consumerProperties.Set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.GetType().FullName);
//            if (keyDeserializer is TimeWindowedDeserializer<K> || keyDeserializer is SessionWindowedDeserializer<K>)
//            {
//                consumerProperties.Set(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASSConfig,
//                    Serdes.SerdeFrom(innerClass).GetType().FullName);
//            }
//            return IntegrationTestUtils.WaitUntilMinKeyValueWithTimestampRecordsReceived<K, V>(
//                consumerProperties,
//                outputTopic,
//                numMessages,
//                60 * 1000);
//        }

//        private string readWindowedKeyedMessagesViaConsoleConsumer<K, V>(
//            IDeserializer<K> keyDeserializer,
//            IDeserializer<V> valueDeserializer,
//            Type innerClass,
//            int numMessages,
//            bool printTimestamp)
//        {
//            //ByteArrayOutputStream newConsole = new ByteArrayOutputStream();
//            //PrintStream originalStream = System.Console.Out;
//            //PrintStream newStream = new PrintStream(newConsole);
//            //System.setOut(newStream);

//            string keySeparator = ", ";
//            // manually construct the console consumer argument array
//            string[] args = new string[] {
//                "--bootstrap-server", CLUSTER.bootstrapServers(),
//                "--from-beginning",
//                "--property", "print.Key=true",
//                "--property", "print.timestamp=" + printTimestamp,
//                "--topic", outputTopic,
//                "--max-messages", numMessages.ToString(),
//                "--property", "key.deserializer=" + keyDeserializer.GetType().FullName,
//                "--property", "value.deserializer=" + valueDeserializer.GetType().FullName,
//                "--property", "key.separator=" + keySeparator,
//                "--property", "key.deserializer." + StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASSConfig + "=" + Serdes.SerdeFrom(innerClass).GetType().FullName
//            };

//            // ConsoleConsumer.messageCount(0); //reset the message count
//            // ConsoleConsumer.run(new ConsoleConsumer.ConsumerConfig(args));
//            //newStream.Flush();
//            //System.setOut(originalStream);
//            return ""; // newConsole.ToString();
//        }
//    }
//}
