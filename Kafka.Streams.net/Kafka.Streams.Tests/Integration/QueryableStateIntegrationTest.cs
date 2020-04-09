//using Confluent.Kafka;
//using Kafka.Common;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Mappers;
//using Kafka.Streams.State;
//using Kafka.Streams.State.ReadOnly;
//using Kafka.Streams.State.Windowed;
//using Kafka.Streams.Tests.Helpers;
//using Kafka.Streams.Tests.Mocks;
//using System;
//using System.Collections.Generic;
//using System.IO;
//using System.Runtime.CompilerServices;

//namespace Kafka.Streams.Tests.Integration
//{
//    public class QueryableStateIntegrationTest
//    {
//        private static Logger log = LoggerFactory.getLogger(QueryableStateIntegrationTest);

//        private const int NUM_BROKERS = 1;


//        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
//        private const int STREAM_THREE_PARTITIONS = 4;
//        private MockTime mockTime = CLUSTER.time;
//        private string streamOne = "stream-one";
//        private string streamTwo = "stream-two";
//        private string streamThree = "stream-three";
//        private string streamConcurrent = "stream-concurrent";
//        private string outputTopic = "output";
//        private string outputTopicConcurrent = "output-concurrent";
//        private string outputTopicConcurrentWindowed = "output-concurrent-windowed";
//        private string outputTopicThree = "output-three";
//        // sufficiently large window size such that everything falls into 1 window
//        private static readonly long WINDOW_SIZE = TimeUnit.MILLISECONDS.convert(2, TimeUnit.DAYS);
//        private const int STREAM_TWO_PARTITIONS = 2;
//        private const int NUM_REPLICAS = NUM_BROKERS;
//        private StreamsConfig streamsConfiguration;
//        private List<string> inputValues;
//        private int numberOfWordsPerIteration = 0;
//        private HashSet<string> inputValuesKeys;
//        private KafkaStreams kafkaStreams;
//        private Comparator<KeyValuePair<string, string>> stringComparator;
//        private Comparator<KeyValuePair<string, long>> stringLongComparator;
//        private static volatile AtomicInteger testNo = new AtomicInteger(0);

//        private void CreateTopics()
//        {// throws Exception
//            streamOne = streamOne + "-" + testNo;
//            streamConcurrent = streamConcurrent + "-" + testNo;
//            streamThree = streamThree + "-" + testNo;
//            outputTopic = outputTopic + "-" + testNo;
//            outputTopicConcurrent = outputTopicConcurrent + "-" + testNo;
//            outputTopicConcurrentWindowed = outputTopicConcurrentWindowed + "-" + testNo;
//            outputTopicThree = outputTopicThree + "-" + testNo;
//            streamTwo = streamTwo + "-" + testNo;
//            CLUSTER.createTopics(streamOne, streamConcurrent);
//            CLUSTER.createTopic(streamTwo, STREAM_TWO_PARTITIONS, NUM_REPLICAS);
//            CLUSTER.createTopic(streamThree, STREAM_THREE_PARTITIONS, 1);
//            CLUSTER.createTopics(outputTopic, outputTopicConcurrent, outputTopicConcurrentWindowed, outputTopicThree);
//        }

//        /**
//         * Try to read inputValues from {@code resources/QueryableStateIntegrationTest/inputValues.txt}, which might be useful
//         * for larger scale testing. In case of exception, for instance if no such file can be read, return a small list
//         * which satisfies all the prerequisites of the tests.
//         */
//        private List<string> GetInputValues()
//        {
//            List<string> input = new ArrayList<>();
//            ClassLoader classLoader = getClass().getClassLoader();
//            string fileName = "QueryableStateIntegrationTest" + Path.DirectorySeparatorChar + "inputValues.txt";
//            BufferedReader reader = new BufferedReader(
//                new FileReader(Objects.requireNonNull(classLoader.getResource(fileName)).getFile()));

//            for (string line = reader.readLine(); line != null; line = reader.readLine())
//            {
//                input.Add(line);
//            }
//            log.warn("Unable to read '{}{}{}'. Using default inputValues list", "resources", Path.DirectorySeparatorChar, fileName);
//            input = Array.asList(
//                        "hello world",
//                        "all streams lead to kafka",
//                        "streams",
//                        "kafka streams",
//                        "the cat in the hat",
//                        "green eggs and ham",
//                        "that Sam i am",
//                        "up the creek without a paddle",
//                        "run forest run",
//                        "a tank full of gas",
//                        "eat sleep rave repeat",
//                        "one jolly sailor",
//                        "king of the world");

//            return input;
//        }


//        public void Before()
//        {// throws Exception
//            CreateTopics();
//            streamsConfiguration = new StreamsConfig();
//            string applicationId = "queryable-state-" + testNo.incrementAndGet();

//            streamsConfiguration.Set(StreamsConfigPropertyNames.ApplicationId, applicationId);
//            streamsConfiguration.Set(StreamsConfigPropertyNames.BootstrapServers, CLUSTER.bootstrapServers());
//            //streamsConfiguration.Set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            streamsConfiguration.Set(StreamsConfigPropertyNames.STATE_DIR_CONFIG, TestUtils.GetTempDirectory("qs-test").getPath());
//            streamsConfiguration.Set(StreamsConfigPropertyNames.DefaultKeySerdeClass, Serdes.String().getClass());
//            streamsConfiguration.Set(StreamsConfigPropertyNames.DefaultValueSerdeClass, Serdes.String().getClass());
//            streamsConfiguration.Set(StreamsConfigPropertyNames.COMMIT_INTERVAL_MS_CONFIG, 100);

//            stringComparator = Comparator.comparing((KeyValuePair<string, string> o) => o.key).thenComparing(o => o.value);
//            stringLongComparator = Comparator.comparing((KeyValuePair<string, long> o) => o.key).thenComparingLong(o => o.value);
//            inputValues = GetInputValues();
//            inputValuesKeys = new HashSet<string>();
//            foreach (string sentence in inputValues)
//            {
//                string[] words = sentence.Split("\\W+");
//                numberOfWordsPerIteration += words.Length;
//                Collections.addAll(inputValuesKeys, words);
//            }
//        }


//        public void Shutdown()
//        {// throws Exception
//            if (kafkaStreams != null)
//            {
//                kafkaStreams.close(ofSeconds(30));
//            }
//            IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
//        }

//        /**
//         * Creates a typical word count topology
//         */
//        private KafkaStreams CreateCountStream(string inputTopic,
//                                               string outputTopic,
//                                               string windowOutputTopic,
//                                               string storeName,
//                                               string windowStoreName,
//                                               StreamsConfig streamsConfiguration)
//        {
//            StreamsBuilder builder = new StreamsBuilder();
//            ISerde<string> stringSerde = Serdes.String();
//            IKStream<string, string> textLines = builder.Stream(inputTopic, Consumed.With(stringSerde, stringSerde));

//            IKGroupedStream<string, string> groupedByWord = textLines
//                .FlatMapValues((ValueMapper<string, Iterable<string>>)value => Array.asList(value.Split("\\W+")))
//                .GroupBy(MockMapper.selectValueMapper());

//            // Create a State Store for the all time word count
//            groupedByWords
//                .Count(Materialized.As(storeName + "-" + inputTopic))
//                .ToStream()
//                .To(outputTopic, Produced.With(Serdes.String(), Serdes.Long()));

//            // Create a Windowed State Store that contains the word count for every 1 minute
//            groupedByWord
//                .windowedBy(TimeWindows.of(FromMilliseconds(WINDOW_SIZE)))
//                .count(Materialized.As(windowStoreName + "-" + inputTopic))
//                .toStream((key, value) => key.Key)
//                .To(windowOutputTopic, Produced.With(Serdes.String(), Serdes.Long()));

//            return new KafkaStreams(builder.Build(), streamsConfiguration);
//        }

//        private class StreamRunnable : Runnable
//        {
//            private KafkaStreams myStream;
//            private bool closed = false;
//            private KafkaStreamsTest.StateListenerStub stateListener = new KafkaStreamsTest.StateListenerStub();

//            StreamRunnable(string inputTopic,
//                           string outputTopic,
//                           string outputTopicWindowed,
//                           string storeName,
//                           string windowStoreName,
//                           int queryPort)
//            {
//                StreamsConfig props = (StreamsConfig)streamsConfiguration.clone();
//                props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + queryPort);
//                myStream = CreateCountStream(inputTopic, outputTopic, outputTopicWindowed, storeName, windowStoreName, props);
//                myStream.setStateListener(stateListener);
//            }


//            public void Run()
//            {
//                myStream.start();
//            }

//            public void Close()
//            {
//                if (!closed)
//                {
//                    myStream.close();
//                    closed = true;
//                }
//            }

//            public bool IsClosed()
//            {
//                return closed;
//            }

//            public KafkaStreams GetStream()
//            {
//                return myStream;
//            }

//            StateListenerStub GetStateListener()
//            {
//                return stateListener;
//            }
//        }

//        private void VerifyAllKVKeys(StreamRunnable[] streamRunnables,
//                                     KafkaStreams streams,
//                                     KafkaStreamsTest.StateListenerStub stateListenerStub,
//                                     HashSet<string> keys,
//                                     string storeName)
//        {// throws Exception
//            foreach (string key in keys)
//            {
//                TestUtils.WaitForCondition(
//                    () =>
//                    {
//                        try
//                        {
//                            StreamsMetadata metadata = streams.metadataForKey(storeName, key, new Utf8Serializer());

//                            if (metadata == null || metadata.equals(StreamsMetadata.NOT_AVAILABLE))
//                            {
//                                return false;
//                            }
//                            int index = metadata.hostInfo().port();
//                            KafkaStreams streamsWithKey = streamRunnables[index].GetStream();
//                            IReadOnlyKeyValueStore<string, long> store =
//                                streamsWithKey.store(storeName, QueryableStoreTypes.KeyValueStore());

//                            return store != null && store.Get(key) != null;
//                        }
//                        catch (IllegalStateException e)
//                        {
//                            // Kafka Streams instance may have closed but rebalance hasn't happened
//                            return false;
//                        }
//                        catch (InvalidStateStoreException e)
//                        {
//                            // there must have been at least one rebalance state
//                            Assert.True(stateListenerStub.mapStates.Get(KafkaStreams.State.REBALANCING) >= 1);
//                            return false;
//                        }
//                    },
//                    120000,
//                    "waiting for metadata, store and value to be non null");
//            }
//        }

//        private void VerifyAllWindowedKeys(StreamRunnable[] streamRunnables,
//                                           KafkaStreams streams,
//                                           KafkaStreamsTest.StateListenerStub stateListenerStub,
//                                           HashSet<string> keys,
//                                           string storeName,
//                                           long from,
//                                           long to)
//        {// throws Exception
//            foreach (string key in keys)
//            {
//                TestUtils.WaitForCondition(
//                    () =>
//                    {
//                        try
//                        {
//                            StreamsMetadata metadata = streams.metadataForKey(storeName, key, new Serdes.String().Serializer());
//                            if (metadata == null || metadata.equals(StreamsMetadata.NOT_AVAILABLE))
//                            {
//                                return false;
//                            }
//                            int index = metadata.hostInfo().port();
//                            KafkaStreams streamsWithKey = streamRunnables[index].GetStream();
//                            IReadOnlyWindowStore<string, long> store =
//                                streamsWithKey.store(storeName, QueryableStoreTypes.windowStore());
//                            return store != null && store.Fetch(key, ofEpochMilli(from), ofEpochMilli(to)) != null;
//                        }
//                        catch (IllegalStateException e)
//                        {
//                            // Kafka Streams instance may have closed but rebalance hasn't happened
//                            return false;
//                        }
//                        catch (InvalidStateStoreException e)
//                        {
//                            // there must have been at least one rebalance state
//                            Assert.True(stateListenerStub.mapStates.Get(KafkaStreams.State.REBALANCING) >= 1);
//                            return false;
//                        }
//                    },
//                    120000,
//                    "waiting for metadata, store and value to be non null");
//            }
//        }

//        [Fact]
//        public void QueryOnRebalance()
//        {// throws Exception
//            int numThreads = STREAM_TWO_PARTITIONS;
//            StreamRunnable[] streamRunnables = new StreamRunnable[numThreads];
//            Thread[] streamThreads = new Thread[numThreads];

//            ProducerRunnable producerRunnable = new ProducerRunnable(streamThree, inputValues, 1);
//            producerRunnable.Run();

//            // Create stream threads
//            string storeName = "word-count-store";
//            string windowStoreName = "windowed-word-count-store";
//            for (int i = 0; i < numThreads; i++)
//            {
//                streamRunnables[i] = new StreamRunnable(
//                    streamThree,
//                    outputTopicThree,
//                    outputTopicConcurrentWindowed,
//                    storeName,
//                    windowStoreName,
//                    i);
//                streamThreads[i] = new Thread(streamRunnables[i]);
//                streamThreads[i].start();
//            }

//            try
//            {
//                WaitUntilAtLeastNumRecordProcessed(outputTopicThree, 1);

//                for (int i = 0; i < numThreads; i++)
//                {
//                    VerifyAllKVKeys(
//                        streamRunnables,
//                        streamRunnables[i].GetStream(),
//                        streamRunnables[i].GetStateListener(),
//                        inputValuesKeys,
//                        storeName + "-" + streamThree);
//                    VerifyAllWindowedKeys(
//                        streamRunnables,
//                        streamRunnables[i].GetStream(),
//                        streamRunnables[i].GetStateListener(),
//                        inputValuesKeys,
//                        windowStoreName + "-" + streamThree,
//                        0L,
//                        WINDOW_SIZE);
//                    Assert.Equal(KafkaStreams.State.RUNNING, streamRunnables[i].GetStream().state());
//                }

//                // kill N-1 threads
//                for (int i = 1; i < numThreads; i++)
//                {
//                    streamRunnables[i].Close();
//                    streamThreads[i].interrupt();
//                    streamThreads[i].join();
//                }

//                // query from the remaining thread
//                VerifyAllKVKeys(
//                    streamRunnables,
//                    streamRunnables[0].GetStream(),
//                    streamRunnables[0].GetStateListener(),
//                    inputValuesKeys,
//                    storeName + "-" + streamThree);
//                VerifyAllWindowedKeys(
//                    streamRunnables,
//                    streamRunnables[0].GetStream(),
//                    streamRunnables[0].GetStateListener(),
//                    inputValuesKeys,
//                    windowStoreName + "-" + streamThree,
//                    0L,
//                    WINDOW_SIZE);
//                Assert.Equal(KafkaStreams.State.RUNNING, streamRunnables[0].GetStream().state());
//            }
//            finally
//            {
//                for (int i = 0; i < numThreads; i++)
//                {
//                    if (!streamRunnables[i].IsClosed())
//                    {
//                        streamRunnables[i].Close();
//                        streamThreads[i].interrupt();
//                        streamThreads[i].join();
//                    }
//                }
//            }
//        }

//        [Fact]
//        public void ConcurrentAccesses()
//        {// throws Exception
//            int numIterations = 500000;
//            string storeName = "word-count-store";
//            string windowStoreName = "windowed-word-count-store";

//            ProducerRunnable producerRunnable = new ProducerRunnable(streamConcurrent, inputValues, numIterations);
//            Thread producerThread = new Thread(producerRunnable);
//            kafkaStreams = CreateCountStream(
//                streamConcurrent,
//                outputTopicConcurrent,
//                outputTopicConcurrentWindowed,
//                storeName,
//                windowStoreName,
//                streamsConfiguration);

//            kafkaStreams.start();
//            producerThread.start();

//            try
//            {
//                WaitUntilAtLeastNumRecordProcessed(outputTopicConcurrent, numberOfWordsPerIteration);
//                WaitUntilAtLeastNumRecordProcessed(outputTopicConcurrentWindowed, numberOfWordsPerIteration);

//                IReadOnlyKeyValueStore<string, long> KeyValueStore =
//                    kafkaStreams.store(storeName + "-" + streamConcurrent, QueryableStoreTypes.KeyValueStore());

//                IReadOnlyWindowStore<string, long> windowStore =
//                    kafkaStreams.store(windowStoreName + "-" + streamConcurrent, QueryableStoreTypes.windowStore());

//                Dictionary<string, long> expectedWindowState = new HashMap<>();
//                Dictionary<string, long> expectedCount = new HashMap<>();
//                while (producerRunnable.GetCurrIteration() < numIterations)
//                {
//                    verifyGreaterOrEqual(inputValuesKeys.ToArray(Array.Empty<string>()), expectedWindowState,
//                        expectedCount, windowStore, KeyValueStore, true);
//                }
//            }
//            finally
//            {
//                producerRunnable.shutdown();
//                producerThread.interrupt();
//                producerThread.join();
//            }
//        }

//        [Fact]
//        public void ShouldBeAbleToQueryStateWithZeroSizedCache()
//        {// throws Exception
//            VerifyCanQueryState(0);
//        }

//        [Fact]
//        public void ShouldBeAbleToQueryStateWithNonZeroSizedCache()
//        {// throws Exception
//            VerifyCanQueryState(10 * 1024 * 1024);
//        }

//        [Fact]
//        public void ShouldBeAbleToQueryFilterState()
//        {// throws Exception
//            streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//            streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
//            StreamsBuilder builder = new StreamsBuilder();
//            string[] keys = { "hello", "goodbye", "welcome", "go", "kafka" };
//            HashSet<KeyValuePair<string, long>> batch1 = new HashSet<>(
//                Array.asList(
//                    KeyValuePair.Create(keys[0], 1L),
//                    KeyValuePair.Create(keys[1], 1L),
//                    KeyValuePair.Create(keys[2], 3L),
//                    KeyValuePair.Create(keys[3], 5L),
//                    KeyValuePair.Create(keys[4], 2L))
//            );
//            HashSet<KeyValuePair<string, long>> expectedBatch1 =
//                new HashSet<>(Collections.singleton(KeyValuePair.Create(keys[4], 2L)));

//            IntegrationTestUtils.produceKeyValuesSynchronously(
//                streamOne,
//                batch1,
//                TestUtils.producerConfig(
//                    CLUSTER.bootstrapServers(),
//                    Serdes.String().Serializer,
//                    Serdes.Long().Serializer,
//                    new StreamsConfig()),
//                mockTime);
//            Predicate<string, long> filterPredicate = (key, value) => key.Contains("kafka");
//            KTable<string, long> t1 = builder.table(streamOne);
//            KTable<string, long> t2 = t1.filter(filterPredicate, Materialized.As("queryFilter"));
//            t1.filterNot(filterPredicate, Materialized.As("queryFilterNot"));
//            t2.toStream().To(outputTopic);

//            kafkaStreams = new KafkaStreams(builder.Build(), streamsConfiguration);
//            kafkaStreams.start();

//            WaitUntilAtLeastNumRecordProcessed(outputTopic, 1);

//            IReadOnlyKeyValueStore<string, long>
//                myFilterStore = kafkaStreams.store("queryFilter", QueryableStoreTypes.KeyValueStore());
//            IReadOnlyKeyValueStore<string, long>
//                myFilterNotStore = kafkaStreams.store("queryFilterNot", QueryableStoreTypes.KeyValueStore());

//            foreach (KeyValuePair<string, long> expectedEntry in expectedBatch1)
//            {
//                TestUtils.WaitForCondition(() => expectedEntry.value.equals(myFilterStore.Get(expectedEntry.key)),
//                        "Cannot get expected result");
//            }
//            foreach (KeyValuePair<string, long> batchEntry in batch1)
//            {
//                if (!expectedBatch1.Contains(batchEntry))
//                {
//                    TestUtils.WaitForCondition(() => myFilterStore.Get(batchEntry.key) == null,
//                            "Cannot get null result");
//                }
//            }

//            foreach (KeyValuePair<string, long> expectedEntry in expectedBatch1)
//            {
//                TestUtils.WaitForCondition(() => myFilterNotStore.Get(expectedEntry.key) == null,
//                        "Cannot get null result");
//            }
//            foreach (KeyValuePair<string, long> batchEntry in batch1)
//            {
//                if (!expectedBatch1.Contains(batchEntry))
//                {
//                    TestUtils.WaitForCondition(() => batchEntry.value.equals(myFilterNotStore.Get(batchEntry.key)),
//                            "Cannot get expected result");
//                }
//            }
//        }

//        [Fact]
//        public void ShouldBeAbleToQueryMapValuesState()
//        {// throws Exception
//            streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//            streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//            StreamsBuilder builder = new StreamsBuilder();
//            string[] keys = { "hello", "goodbye", "welcome", "go", "kafka" };
//            HashSet<KeyValuePair<string, string>> batch1 = new HashSet<>(
//                Array.asList(
//                    KeyValuePair.Create(keys[0], "1"),
//                    KeyValuePair.Create(keys[1], "1"),
//                    KeyValuePair.Create(keys[2], "3"),
//                    KeyValuePair.Create(keys[3], "5"),
//                    KeyValuePair.Create(keys[4], "2"))
//            );

//            IntegrationTestUtils.produceKeyValuesSynchronously(
//                streamOne,
//                batch1,
//                TestUtils.producerConfig(
//                    CLUSTER.bootstrapServers(),
//                    Serdes.String().Serializer,
//                    Serdes.String().Serializer,
//                    new StreamsConfig()),
//                mockTime);

//            KTable<string, string> t1 = builder.table(streamOne);
//            t1
//                .mapValues(
//                    (ValueMapper<string, long>)long::valueOf,
//                    Materialized<string, long, IKeyValueStore<Bytes, byte[]>>.As("queryMapValues").withValueSerde(Serdes.Long()))
//                .toStream()
//                .To(outputTopic, Produced.With(Serdes.String(), Serdes.Long()));

//            kafkaStreams = new KafkaStreams(builder.Build(), streamsConfiguration);
//            kafkaStreams.start();

//            WaitUntilAtLeastNumRecordProcessed(outputTopic, 5);

//            IReadOnlyKeyValueStore<string, long> myMapStore =
//                kafkaStreams.store("queryMapValues", QueryableStoreTypes.KeyValueStore());
//            foreach (KeyValuePair<string, string> batchEntry in batch1)
//            {
//                Assert.Equal(long.valueOf(batchEntry.value), myMapStore.Get(batchEntry.key));
//            }
//        }

//        [Fact]
//        public void ShouldBeAbleToQueryMapValuesAfterFilterState()
//        {// throws Exception
//            streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//            streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//            StreamsBuilder builder = new StreamsBuilder();
//            string[] keys = { "hello", "goodbye", "welcome", "go", "kafka" };
//            HashSet<KeyValuePair<string, string>> batch1 = new HashSet<>(
//                Array.asList(
//                    KeyValuePair.Create(keys[0], "1"),
//                    KeyValuePair.Create(keys[1], "1"),
//                    KeyValuePair.Create(keys[2], "3"),
//                    KeyValuePair.Create(keys[3], "5"),
//                    KeyValuePair.Create(keys[4], "2"))
//            );
//            HashSet<KeyValuePair<string, long>> expectedBatch1 =
//                new HashSet<>(Collections.singleton(KeyValuePair.Create(keys[4], 2L)));

//            IntegrationTestUtils.produceKeyValuesSynchronously(
//                streamOne,
//                batch1,
//                TestUtils.producerConfig(
//                    CLUSTER.bootstrapServers(),
//                    Serdes.String().Serializer,
//                    Serdes.String().Serializer,
//                    new StreamsConfig()),
//                mockTime);

//            Predicate<string, string> filterPredicate = (key, value) => key.Contains("kafka");
//            KTable<string, string> t1 = builder.table(streamOne);
//            KTable<string, string> t2 = t1.filter(filterPredicate, Materialized.As("queryFilter"));
//            KTable<string, long> t3 = t2
//                .mapValues(
//                    (ValueMapper<string, long>)long::valueOf,
//                    Materialized<string, long, IKeyValueStore<Bytes, byte[]>>.As("queryMapValues").withValueSerde(Serdes.Long()));
//            t3.toStream().To(outputTopic, Produced.With(Serdes.String(), Serdes.Long()));

//            kafkaStreams = new KafkaStreams(builder.Build(), streamsConfiguration);
//            kafkaStreams.start();

//            WaitUntilAtLeastNumRecordProcessed(outputTopic, 1);

//            IReadOnlyKeyValueStore<string, long>
//                myMapStore = kafkaStreams.store("queryMapValues",
//                QueryableStoreTypes.KeyValueStore());
//            foreach (KeyValuePair<string, long> expectedEntry in expectedBatch1)
//            {
//                Assert.Equal(myMapStore.Get(expectedEntry.key), expectedEntry.value);
//            }
//            foreach (KeyValuePair<string, string> batchEntry in batch1)
//            {
//                KeyValuePair<string, long> batchEntryMapValue =
//                    KeyValuePair.Create(batchEntry.key, long.valueOf(batchEntry.value));
//                if (!expectedBatch1.Contains(batchEntryMapValue))
//                {
//                    Assert.Null(myMapStore.Get(batchEntry.key));
//                }
//            }
//        }

//        private void VerifyCanQueryState(int cacheSizeBytes)
//        {// throws Exception
//            streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, cacheSizeBytes);
//            StreamsBuilder builder = new StreamsBuilder();
//            string[] keys = { "hello", "goodbye", "welcome", "go", "kafka" };

//            HashSet<KeyValuePair<string, string>> batch1 = new TreeSet<>(stringComparator);
//            batch1.addAll(Array.asList(
//                KeyValuePair.Create(keys[0], "hello"),
//                KeyValuePair.Create(keys[1], "goodbye"),
//                KeyValuePair.Create(keys[2], "welcome"),
//                KeyValuePair.Create(keys[3], "go"),
//                KeyValuePair.Create(keys[4], "kafka")));

//            HashSet<KeyValuePair<string, long>> expectedCount = new TreeSet<>(stringLongComparator);
//            foreach (string key in keys)
//            {
//                expectedCount.Add(KeyValuePair.Create(key, 1L));
//            }

//            IntegrationTestUtils.produceKeyValuesSynchronously(
//                    streamOne,
//                    batch1,
//                    TestUtils.producerConfig(
//                    CLUSTER.bootstrapServers(),
//                    Serdes.String().Serializer,
//                    Serdes.String().Serializer,
//                    new StreamsConfig()),
//                    mockTime);

//            KStream<string, string> s1 = builder.Stream(streamOne);

//            // Non Windowed
//            string storeName = "my-count";
//            s1.groupByKey()
//                .count(Materialized.As(storeName))
//                .toStream()
//                .To(outputTopic, Produced.With(Serdes.String(), Serdes.Long()));

//            string windowStoreName = "windowed-count";
//            s1.groupByKey()
//                .windowedBy(TimeWindows.of(FromMilliseconds(WINDOW_SIZE)))
//                .count(Materialized.As(windowStoreName));
//            kafkaStreams = new KafkaStreams(builder.Build(), streamsConfiguration);
//            kafkaStreams.start();

//            WaitUntilAtLeastNumRecordProcessed(outputTopic, 1);

//            IReadOnlyKeyValueStore<string, long>
//                myCount = kafkaStreams.store(storeName, QueryableStoreTypes.KeyValueStore());

//            IReadOnlyWindowStore<string, long> windowStore =
//                kafkaStreams.store(windowStoreName, QueryableStoreTypes.windowStore());
//            VerifyCanGetByKey(keys,
//                expectedCount,
//                expectedCount,
//                windowStore,
//                myCount);

//            VerifyRangeAndAll(expectedCount, myCount);
//        }

//        [Fact]
//        public void ShouldNotMakeStoreAvailableUntilAllStoresAvailable()
//        {// throws Exception
//            StreamsBuilder builder = new StreamsBuilder();
//            KStream<string, string> stream = builder.Stream(streamThree);

//            string storeName = "count-by-key";
//            stream.groupByKey().count(Materialized.As(storeName));
//            kafkaStreams = new KafkaStreams(builder.Build(), streamsConfiguration);
//            kafkaStreams.start();

//            KeyValuePair<string, string> hello = KeyValuePair.Create("hello", "hello");
//            IntegrationTestUtils.produceKeyValuesSynchronously(
//                    streamThree,
//                    Array.asList(hello, hello, hello, hello, hello, hello, hello, hello),
//                    TestUtils.producerConfig(
//                            CLUSTER.bootstrapServers(),
//                            Serdes.String().Serializer,
//                            Serdes.String().Serializer,
//                            new StreamsConfig()),
//                    mockTime);

//            int maxWaitMs = 30000;

//            TestUtils.WaitForCondition(
//                new WaitForStore(storeName),
//                maxWaitMs,
//                "waiting for store " + storeName);

//            IReadOnlyKeyValueStore<string, long> store =
//                kafkaStreams.store(storeName, QueryableStoreTypes.KeyValueStore());

//            TestUtils.WaitForCondition(
//                () => new long(8).equals(store.Get("hello")),
//                maxWaitMs,
//                "wait for count to be 8");

//            // close stream
//            kafkaStreams.close();

//            // start again
//            kafkaStreams = new KafkaStreams(builder.Build(), streamsConfiguration);
//            kafkaStreams.start();

//            // make sure we never get any value other than 8 for hello
//            TestUtils.WaitForCondition(
//                () =>
//                {
//                    try
//                    {
//                        Assert.Equal(
//                            long.valueOf(8L),
//                            kafkaStreams.store(storeName, QueryableStoreTypes.< string, long > KeyValueStore()).Get("hello"));
//                        return true;
//                    }
//                    catch (InvalidStateStoreException ise)
//                    {
//                        return false;
//                    }
//                },
//                maxWaitMs,
//                "waiting for store " + storeName);

//        }

//        private class WaitForStore : TestCondition
//        {
//            private readonly string storeName;

//            WaitForStore(string storeName)
//            {
//                this.storeName = storeName;
//            }


//            public bool ConditionMet()
//            {
//                try
//                {
//                    kafkaStreams.store(storeName, QueryableStoreTypes.< string, long > KeyValueStore());
//                    return true;
//                }
//                catch (InvalidStateStoreException ise)
//                {
//                    return false;
//                }
//            }
//        }

//        [Fact]
//        public void ShouldAllowToQueryAfterThreadDied()
//        {// throws Exception
//            AtomicBoolean beforeFailure = new AtomicBoolean(true);
//            AtomicBoolean failed = new AtomicBoolean(false);
//            string storeName = "store";

//            StreamsBuilder builder = new StreamsBuilder();
//            KStream<string, string> input = builder.Stream(streamOne);
//            input
//                .groupByKey()
//                .reduce((value1, value2) =>
//                {
//                    if (value1.Length() > 1)
//                    {
//                        if (beforeFailure.compareAndSet(true, false))
//                        {
//                            throw new RuntimeException("Injected test exception");
//                        }
//                    }
//                    return value1 + value2;
//                }, Materialized.As(storeName))
//                .toStream()
//                .To(outputTopic);

//            streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
//            kafkaStreams = new KafkaStreams(builder.Build(), streamsConfiguration);
//            kafkaStreams.setUncaughtExceptionHandler((t, e) => failed.set(true));
//            kafkaStreams.start();

//            IntegrationTestUtils.produceKeyValuesSynchronously(
//                streamOne,
//                Array.asList(
//                    KeyValuePair.Create("a", "1"),
//                    KeyValuePair.Create("a", "2"),
//                    KeyValuePair.Create("b", "3"),
//                    KeyValuePair.Create("b", "4")),
//                TestUtils.producerConfig(
//                    CLUSTER.bootstrapServers(),
//                    Serdes.String().Serializer,
//                    Serdes.String().Serializer,
//                    new StreamsConfig()),
//                mockTime);

//            int maxWaitMs = 30000;

//            TestUtils.WaitForCondition(
//                new WaitForStore(storeName),
//                maxWaitMs,
//                "waiting for store " + storeName);

//            IReadOnlyKeyValueStore<string, string> store =
//                kafkaStreams.store(storeName, QueryableStoreTypes.KeyValueStore());

//            TestUtils.WaitForCondition(
//                () => "12".equals(store.Get("a")) && "34".equals(store.Get("b")),
//                maxWaitMs,
//                "wait for agg to be <a,12> and <b,34>");

//            IntegrationTestUtils.produceKeyValuesSynchronously(
//                streamOne,
//                Collections.singleton(KeyValuePair.Create("a", "5")),
//                TestUtils.producerConfig(
//                    CLUSTER.bootstrapServers(),
//                    Serdes.String().Serializer,
//                    Serdes.String().Serializer,
//                    new StreamsConfig()),
//                mockTime);

//            TestUtils.WaitForCondition(
//                failed::get,
//                maxWaitMs,
//                "wait for thread to fail");
//            TestUtils.WaitForCondition(
//                new WaitForStore(storeName),
//                maxWaitMs,
//                "waiting for store " + storeName);

//            IReadOnlyKeyValueStore<string, string> store2 =
//                kafkaStreams.store(storeName, QueryableStoreTypes.KeyValueStore());

//            try
//            {
//                TestUtils.WaitForCondition(
//                    () => ("125".equals(store2.Get("a"))
//                        || "1225".equals(store2.Get("a"))
//                        || "12125".equals(store2.Get("a")))
//                        &&
//                        ("34".equals(store2.Get("b"))
//                        || "344".equals(store2.Get("b"))
//                        || "3434".equals(store2.Get("b"))),
//                    maxWaitMs,
//                    "wait for agg to be <a,125>||<a,1225>||<a,12125> and <b,34>||<b,344>||<b,3434>");
//            }
//            catch (Throwable t)
//            {
//                throw new RuntimeException("Store content is a: " + store2.Get("a") + "; b: " + store2.Get("b"), t);
//            }
//        }

//        private void VerifyRangeAndAll(HashSet<KeyValuePair<string, long>> expectedCount,
//                                       IReadOnlyKeyValueStore<string, long> myCount)
//        {
//            HashSet<KeyValuePair<string, long>> countRangeResults = new TreeSet<>(stringLongComparator);
//            HashSet<KeyValuePair<string, long>> countAllResults = new TreeSet<>(stringLongComparator);
//            HashSet<KeyValuePair<string, long>> expectedRangeResults = new TreeSet<>(stringLongComparator);

//            expectedRangeResults.addAll(Array.asList(
//                KeyValuePair.Create("hello", 1L),
//                KeyValuePair.Create("go", 1L),
//                KeyValuePair.Create("goodbye", 1L),
//                KeyValuePair.Create("kafka", 1L)
//            ));

//            try
//            {
//                (IKeyValueIterator<string, long> range = myCount.Range("go", "kafka"));
//                while (range.HasNext())
//                {
//                    countRangeResults.Add(range.MoveNext());
//                }
//            }

//        try
//            {
//                (IKeyValueIterator<string, long> all = myCount.all());
//                while (all.HasNext())
//                {
//                    countAllResults.Add(all.MoveNext());
//                }
//            }

//        Assert.Equal(countRangeResults, (expectedRangeResults));
//            Assert.Equal(countAllResults, (expectedCount));
//        }

//        private void VerifyCanGetByKey(string[] keys,
//                                       HashSet<KeyValuePair<string, long>> expectedWindowState,
//                                       HashSet<KeyValuePair<string, long>> expectedCount,
//                                       IReadOnlyWindowStore<string, long> windowStore,
//                                       IReadOnlyKeyValueStore<string, long> myCount)
//        {// throws Exception
//            HashSet<KeyValuePair<string, long>> windowState = new TreeSet<>(stringLongComparator);
//            HashSet<KeyValuePair<string, long>> countState = new TreeSet<>(stringLongComparator);

//            long timeout = System.currentTimeMillis() + 30000;
//            while ((windowState.Count < keys.Length ||
//                countState.Count < keys.Length) &&
//                System.currentTimeMillis() < timeout)
//            {
//                Thread.sleep(10);
//                foreach (string key in keys)
//                {
//                    windowState.addAll(Fetch(windowStore, key));
//                    long value = myCount.Get(key);
//                    if (value != null)
//                    {
//                        countState.Add(KeyValuePair.Create(key, value));
//                    }
//                }
//            }
//            Assert.Equal(windowState, (expectedWindowState));
//            Assert.Equal(countState, (expectedCount));
//        }

//        /**
//         * Verify that the new count is greater than or equal to the previous count.
//         * Note: this method changes the values in expectedWindowState and expectedCount
//         *
//         * @param keys                  All the keys we ever expect to find
//         * @param expectedWindowedCount Expected windowed count
//         * @param expectedCount         Expected count
//         * @param windowStore           Window Store
//         * @param KeyValueStore         Key-value store
//         * @param failIfKeyNotFound     if true, tests fails if an expected key is not found in store. If false,
//         *                              the method merely inserts the new found key into the list of
//         *                              expected keys.
//         */
//        private void VerifyGreaterOrEqual(string[] keys,
//                                          Dictionary<string, long> expectedWindowedCount,
//                                          Dictionary<string, long> expectedCount,
//                                          IReadOnlyWindowStore<string, long> windowStore,
//                                          IReadOnlyKeyValueStore<string, long> KeyValueStore,
//                                          bool failIfKeyNotFound)
//        {
//            Dictionary<string, long> windowState = new HashMap<>();
//            Dictionary<string, long> countState = new HashMap<>();

//            foreach (string key in keys)
//            {
//                Dictionary<string, long> map = FetchMap(windowStore, key);
//                if (map.equals(Collections.< string, long > emptyMap()) && failIfKeyNotFound)
//                {
//                    Assert.True(false, "Key in windowed-store not found " + key);
//                }
//                windowState.putAll(map);
//                long value = KeyValueStore.Get(key);
//                if (value != null)
//                {
//                    countState.put(key, value);
//                }
//                else if (failIfKeyNotFound)
//                {
//                    Assert.True(false, "Key in key-value-store not found " + key);
//                }
//            }

//            foreach (Map.Entry<string, long> actualWindowStateEntry in windowState.entrySet())
//            {
//                if (expectedWindowedCount.containsKey(actualWindowStateEntry.getKey()))
//                {
//                    long expectedValue = expectedWindowedCount.Get(actualWindowStateEntry.getKey());
//                    Assert.True(actualWindowStateEntry.getValue() >= expectedValue);
//                }
//                // return this for next round of comparisons
//                expectedWindowedCount.put(actualWindowStateEntry.getKey(), actualWindowStateEntry.getValue());
//            }

//            foreach (Map.Entry<string, long> actualCountStateEntry in countState.entrySet())
//            {
//                if (expectedCount.containsKey(actualCountStateEntry.getKey()))
//                {
//                    long expectedValue = expectedCount.Get(actualCountStateEntry.getKey());
//                    Assert.True(actualCountStateEntry.getValue() >= expectedValue);
//                }
//                // return this for next round of comparisons
//                expectedCount.put(actualCountStateEntry.getKey(), actualCountStateEntry.getValue());
//            }

//        }

//        private void WaitUntilAtLeastNumRecordProcessed(string topic,
//                                                        int numRecs)
//        {// throws Exception
//            StreamsConfig config = new StreamsConfig();
//            config.Set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//            config.Set(ConsumerConfig.GROUP_ID_CONFIG, "queryable-state-consumer");
//            config.Set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            config.Set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().Deserializer.getName());
//            config.Set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.getName());
//            IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
//                config,
//                topic,
//                numRecs,
//                120 * 1000);
//        }

//        private HashSet<KeyValuePair<string, long>> Fetch(IReadOnlyWindowStore<string, long> store,
//                                                  string key)
//        {
//            IWindowStoreIterator<long> fetch =
//                store.Fetch(key, ofEpochMilli(0), ofEpochMilli(System.currentTimeMillis()));
//            if (fetch.MoveNext())
//            {
//                KeyValuePair<long, long> next = fetch.Current;
//                return Collections.singleton(KeyValuePair.Create(key, next.Value));
//            }

//            return Collections.emptySet();
//        }

//        private Dictionary<string, long> FetchMap(IReadOnlyWindowStore<string, long> store,
//                                           string key)
//        {
//            IWindowStoreIterator<long> fetch =
//                store.Fetch(key, ofEpochMilli(0), ofEpochMilli(System.currentTimeMillis()));
//            if (fetch.HasNext())
//            {
//                KeyValuePair<long, long> next = fetch.MoveNext();
//                return Collections.singletonMap(key, next.Value);
//            }
//            return Collections.emptyMap();
//        }

//        /**
//         * A class that periodically produces records in a separate thread
//         */
//        private class ProducerRunnable : Runnable
//        {
//            private readonly string topic;
//            private List<string> inputValues;
//            private readonly int numIterations;
//            private int currIteration = 0;
//            bool shutdown = false;

//            ProducerRunnable(string topic,
//                             List<string> inputValues,
//                             int numIterations)
//            {
//                this.topic = topic;
//                this.inputValues = inputValues;
//                this.numIterations = numIterations;
//            }

//            [MethodImpl(MethodImplOptions.Synchronized)]
//            private void IncrementIteration()
//            {
//                currIteration++;
//            }

//            [MethodImpl(MethodImplOptions.Synchronized)]
//            int GetCurrIteration()
//            {
//                return currIteration;
//            }

//            [MethodImpl(MethodImplOptions.Synchronized)]
//            void Shutdown()
//            {
//                shutdown = true;
//            }


//            public void Run()
//            {
//                StreamsConfig producerConfig = new StreamsConfig();
//                producerConfig.Set(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//                producerConfig.Set(ProducerConfig.ACKS_CONFIG, "all");
//                producerConfig.Set(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);
//                producerConfig.Set(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);

//                KafkaProducer<string, string> producer =
//                         new KafkaProducer<>(producerConfig, new Serdes.String().Serializer(), new Serdes.String().Serializer());

//                while (GetCurrIteration() < numIterations && !shutdown)
//                {
//                    foreach (string value in inputValues)
//                    {
//                        producer.send(new ProducerRecord<>(topic, value));
//                    }
//                    IncrementIteration();
//                }
//            }
//        }
//    }
//}
