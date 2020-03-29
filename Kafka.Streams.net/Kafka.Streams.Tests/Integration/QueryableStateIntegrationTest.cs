/*






 *

 *





 */
















































































public class QueryableStateIntegrationTest {
    private static Logger log = LoggerFactory.getLogger(QueryableStateIntegrationTest);

    private static int NUM_BROKERS = 1;

    
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private static int STREAM_THREE_PARTITIONS = 4;
    private MockTime mockTime = CLUSTER.time;
    private string streamOne = "stream-one";
    private string streamTwo = "stream-two";
    private string streamThree = "stream-three";
    private string streamConcurrent = "stream-concurrent";
    private string outputTopic = "output";
    private string outputTopicConcurrent = "output-concurrent";
    private string outputTopicConcurrentWindowed = "output-concurrent-windowed";
    private string outputTopicThree = "output-three";
    // sufficiently large window size such that everything falls into 1 window
    private static long WINDOW_SIZE = TimeUnit.MILLISECONDS.convert(2, TimeUnit.DAYS);
    private static int STREAM_TWO_PARTITIONS = 2;
    private static int NUM_REPLICAS = NUM_BROKERS;
    private Properties streamsConfiguration;
    private List<string> inputValues;
    private int numberOfWordsPerIteration = 0;
    private HashSet<string> inputValuesKeys;
    private KafkaStreams kafkaStreams;
    private Comparator<KeyValuePair<string, string>> stringComparator;
    private Comparator<KeyValuePair<string, long>> stringLongComparator;
    private static volatile AtomicInteger testNo = new AtomicInteger(0);

    private void CreateTopics() {// throws Exception
        streamOne = streamOne + "-" + testNo;
        streamConcurrent = streamConcurrent + "-" + testNo;
        streamThree = streamThree + "-" + testNo;
        outputTopic = outputTopic + "-" + testNo;
        outputTopicConcurrent = outputTopicConcurrent + "-" + testNo;
        outputTopicConcurrentWindowed = outputTopicConcurrentWindowed + "-" + testNo;
        outputTopicThree = outputTopicThree + "-" + testNo;
        streamTwo = streamTwo + "-" + testNo;
        CLUSTER.createTopics(streamOne, streamConcurrent);
        CLUSTER.createTopic(streamTwo, STREAM_TWO_PARTITIONS, NUM_REPLICAS);
        CLUSTER.createTopic(streamThree, STREAM_THREE_PARTITIONS, 1);
        CLUSTER.createTopics(outputTopic, outputTopicConcurrent, outputTopicConcurrentWindowed, outputTopicThree);
    }

    /**
     * Try to read inputValues from {@code resources/QueryableStateIntegrationTest/inputValues.txt}, which might be useful
     * for larger scale testing. In case of exception, for instance if no such file can be read, return a small list
     * which satisfies all the prerequisites of the tests.
     */
    private List<string> GetInputValues() {
        List<string> input = new ArrayList<>();
        ClassLoader classLoader = getClass().getClassLoader();
        string fileName = "QueryableStateIntegrationTest" + File.separator + "inputValues.txt";
        try (BufferedReader reader = new BufferedReader(
            new FileReader(Objects.requireNonNull(classLoader.getResource(fileName)).getFile()))) {

            for (string line = reader.readLine(); line != null; line = reader.readLine()) {
                input.add(line);
            }
        } catch (Exception e) {
            log.warn("Unable to read '{}{}{}'. Using default inputValues list", "resources", File.separator, fileName);
            input = Array.asList(
                        "hello world",
                        "all streams lead to kafka",
                        "streams",
                        "kafka streams",
                        "the cat in the hat",
                        "green eggs and ham",
                        "that Sam i am",
                        "up the creek without a paddle",
                        "run forest run",
                        "a tank full of gas",
                        "eat sleep rave repeat",
                        "one jolly sailor",
                        "king of the world");

        }
        return input;
    }

    
    public void Before() {// throws Exception
        CreateTopics();
        streamsConfiguration = new Properties();
        string applicationId = "queryable-state-" + testNo.incrementAndGet();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory("qs-test").getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        stringComparator = Comparator.comparing((KeyValuePair<string, string> o) => o.key).thenComparing(o => o.value);
        stringLongComparator = Comparator.comparing((KeyValuePair<string, long> o) => o.key).thenComparingLong(o => o.value);
        inputValues = GetInputValues();
        inputValuesKeys = new HashSet<>();
        foreach (string sentence in inputValues) {
            string[] words = sentence.split("\\W+");
            numberOfWordsPerIteration += words.Length;
            Collections.addAll(inputValuesKeys, words);
        }
    }

    
    public void Shutdown() {// throws Exception
        if (kafkaStreams != null) {
            kafkaStreams.close(ofSeconds(30));
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    /**
     * Creates a typical word count topology
     */
    private KafkaStreams CreateCountStream(string inputTopic,
                                           string outputTopic,
                                           string windowOutputTopic,
                                           string storeName,
                                           string windowStoreName,
                                           Properties streamsConfiguration) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<string> stringSerde = Serdes.String();
        KStream<string, string> textLines = builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde));

        KGroupedStream<string, string> groupedByWord = textLines
            .flatMapValues((ValueMapper<string, Iterable<string>>) value => Array.asList(value.split("\\W+")))
            .groupBy(MockMapper.selectValueMapper());

        // Create a State Store for the all time word count
        groupedByWord
            .count(Materialized.As(storeName + "-" + inputTopic))
            .toStream()
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        // Create a Windowed State Store that contains the word count for every 1 minute
        groupedByWord
            .windowedBy(TimeWindows.of(ofMillis(WINDOW_SIZE)))
            .count(Materialized.As(windowStoreName + "-" + inputTopic))
            .toStream((key, value) => key.Key)
            .to(windowOutputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        return new KafkaStreams(builder.build(), streamsConfiguration);
    }

    private class StreamRunnable : Runnable {
        private KafkaStreams myStream;
        private bool closed = false;
        private KafkaStreamsTest.StateListenerStub stateListener = new KafkaStreamsTest.StateListenerStub();

        StreamRunnable(string inputTopic,
                       string outputTopic,
                       string outputTopicWindowed,
                       string storeName,
                       string windowStoreName,
                       int queryPort) {
            Properties props = (Properties) streamsConfiguration.clone();
            props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + queryPort);
            myStream = CreateCountStream(inputTopic, outputTopic, outputTopicWindowed, storeName, windowStoreName, props);
            myStream.setStateListener(stateListener);
        }

        
        public void Run() {
            myStream.start();
        }

        public void Close() {
            if (!closed) {
                myStream.close();
                closed = true;
            }
        }

        public bool IsClosed() {
            return closed;
        }

        public KafkaStreams GetStream() {
            return myStream;
        }

        KafkaStreamsTest.StateListenerStub GetStateListener() {
            return stateListener;
        }
    }

    private void VerifyAllKVKeys(StreamRunnable[] streamRunnables,
                                 KafkaStreams streams,
                                 KafkaStreamsTest.StateListenerStub stateListenerStub,
                                 HashSet<string> keys,
                                 string storeName) {// throws Exception
        foreach (string key in keys) {
            TestUtils.waitForCondition(
                () => {
                    try {
                        StreamsMetadata metadata = streams.metadataForKey(storeName, key, new StringSerializer());

                        if (metadata == null || metadata.equals(StreamsMetadata.NOT_AVAILABLE)) {
                            return false;
                        }
                        int index = metadata.hostInfo().port();
                        KafkaStreams streamsWithKey = streamRunnables[index].GetStream();
                        ReadOnlyKeyValueStore<string, long> store =
                            streamsWithKey.store(storeName, QueryableStoreTypes.keyValueStore());

                        return store != null && store.get(key) != null;
                    } catch (IllegalStateException e) {
                        // Kafka Streams instance may have closed but rebalance hasn't happened
                        return false;
                    } catch (InvalidStateStoreException e) {
                        // there must have been at least one rebalance state
                        Assert.True(stateListenerStub.mapStates.get(KafkaStreams.State.REBALANCING) >= 1);
                        return false;
                    }
                },
                120000,
                "waiting for metadata, store and value to be non null");
        }
    }

    private void VerifyAllWindowedKeys(StreamRunnable[] streamRunnables,
                                       KafkaStreams streams,
                                       KafkaStreamsTest.StateListenerStub stateListenerStub,
                                       HashSet<string> keys,
                                       string storeName,
                                       long from,
                                       long to) {// throws Exception
        foreach (string key in keys) {
            TestUtils.waitForCondition(
                () => {
                    try {
                        StreamsMetadata metadata = streams.metadataForKey(storeName, key, new StringSerializer());
                        if (metadata == null || metadata.equals(StreamsMetadata.NOT_AVAILABLE)) {
                            return false;
                        }
                        int index = metadata.hostInfo().port();
                        KafkaStreams streamsWithKey = streamRunnables[index].GetStream();
                        ReadOnlyWindowStore<string, long> store =
                            streamsWithKey.store(storeName, QueryableStoreTypes.windowStore());
                        return store != null && store.fetch(key, ofEpochMilli(from), ofEpochMilli(to)) != null;
                    } catch (IllegalStateException e) {
                        // Kafka Streams instance may have closed but rebalance hasn't happened
                        return false;
                    } catch (InvalidStateStoreException e) {
                        // there must have been at least one rebalance state
                        Assert.True(stateListenerStub.mapStates.get(KafkaStreams.State.REBALANCING) >= 1);
                        return false;
                    }
                },
                120000,
                "waiting for metadata, store and value to be non null");
        }
    }

    [Xunit.Fact]
    public void QueryOnRebalance() {// throws Exception
        int numThreads = STREAM_TWO_PARTITIONS;
        StreamRunnable[] streamRunnables = new StreamRunnable[numThreads];
        Thread[] streamThreads = new Thread[numThreads];

        ProducerRunnable producerRunnable = new ProducerRunnable(streamThree, inputValues, 1);
        producerRunnable.Run();

        // create stream threads
        string storeName = "word-count-store";
        string windowStoreName = "windowed-word-count-store";
        for (int i = 0; i < numThreads; i++) {
            streamRunnables[i] = new StreamRunnable(
                streamThree,
                outputTopicThree,
                outputTopicConcurrentWindowed,
                storeName,
                windowStoreName,
                i);
            streamThreads[i] = new Thread(streamRunnables[i]);
            streamThreads[i].start();
        }

        try {
            WaitUntilAtLeastNumRecordProcessed(outputTopicThree, 1);

            for (int i = 0; i < numThreads; i++) {
                VerifyAllKVKeys(
                    streamRunnables,
                    streamRunnables[i].GetStream(),
                    streamRunnables[i].GetStateListener(),
                    inputValuesKeys,
                    storeName + "-" + streamThree);
                VerifyAllWindowedKeys(
                    streamRunnables,
                    streamRunnables[i].GetStream(),
                    streamRunnables[i].GetStateListener(),
                    inputValuesKeys,
                    windowStoreName + "-" + streamThree,
                    0L,
                    WINDOW_SIZE);
                Assert.Equal(KafkaStreams.State.RUNNING, streamRunnables[i].GetStream().state());
            }

            // kill N-1 threads
            for (int i = 1; i < numThreads; i++) {
                streamRunnables[i].Close();
                streamThreads[i].interrupt();
                streamThreads[i].join();
            }

            // query from the remaining thread
            VerifyAllKVKeys(
                streamRunnables,
                streamRunnables[0].GetStream(),
                streamRunnables[0].GetStateListener(),
                inputValuesKeys,
                storeName + "-" + streamThree);
            VerifyAllWindowedKeys(
                streamRunnables,
                streamRunnables[0].GetStream(),
                streamRunnables[0].GetStateListener(),
                inputValuesKeys,
                windowStoreName + "-" + streamThree,
                0L,
                WINDOW_SIZE);
            Assert.Equal(KafkaStreams.State.RUNNING, streamRunnables[0].GetStream().state());
        } finally {
            for (int i = 0; i < numThreads; i++) {
                if (!streamRunnables[i].IsClosed()) {
                    streamRunnables[i].Close();
                    streamThreads[i].interrupt();
                    streamThreads[i].join();
                }
            }
        }
    }

    [Xunit.Fact]
    public void ConcurrentAccesses() {// throws Exception
        int numIterations = 500000;
        string storeName = "word-count-store";
        string windowStoreName = "windowed-word-count-store";

        ProducerRunnable producerRunnable = new ProducerRunnable(streamConcurrent, inputValues, numIterations);
        Thread producerThread = new Thread(producerRunnable);
        kafkaStreams = CreateCountStream(
            streamConcurrent,
            outputTopicConcurrent,
            outputTopicConcurrentWindowed,
            storeName,
            windowStoreName,
            streamsConfiguration);

        kafkaStreams.start();
        producerThread.start();

        try {
            WaitUntilAtLeastNumRecordProcessed(outputTopicConcurrent, numberOfWordsPerIteration);
            WaitUntilAtLeastNumRecordProcessed(outputTopicConcurrentWindowed, numberOfWordsPerIteration);

            ReadOnlyKeyValueStore<string, long> keyValueStore =
                kafkaStreams.store(storeName + "-" + streamConcurrent, QueryableStoreTypes.keyValueStore());

            ReadOnlyWindowStore<string, long> windowStore =
                kafkaStreams.store(windowStoreName + "-" + streamConcurrent, QueryableStoreTypes.windowStore());

            Dictionary<string, long> expectedWindowState = new HashMap<>();
            Dictionary<string, long> expectedCount = new HashMap<>();
            while (producerRunnable.GetCurrIteration() < numIterations) {
                verifyGreaterOrEqual(inputValuesKeys.toArray(new string[0]), expectedWindowState,
                    expectedCount, windowStore, keyValueStore, true);
            }
        } finally {
            producerRunnable.shutdown();
            producerThread.interrupt();
            producerThread.join();
        }
    }

    [Xunit.Fact]
    public void ShouldBeAbleToQueryStateWithZeroSizedCache() {// throws Exception
        VerifyCanQueryState(0);
    }

    [Xunit.Fact]
    public void ShouldBeAbleToQueryStateWithNonZeroSizedCache() {// throws Exception
        VerifyCanQueryState(10 * 1024 * 1024);
    }

    [Xunit.Fact]
    public void ShouldBeAbleToQueryFilterState() {// throws Exception
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        string[] keys = {"hello", "goodbye", "welcome", "go", "kafka"};
        HashSet<KeyValuePair<string, long>> batch1 = new HashSet<>(
            Array.asList(
                new KeyValuePair<>(keys[0], 1L),
                new KeyValuePair<>(keys[1], 1L),
                new KeyValuePair<>(keys[2], 3L),
                new KeyValuePair<>(keys[3], 5L),
                new KeyValuePair<>(keys[4], 2L))
        );
        HashSet<KeyValuePair<string, long>> expectedBatch1 =
            new HashSet<>(Collections.singleton(new KeyValuePair<>(keys[4], 2L)));

        IntegrationTestUtils.produceKeyValuesSynchronously(
            streamOne,
            batch1,
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer,
                LongSerializer,
                new Properties()),
            mockTime);
        Predicate<string, long> filterPredicate = (key, value) => key.Contains("kafka");
        KTable<string, long> t1 = builder.table(streamOne);
        KTable<string, long> t2 = t1.filter(filterPredicate, Materialized.As("queryFilter"));
        t1.filterNot(filterPredicate, Materialized.As("queryFilterNot"));
        t2.toStream().to(outputTopic);

        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();

        WaitUntilAtLeastNumRecordProcessed(outputTopic, 1);

        ReadOnlyKeyValueStore<string, long>
            myFilterStore = kafkaStreams.store("queryFilter", QueryableStoreTypes.keyValueStore());
        ReadOnlyKeyValueStore<string, long>
            myFilterNotStore = kafkaStreams.store("queryFilterNot", QueryableStoreTypes.keyValueStore());

        foreach (KeyValuePair<string, long> expectedEntry in expectedBatch1) {
            TestUtils.waitForCondition(() => expectedEntry.value.equals(myFilterStore.get(expectedEntry.key)),
                    "Cannot get expected result");
        }
        foreach (KeyValuePair<string, long> batchEntry in batch1) {
            if (!expectedBatch1.Contains(batchEntry)) {
                TestUtils.waitForCondition(() => myFilterStore.get(batchEntry.key) == null,
                        "Cannot get null result");
            }
        }

        foreach (KeyValuePair<string, long> expectedEntry in expectedBatch1) {
            TestUtils.waitForCondition(() => myFilterNotStore.get(expectedEntry.key) == null,
                    "Cannot get null result");
        }
        foreach (KeyValuePair<string, long> batchEntry in batch1) {
            if (!expectedBatch1.Contains(batchEntry)) {
                TestUtils.waitForCondition(() => batchEntry.value.equals(myFilterNotStore.get(batchEntry.key)),
                        "Cannot get expected result");
            }
        }
    }

    [Xunit.Fact]
    public void ShouldBeAbleToQueryMapValuesState() {// throws Exception
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        string[] keys = {"hello", "goodbye", "welcome", "go", "kafka"};
        HashSet<KeyValuePair<string, string>> batch1 = new HashSet<>(
            Array.asList(
                new KeyValuePair<>(keys[0], "1"),
                new KeyValuePair<>(keys[1], "1"),
                new KeyValuePair<>(keys[2], "3"),
                new KeyValuePair<>(keys[3], "5"),
                new KeyValuePair<>(keys[4], "2"))
        );

        IntegrationTestUtils.produceKeyValuesSynchronously(
            streamOne,
            batch1,
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer,
                StringSerializer,
                new Properties()),
            mockTime);

        KTable<string, string> t1 = builder.table(streamOne);
        t1
            .mapValues(
                (ValueMapper<string, long>) long::valueOf,
                Materialized<string, long, KeyValueStore<Bytes, byte[]>>.As("queryMapValues").withValueSerde(Serdes.Long()))
            .toStream()
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();

        WaitUntilAtLeastNumRecordProcessed(outputTopic, 5);

        ReadOnlyKeyValueStore<string, long> myMapStore =
            kafkaStreams.store("queryMapValues", QueryableStoreTypes.keyValueStore());
        foreach (KeyValuePair<string, string> batchEntry in batch1) {
            Assert.Equal(long.valueOf(batchEntry.value), myMapStore.get(batchEntry.key));
        }
    }

    [Xunit.Fact]
    public void ShouldBeAbleToQueryMapValuesAfterFilterState() {// throws Exception
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
        string[] keys = {"hello", "goodbye", "welcome", "go", "kafka"};
        HashSet<KeyValuePair<string, string>> batch1 = new HashSet<>(
            Array.asList(
                new KeyValuePair<>(keys[0], "1"),
                new KeyValuePair<>(keys[1], "1"),
                new KeyValuePair<>(keys[2], "3"),
                new KeyValuePair<>(keys[3], "5"),
                new KeyValuePair<>(keys[4], "2"))
        );
        HashSet<KeyValuePair<string, long>> expectedBatch1 =
            new HashSet<>(Collections.singleton(new KeyValuePair<>(keys[4], 2L)));

        IntegrationTestUtils.produceKeyValuesSynchronously(
            streamOne,
            batch1,
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer,
                StringSerializer,
                new Properties()),
            mockTime);

        Predicate<string, string> filterPredicate = (key, value) => key.Contains("kafka");
        KTable<string, string> t1 = builder.table(streamOne);
        KTable<string, string> t2 = t1.filter(filterPredicate, Materialized.As("queryFilter"));
        KTable<string, long> t3 = t2
            .mapValues(
                (ValueMapper<string, long>) long::valueOf,
                Materialized<string, long, KeyValueStore<Bytes, byte[]>>.As("queryMapValues").withValueSerde(Serdes.Long()));
        t3.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();

        WaitUntilAtLeastNumRecordProcessed(outputTopic, 1);

        ReadOnlyKeyValueStore<string, long>
            myMapStore = kafkaStreams.store("queryMapValues",
            QueryableStoreTypes.keyValueStore());
        foreach (KeyValuePair<string, long> expectedEntry in expectedBatch1) {
            Assert.Equal(myMapStore.get(expectedEntry.key), expectedEntry.value);
        }
        foreach (KeyValuePair<string, string> batchEntry in batch1) {
            KeyValuePair<string, long> batchEntryMapValue =
                new KeyValuePair<>(batchEntry.key, long.valueOf(batchEntry.value));
            if (!expectedBatch1.Contains(batchEntryMapValue)) {
                assertNull(myMapStore.get(batchEntry.key));
            }
        }
    }

    private void VerifyCanQueryState(int cacheSizeBytes) {// throws Exception
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, cacheSizeBytes);
        StreamsBuilder builder = new StreamsBuilder();
        string[] keys = {"hello", "goodbye", "welcome", "go", "kafka"};

        HashSet<KeyValuePair<string, string>> batch1 = new TreeSet<>(stringComparator);
        batch1.addAll(Array.asList(
            new KeyValuePair<>(keys[0], "hello"),
            new KeyValuePair<>(keys[1], "goodbye"),
            new KeyValuePair<>(keys[2], "welcome"),
            new KeyValuePair<>(keys[3], "go"),
            new KeyValuePair<>(keys[4], "kafka")));

        HashSet<KeyValuePair<string, long>> expectedCount = new TreeSet<>(stringLongComparator);
        foreach (string key in keys) {
            expectedCount.add(new KeyValuePair<>(key, 1L));
        }

        IntegrationTestUtils.produceKeyValuesSynchronously(
                streamOne,
                batch1,
                TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer,
                StringSerializer,
                new Properties()),
                mockTime);

        KStream<string, string> s1 = builder.stream(streamOne);

        // Non Windowed
        string storeName = "my-count";
        s1.groupByKey()
            .count(Materialized.As(storeName))
            .toStream()
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        string windowStoreName = "windowed-count";
        s1.groupByKey()
            .windowedBy(TimeWindows.of(ofMillis(WINDOW_SIZE)))
            .count(Materialized.As(windowStoreName));
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();

        WaitUntilAtLeastNumRecordProcessed(outputTopic, 1);

        ReadOnlyKeyValueStore<string, long>
            myCount = kafkaStreams.store(storeName, QueryableStoreTypes.keyValueStore());

        ReadOnlyWindowStore<string, long> windowStore =
            kafkaStreams.store(windowStoreName, QueryableStoreTypes.windowStore());
        VerifyCanGetByKey(keys,
            expectedCount,
            expectedCount,
            windowStore,
            myCount);

        VerifyRangeAndAll(expectedCount, myCount);
    }

    [Xunit.Fact]
    public void ShouldNotMakeStoreAvailableUntilAllStoresAvailable() {// throws Exception
        StreamsBuilder builder = new StreamsBuilder();
        KStream<string, string> stream = builder.stream(streamThree);

        string storeName = "count-by-key";
        stream.groupByKey().count(Materialized.As(storeName));
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();

        KeyValuePair<string, string> hello = KeyValuePair.Create("hello", "hello");
        IntegrationTestUtils.produceKeyValuesSynchronously(
                streamThree,
                Array.asList(hello, hello, hello, hello, hello, hello, hello, hello),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        StringSerializer,
                        StringSerializer,
                        new Properties()),
                mockTime);

        int maxWaitMs = 30000;

        TestUtils.waitForCondition(
            new WaitForStore(storeName),
            maxWaitMs,
            "waiting for store " + storeName);

        ReadOnlyKeyValueStore<string, long> store =
            kafkaStreams.store(storeName, QueryableStoreTypes.keyValueStore());

        TestUtils.waitForCondition(
            () => new long(8).equals(store.get("hello")),
            maxWaitMs,
            "wait for count to be 8");

        // close stream
        kafkaStreams.close();

        // start again
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();

        // make sure we never get any value other than 8 for hello
        TestUtils.waitForCondition(
            () => {
                try {
                    Assert.Equal(
                        long.valueOf(8L),
                        kafkaStreams.store(storeName, QueryableStoreTypes.<string, long>keyValueStore()).get("hello"));
                    return true;
                } catch (InvalidStateStoreException ise) {
                    return false;
                }
            },
            maxWaitMs,
            "waiting for store " + storeName);

    }

    private class WaitForStore : TestCondition {
        private string storeName;

        WaitForStore(string storeName) {
            this.storeName = storeName;
        }

        
        public bool ConditionMet() {
            try {
                kafkaStreams.store(storeName, QueryableStoreTypes.<string, long>keyValueStore());
                return true;
            } catch (InvalidStateStoreException ise) {
                return false;
            }
        }
    }

    [Xunit.Fact]
    public void ShouldAllowToQueryAfterThreadDied() {// throws Exception
        AtomicBoolean beforeFailure = new AtomicBoolean(true);
        AtomicBoolean failed = new AtomicBoolean(false);
        string storeName = "store";

        StreamsBuilder builder = new StreamsBuilder();
        KStream<string, string> input = builder.stream(streamOne);
        input
            .groupByKey()
            .reduce((value1, value2) => {
                if (value1.Length() > 1) {
                    if (beforeFailure.compareAndSet(true, false)) {
                        throw new RuntimeException("Injected test exception");
                    }
                }
                return value1 + value2;
            }, Materialized.As(storeName))
            .toStream()
            .to(outputTopic);

        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.setUncaughtExceptionHandler((t, e) => failed.set(true));
        kafkaStreams.start();

        IntegrationTestUtils.produceKeyValuesSynchronously(
            streamOne,
            Array.asList(
                KeyValuePair.Create("a", "1"),
                KeyValuePair.Create("a", "2"),
                KeyValuePair.Create("b", "3"),
                KeyValuePair.Create("b", "4")),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer,
                StringSerializer,
                new Properties()),
            mockTime);

        int maxWaitMs = 30000;

        TestUtils.waitForCondition(
            new WaitForStore(storeName),
            maxWaitMs,
            "waiting for store " + storeName);

        ReadOnlyKeyValueStore<string, string> store =
            kafkaStreams.store(storeName, QueryableStoreTypes.keyValueStore());

        TestUtils.waitForCondition(
            () => "12".equals(store.get("a")) && "34".equals(store.get("b")),
            maxWaitMs,
            "wait for agg to be <a,12> and <b,34>");

        IntegrationTestUtils.produceKeyValuesSynchronously(
            streamOne,
            Collections.singleton(KeyValuePair.Create("a", "5")),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer,
                StringSerializer,
                new Properties()),
            mockTime);

        TestUtils.waitForCondition(
            failed::get,
            maxWaitMs,
            "wait for thread to fail");
        TestUtils.waitForCondition(
            new WaitForStore(storeName),
            maxWaitMs,
            "waiting for store " + storeName);

        ReadOnlyKeyValueStore<string, string> store2 =
            kafkaStreams.store(storeName, QueryableStoreTypes.keyValueStore());

        try {
            TestUtils.waitForCondition(
                () => ("125".equals(store2.get("a"))
                    || "1225".equals(store2.get("a"))
                    || "12125".equals(store2.get("a")))
                    &&
                    ("34".equals(store2.get("b"))
                    || "344".equals(store2.get("b"))
                    || "3434".equals(store2.get("b"))),
                maxWaitMs,
                "wait for agg to be <a,125>||<a,1225>||<a,12125> and <b,34>||<b,344>||<b,3434>");
        } catch (Throwable t) {
            throw new RuntimeException("Store content is a: " + store2.get("a") + "; b: " + store2.get("b"), t);
        }
    }

    private void VerifyRangeAndAll(HashSet<KeyValuePair<string, long>> expectedCount,
                                   ReadOnlyKeyValueStore<string, long> myCount) {
        HashSet<KeyValuePair<string, long>> countRangeResults = new TreeSet<>(stringLongComparator);
        HashSet<KeyValuePair<string, long>> countAllResults = new TreeSet<>(stringLongComparator);
        HashSet<KeyValuePair<string, long>> expectedRangeResults = new TreeSet<>(stringLongComparator);

        expectedRangeResults.addAll(Array.asList(
            new KeyValuePair<>("hello", 1L),
            new KeyValuePair<>("go", 1L),
            new KeyValuePair<>("goodbye", 1L),
            new KeyValuePair<>("kafka", 1L)
        ));

        try { 
 (KeyValueIterator<string, long> range = myCount.range("go", "kafka"));
            while (range.hasNext()) {
                countRangeResults.add(range.next());
            }
        }

        try { 
 (KeyValueIterator<string, long> all = myCount.all());
            while (all.hasNext()) {
                countAllResults.add(all.next());
            }
        }

        Assert.Equal(countRangeResults, (expectedRangeResults));
        Assert.Equal(countAllResults, (expectedCount));
    }

    private void VerifyCanGetByKey(string[] keys,
                                   HashSet<KeyValuePair<string, long>> expectedWindowState,
                                   HashSet<KeyValuePair<string, long>> expectedCount,
                                   ReadOnlyWindowStore<string, long> windowStore,
                                   ReadOnlyKeyValueStore<string, long> myCount) {// throws Exception
        HashSet<KeyValuePair<string, long>> windowState = new TreeSet<>(stringLongComparator);
        HashSet<KeyValuePair<string, long>> countState = new TreeSet<>(stringLongComparator);

        long timeout = System.currentTimeMillis() + 30000;
        while ((windowState.Count < keys.Length ||
            countState.Count < keys.Length) &&
            System.currentTimeMillis() < timeout) {
            Thread.sleep(10);
            foreach (string key in keys) {
                windowState.addAll(Fetch(windowStore, key));
                long value = myCount.get(key);
                if (value != null) {
                    countState.add(new KeyValuePair<>(key, value));
                }
            }
        }
        Assert.Equal(windowState, (expectedWindowState));
        Assert.Equal(countState, (expectedCount));
    }

    /**
     * Verify that the new count is greater than or equal to the previous count.
     * Note: this method changes the values in expectedWindowState and expectedCount
     *
     * @param keys                  All the keys we ever expect to find
     * @param expectedWindowedCount Expected windowed count
     * @param expectedCount         Expected count
     * @param windowStore           Window Store
     * @param keyValueStore         Key-value store
     * @param failIfKeyNotFound     if true, tests fails if an expected key is not found in store. If false,
     *                              the method merely inserts the new found key into the list of
     *                              expected keys.
     */
    private void VerifyGreaterOrEqual(string[] keys,
                                      Dictionary<string, long> expectedWindowedCount,
                                      Dictionary<string, long> expectedCount,
                                      ReadOnlyWindowStore<string, long> windowStore,
                                      ReadOnlyKeyValueStore<string, long> keyValueStore,
                                      bool failIfKeyNotFound) {
        Dictionary<string, long> windowState = new HashMap<>();
        Dictionary<string, long> countState = new HashMap<>();

        foreach (string key in keys) {
            Dictionary<string, long> map = FetchMap(windowStore, key);
            if (map.equals(Collections.<string, long>emptyMap()) && failIfKeyNotFound) {
                Assert.True(false, "Key in windowed-store not found " + key);
            }
            windowState.putAll(map);
            long value = keyValueStore.get(key);
            if (value != null) {
                countState.put(key, value);
            } else if (failIfKeyNotFound) {
                Assert.True(false, "Key in key-value-store not found " + key);
            }
        }

        foreach (Map.Entry<string, long> actualWindowStateEntry in windowState.entrySet()) {
            if (expectedWindowedCount.containsKey(actualWindowStateEntry.getKey())) {
                long expectedValue = expectedWindowedCount.get(actualWindowStateEntry.getKey());
                Assert.True(actualWindowStateEntry.getValue() >= expectedValue);
            }
            // return this for next round of comparisons
            expectedWindowedCount.put(actualWindowStateEntry.getKey(), actualWindowStateEntry.getValue());
        }

        foreach (Map.Entry<string, long> actualCountStateEntry in countState.entrySet()) {
            if (expectedCount.containsKey(actualCountStateEntry.getKey())) {
                long expectedValue = expectedCount.get(actualCountStateEntry.getKey());
                Assert.True(actualCountStateEntry.getValue() >= expectedValue);
            }
            // return this for next round of comparisons
            expectedCount.put(actualCountStateEntry.getKey(), actualCountStateEntry.getValue());
        }

    }

    private void WaitUntilAtLeastNumRecordProcessed(string topic,
                                                    int numRecs) {// throws Exception
        Properties config = new Properties();
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "queryable-state-consumer");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.getName());
        config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.getName());
        IntegrationTestUtils.waitUntilMinValuesRecordsReceived(
            config,
            topic,
            numRecs,
            120 * 1000);
    }

    private HashSet<KeyValuePair<string, long>> Fetch(ReadOnlyWindowStore<string, long> store,
                                              string key) {
        WindowStoreIterator<long> fetch =
            store.fetch(key, ofEpochMilli(0), ofEpochMilli(System.currentTimeMillis()));
        if (fetch.hasNext()) {
            KeyValuePair<long, long> next = fetch.next();
            return Collections.singleton(KeyValuePair.Create(key, next.value));
        }
        return Collections.emptySet();
    }

    private Dictionary<string, long> FetchMap(ReadOnlyWindowStore<string, long> store,
                                       string key) {
        WindowStoreIterator<long> fetch =
            store.fetch(key, ofEpochMilli(0), ofEpochMilli(System.currentTimeMillis()));
        if (fetch.hasNext()) {
            KeyValuePair<long, long> next = fetch.next();
            return Collections.singletonMap(key, next.value);
        }
        return Collections.emptyMap();
    }

    /**
     * A class that periodically produces records in a separate thread
     */
    private class ProducerRunnable : Runnable {
        private string topic;
        private List<string> inputValues;
        private int numIterations;
        private int currIteration = 0;
        bool shutdown = false;

        ProducerRunnable(string topic,
                         List<string> inputValues,
                         int numIterations) {
            this.topic = topic;
            this.inputValues = inputValues;
            this.numIterations = numIterations;
        }

        private synchronized void IncrementIteration() {
            currIteration++;
        }

        synchronized int GetCurrIteration() {
            return currIteration;
        }

        synchronized void Shutdown() {
            shutdown = true;
        }

        
        public void Run() {
            Properties producerConfig = new Properties();
            producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
            producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
            producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer);
            producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer);

            try (KafkaProducer<string, string> producer =
                     new KafkaProducer<>(producerConfig, new StringSerializer(), new StringSerializer())) {

                while (GetCurrIteration() < numIterations && !shutdown) {
                    foreach (string value in inputValues) {
                        producer.send(new ProducerRecord<>(topic, value));
                    }
                    IncrementIteration();
                }
            }
        }
    }

}
