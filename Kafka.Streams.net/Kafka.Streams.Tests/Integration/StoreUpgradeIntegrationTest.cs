namespace Kafka.Streams.Tests.Integration
{
}
///*






// *

// *





// */










































//public class StoreUpgradeIntegrationTest {
//    private static string inputStream;
//    private static string STORE_NAME = "store";

//    private KafkaStreams kafkaStreams;
//    private static int testCounter = 0;


//    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);


//    public void createTopics() {// throws Exception
//        inputStream = "input-stream-" + testCounter;
//        CLUSTER.createTopic(inputStream);
//    }

//    private Properties props() {
//        Properties streamsConfiguration = new Properties();
//        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "addId-" + testCounter++);
//        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
//        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Int().getClass());
//        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Int().getClass());
//        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
//        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        return streamsConfiguration;
//    }


//    public void shutdown() {
//        if (kafkaStreams != null) {
//            kafkaStreams.close(Duration.ofSeconds(30L));
//            kafkaStreams.cleanUp();
//        }
//    }

//    [Xunit.Fact]
//    public void shouldMigrateInMemoryKeyValueStoreToTimestampedKeyValueStoreUsingPapi() {// throws Exception
//        shouldMigrateKeyValueStoreToTimestampedKeyValueStoreUsingPapi(false);
//    }

//    [Xunit.Fact]
//    public void shouldMigratePersistentKeyValueStoreToTimestampedKeyValueStoreUsingPapi() {// throws Exception
//        shouldMigrateKeyValueStoreToTimestampedKeyValueStoreUsingPapi(true);
//    }

//    private void shouldMigrateKeyValueStoreToTimestampedKeyValueStoreUsingPapi(bool persistentStore) {// throws Exception
//        StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();

//        streamsBuilderForOldStore.addStateStore(
//            Stores.keyValueStoreBuilder(
//                persistentStore ? Stores.persistentKeyValueStore(STORE_NAME) : Stores.inMemoryKeyValueStore(STORE_NAME),
//                Serdes.Int(),
//                Serdes.Long()))
//            .<int, int>stream(inputStream)
//            .process(KeyValueProcessor::new, STORE_NAME);

//        Properties props = props();
//        kafkaStreams = new KafkaStreams(streamsBuilderForOldStore.build(), props);
//        kafkaStreams.start();

//        processKeyValueAndVerifyPlainCount(1, singletonList(KeyValuePair.Create(1, 1L)));

//        processKeyValueAndVerifyPlainCount(1, singletonList(KeyValuePair.Create(1, 2L)));
//        long lastUpdateKeyOne = persistentStore ? -1L : CLUSTER.time.milliseconds() - 1L;

//        processKeyValueAndVerifyPlainCount(2, asList(
//            KeyValuePair.Create(1, 2L),
//            KeyValuePair.Create(2, 1L)));
//        long lastUpdateKeyTwo = persistentStore ? -1L : CLUSTER.time.milliseconds() - 1L;

//        processKeyValueAndVerifyPlainCount(3, asList(
//            KeyValuePair.Create(1, 2L),
//            KeyValuePair.Create(2, 1L),
//            KeyValuePair.Create(3, 1L)));
//        long lastUpdateKeyThree = persistentStore ? -1L : CLUSTER.time.milliseconds() - 1L;

//        processKeyValueAndVerifyPlainCount(4, asList(
//            KeyValuePair.Create(1, 2L),
//            KeyValuePair.Create(2, 1L),
//            KeyValuePair.Create(3, 1L),
//            KeyValuePair.Create(4, 1L)));

//        processKeyValueAndVerifyPlainCount(4, asList(
//            KeyValuePair.Create(1, 2L),
//            KeyValuePair.Create(2, 1L),
//            KeyValuePair.Create(3, 1L),
//            KeyValuePair.Create(4, 2L)));

//        processKeyValueAndVerifyPlainCount(4, asList(
//            KeyValuePair.Create(1, 2L),
//            KeyValuePair.Create(2, 1L),
//            KeyValuePair.Create(3, 1L),
//            KeyValuePair.Create(4, 3L)));
//        long lastUpdateKeyFour = persistentStore ? -1L : CLUSTER.time.milliseconds() - 1L;

//        kafkaStreams.close();
//        kafkaStreams = null;



//        StreamsBuilder streamsBuilderForNewStore = new StreamsBuilder();

//        streamsBuilderForNewStore.addStateStore(
//            Stores.timestampedKeyValueStoreBuilder(
//                persistentStore ? Stores.persistentTimestampedKeyValueStore(STORE_NAME) : Stores.inMemoryKeyValueStore(STORE_NAME),
//                Serdes.Int(),
//                Serdes.Long()))
//            .<int, int>stream(inputStream)
//            .process(TimestampedKeyValueProcessor::new, STORE_NAME);

//        kafkaStreams = new KafkaStreams(streamsBuilderForNewStore.build(), props);
//        kafkaStreams.start();

//        verifyCountWithTimestamp(1, 2L, lastUpdateKeyOne);
//        verifyCountWithTimestamp(2, 1L, lastUpdateKeyTwo);
//        verifyCountWithTimestamp(3, 1L, lastUpdateKeyThree);
//        verifyCountWithTimestamp(4, 3L, lastUpdateKeyFour);

//        long currentTime = CLUSTER.time.milliseconds();
//        processKeyValueAndVerifyCountWithTimestamp(1, currentTime + 42L, asList(
//            KeyValuePair.Create(1, ValueAndTimestamp.make(3L, currentTime + 42L)),
//            KeyValuePair.Create(2, ValueAndTimestamp.make(1L, lastUpdateKeyTwo)),
//            KeyValuePair.Create(3, ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
//            KeyValuePair.Create(4, ValueAndTimestamp.make(3L, lastUpdateKeyFour))));

//        processKeyValueAndVerifyCountWithTimestamp(2, currentTime + 45L, asList(
//            KeyValuePair.Create(1, ValueAndTimestamp.make(3L, currentTime + 42L)),
//            KeyValuePair.Create(2, ValueAndTimestamp.make(2L, currentTime + 45L)),
//            KeyValuePair.Create(3, ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
//            KeyValuePair.Create(4, ValueAndTimestamp.make(3L, lastUpdateKeyFour))));

//        // can process "out of order" record for different key
//        processKeyValueAndVerifyCountWithTimestamp(4, currentTime + 21L, asList(
//            KeyValuePair.Create(1, ValueAndTimestamp.make(3L, currentTime + 42L)),
//            KeyValuePair.Create(2, ValueAndTimestamp.make(2L, currentTime + 45L)),
//            KeyValuePair.Create(3, ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
//            KeyValuePair.Create(4, ValueAndTimestamp.make(4L, currentTime + 21L))));

//        processKeyValueAndVerifyCountWithTimestamp(4, currentTime + 42L, asList(
//            KeyValuePair.Create(1, ValueAndTimestamp.make(3L, currentTime + 42L)),
//            KeyValuePair.Create(2, ValueAndTimestamp.make(2L, currentTime + 45L)),
//            KeyValuePair.Create(3, ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
//            KeyValuePair.Create(4, ValueAndTimestamp.make(5L, currentTime + 42L))));

//        // out of order (same key) record should not reduce result timestamp
//        processKeyValueAndVerifyCountWithTimestamp(4, currentTime + 10L, asList(
//            KeyValuePair.Create(1, ValueAndTimestamp.make(3L, currentTime + 42L)),
//            KeyValuePair.Create(2, ValueAndTimestamp.make(2L, currentTime + 45L)),
//            KeyValuePair.Create(3, ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
//            KeyValuePair.Create(4, ValueAndTimestamp.make(6L, currentTime + 42L))));

//        kafkaStreams.close();
//    }

//    [Xunit.Fact]
//    public void shouldProxyKeyValueStoreToTimestampedKeyValueStoreUsingPapi() {// throws Exception
//        StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();

//        streamsBuilderForOldStore.addStateStore(
//            Stores.keyValueStoreBuilder(
//                Stores.persistentKeyValueStore(STORE_NAME),
//                Serdes.Int(),
//                Serdes.Long()))
//            .<int, int>stream(inputStream)
//            .process(KeyValueProcessor::new, STORE_NAME);

//        Properties props = props();
//        kafkaStreams = new KafkaStreams(streamsBuilderForOldStore.build(), props);
//        kafkaStreams.start();

//        processKeyValueAndVerifyPlainCount(1, singletonList(KeyValuePair.Create(1, 1L)));

//        processKeyValueAndVerifyPlainCount(1, singletonList(KeyValuePair.Create(1, 2L)));

//        processKeyValueAndVerifyPlainCount(2, asList(
//            KeyValuePair.Create(1, 2L),
//            KeyValuePair.Create(2, 1L)));

//        processKeyValueAndVerifyPlainCount(3, asList(
//            KeyValuePair.Create(1, 2L),
//            KeyValuePair.Create(2, 1L),
//            KeyValuePair.Create(3, 1L)));

//        processKeyValueAndVerifyPlainCount(4, asList(
//            KeyValuePair.Create(1, 2L),
//            KeyValuePair.Create(2, 1L),
//            KeyValuePair.Create(3, 1L),
//            KeyValuePair.Create(4, 1L)));

//        processKeyValueAndVerifyPlainCount(4, asList(
//            KeyValuePair.Create(1, 2L),
//            KeyValuePair.Create(2, 1L),
//            KeyValuePair.Create(3, 1L),
//            KeyValuePair.Create(4, 2L)));

//        processKeyValueAndVerifyPlainCount(4, asList(
//            KeyValuePair.Create(1, 2L),
//            KeyValuePair.Create(2, 1L),
//            KeyValuePair.Create(3, 1L),
//            KeyValuePair.Create(4, 3L)));

//        kafkaStreams.close();
//        kafkaStreams = null;



//        StreamsBuilder streamsBuilderForNewStore = new StreamsBuilder();

//        streamsBuilderForNewStore.addStateStore(
//            Stores.timestampedKeyValueStoreBuilder(
//                Stores.persistentKeyValueStore(STORE_NAME),
//                Serdes.Int(),
//                Serdes.Long()))
//            .<int, int>stream(inputStream)
//            .process(TimestampedKeyValueProcessor::new, STORE_NAME);

//        kafkaStreams = new KafkaStreams(streamsBuilderForNewStore.build(), props);
//        kafkaStreams.start();

//        verifyCountWithSurrogateTimestamp(1, 2L);
//        verifyCountWithSurrogateTimestamp(2, 1L);
//        verifyCountWithSurrogateTimestamp(3, 1L);
//        verifyCountWithSurrogateTimestamp(4, 3L);

//        processKeyValueAndVerifyCount(1, 42L, asList(
//            KeyValuePair.Create(1, ValueAndTimestamp.make(3L, -1L)),
//            KeyValuePair.Create(2, ValueAndTimestamp.make(1L, -1L)),
//            KeyValuePair.Create(3, ValueAndTimestamp.make(1L, -1L)),
//            KeyValuePair.Create(4, ValueAndTimestamp.make(3L, -1L))));

//        processKeyValueAndVerifyCount(2, 45L, asList(
//            KeyValuePair.Create(1, ValueAndTimestamp.make(3L, -1L)),
//            KeyValuePair.Create(2, ValueAndTimestamp.make(2L, -1L)),
//            KeyValuePair.Create(3, ValueAndTimestamp.make(1L, -1L)),
//            KeyValuePair.Create(4, ValueAndTimestamp.make(3L, -1L))));

//        // can process "out of order" record for different key
//        processKeyValueAndVerifyCount(4, 21L, asList(
//            KeyValuePair.Create(1, ValueAndTimestamp.make(3L, -1L)),
//            KeyValuePair.Create(2, ValueAndTimestamp.make(2L, -1L)),
//            KeyValuePair.Create(3, ValueAndTimestamp.make(1L, -1L)),
//            KeyValuePair.Create(4, ValueAndTimestamp.make(4L, -1L))));

//        processKeyValueAndVerifyCount(4, 42L, asList(
//            KeyValuePair.Create(1, ValueAndTimestamp.make(3L, -1L)),
//            KeyValuePair.Create(2, ValueAndTimestamp.make(2L, -1L)),
//            KeyValuePair.Create(3, ValueAndTimestamp.make(1L, -1L)),
//            KeyValuePair.Create(4, ValueAndTimestamp.make(5L, -1L))));

//        // out of order (same key) record should not reduce result timestamp
//        processKeyValueAndVerifyCount(4, 10L, asList(
//            KeyValuePair.Create(1, ValueAndTimestamp.make(3L, -1L)),
//            KeyValuePair.Create(2, ValueAndTimestamp.make(2L, -1L)),
//            KeyValuePair.Create(3, ValueAndTimestamp.make(1L, -1L)),
//            KeyValuePair.Create(4, ValueAndTimestamp.make(6L, -1L))));

//        kafkaStreams.close();
//    }

//    private void processKeyValueAndVerifyPlainCount<K, V>(K key,
//                                                           List<KeyValuePair<int, object>> expectedStoreContent)
//            //throws Exception {

//        IntegrationTestUtils.produceKeyValuesSynchronously(
//            inputStream,
//            singletonList(KeyValuePair.Create(key, 0)),
//            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
//                IntegerSerializer,
//                IntegerSerializer),
//            CLUSTER.time);

//        TestUtils.waitForCondition(
//            () => {
//                try {
//                    ReadOnlyKeyValueStore<K, V> store =
//                        kafkaStreams.store(STORE_NAME, QueryableStoreTypes.keyValueStore());
//                    try { 
// (KeyValueIterator<K, V> all = store.all());
//                        List<KeyValuePair<K, V>> storeContent = new LinkedList<>();
//                        while (all.hasNext()) {
//                            storeContent.add(all.next());
//                        }
//                        return storeContent.equals(expectedStoreContent);
//                    }
//                } catch (Exception swallow) {
//                    swallow.printStackTrace();
//                    System.Console.Error.println(swallow.getMessage());
//                    return false;
//                }
//            },
//            60_000L,
//            "Could not get expected result in time.");
//    }

//    private <K> void verifyCountWithTimestamp(K key,
//                                              long value,
//                                              long timestamp) {// throws Exception
//        TestUtils.waitForCondition(
//            () => {
//                try {
//                    ReadOnlyKeyValueStore<K, ValueAndTimestamp<long>> store =
//                        kafkaStreams.store(STORE_NAME, QueryableStoreTypes.timestampedKeyValueStore());
//                    ValueAndTimestamp<long> count = store.get(key);
//                    return count.Value == value && count.Timestamp == timestamp;
//                } catch (Exception swallow) {
//                    swallow.printStackTrace();
//                    System.Console.Error.println(swallow.getMessage());
//                    return false;
//                }
//            },
//            60_000L,
//            "Could not get expected result in time.");
//    }

//    private <K> void verifyCountWithSurrogateTimestamp(K key,
//                                                       long value) {// throws Exception
//        TestUtils.waitForCondition(
//            () => {
//                try {
//                    ReadOnlyKeyValueStore<K, ValueAndTimestamp<long>> store =
//                        kafkaStreams.store(STORE_NAME, QueryableStoreTypes.timestampedKeyValueStore());
//                    ValueAndTimestamp<long> count = store.get(key);
//                    return count.Value == value && count.Timestamp == -1L;
//                } catch (Exception swallow) {
//                    swallow.printStackTrace();
//                    System.Console.Error.println(swallow.getMessage());
//                    return false;
//                }
//            },
//            60_000L,
//            "Could not get expected result in time.");
//    }

//    private void processKeyValueAndVerifyCount<K, V>(K key,
//                                                      long timestamp,
//                                                      List<KeyValuePair<int, object>> expectedStoreContent)
//            //throws Exception {

//        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
//            inputStream,
//            singletonList(KeyValuePair.Create(key, 0)),
//            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
//                IntegerSerializer,
//                IntegerSerializer),
//            timestamp);

//        TestUtils.waitForCondition(
//            () => {
//                try {
//                    ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>> store =
//                        kafkaStreams.store(STORE_NAME, QueryableStoreTypes.timestampedKeyValueStore());
//                    try { 
// (KeyValueIterator<K, ValueAndTimestamp<V>> all = store.all());
//                        List<KeyValuePair<K, ValueAndTimestamp<V>>> storeContent = new LinkedList<>();
//                        while (all.hasNext()) {
//                            storeContent.add(all.next());
//                        }
//                        return storeContent.equals(expectedStoreContent);
//                    }
//                } catch (Exception swallow) {
//                    swallow.printStackTrace();
//                    System.Console.Error.println(swallow.getMessage());
//                    return false;
//                }
//            },
//            60_000L,
//            "Could not get expected result in time.");
//    }

//    private void processKeyValueAndVerifyCountWithTimestamp<K, V>(K key,
//                                                                   long timestamp,
//                                                                   List<KeyValuePair<int, object>> expectedStoreContent)
//        //throws Exception {

//        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
//            inputStream,
//            singletonList(KeyValuePair.Create(key, 0)),
//            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
//                IntegerSerializer,
//                IntegerSerializer),
//            timestamp);

//        TestUtils.waitForCondition(
//            () => {
//                try {
//                    ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>> store =
//                        kafkaStreams.store(STORE_NAME, QueryableStoreTypes.timestampedKeyValueStore());
//                    try { 
// (KeyValueIterator<K, ValueAndTimestamp<V>> all = store.all());
//                        List<KeyValuePair<K, ValueAndTimestamp<V>>> storeContent = new LinkedList<>();
//                        while (all.hasNext()) {
//                            storeContent.add(all.next());
//                        }
//                        return storeContent.equals(expectedStoreContent);
//                    }
//                } catch (Exception swallow) {
//                    swallow.printStackTrace();
//                    System.Console.Error.println(swallow.getMessage());
//                    return false;
//                }
//            },
//            60_000L,
//            "Could not get expected result in time.");
//    }

//    [Xunit.Fact]
//    public void shouldMigrateInMemoryWindowStoreToTimestampedWindowStoreUsingPapi() {// throws Exception
//        StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();
//        streamsBuilderForOldStore
//            .addStateStore(
//                Stores.windowStoreBuilder(
//                    Stores.inMemoryWindowStore(
//                        STORE_NAME,
//                        Duration.ofMillis(1000L),
//                        Duration.ofMillis(1000L),
//                        false),
//                Serdes.Int(),
//                Serdes.Long()))
//            .<int, int>stream(inputStream)
//            .process(WindowedProcessor::new, STORE_NAME);

//        StreamsBuilder streamsBuilderForNewStore = new StreamsBuilder();
//        streamsBuilderForNewStore
//            .addStateStore(
//                Stores.timestampedWindowStoreBuilder(
//                    Stores.inMemoryWindowStore(
//                        STORE_NAME,
//                        Duration.ofMillis(1000L),
//                        Duration.ofMillis(1000L),
//                        false),
//            Serdes.Int(),
//            Serdes.Long()))
//            .<int, int>stream(inputStream)
//            .process(TimestampedWindowedProcessor::new, STORE_NAME);


//        shouldMigrateWindowStoreToTimestampedWindowStoreUsingPapi(
//            new KafkaStreams(streamsBuilderForOldStore.build(), props()),
//            new KafkaStreams(streamsBuilderForNewStore.build(), props()),
//            false);
//    }

//    [Xunit.Fact]
//    public void shouldMigratePersistentWindowStoreToTimestampedWindowStoreUsingPapi() {// throws Exception
//        StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();

//        streamsBuilderForOldStore
//            .addStateStore(
//                Stores.windowStoreBuilder(
//                    Stores.persistentWindowStore(
//                        STORE_NAME,
//                        Duration.ofMillis(1000L),
//                        Duration.ofMillis(1000L),
//                        false),
//                    Serdes.Int(),
//                    Serdes.Long()))
//            .<int, int>stream(inputStream)
//            .process(WindowedProcessor::new, STORE_NAME);

//        StreamsBuilder streamsBuilderForNewStore = new StreamsBuilder();
//        streamsBuilderForNewStore
//            .addStateStore(
//                Stores.timestampedWindowStoreBuilder(
//                    Stores.persistentTimestampedWindowStore(
//                        STORE_NAME,
//                        Duration.ofMillis(1000L),
//                        Duration.ofMillis(1000L),
//                        false),
//                    Serdes.Int(),
//                    Serdes.Long()))
//            .<int, int>stream(inputStream)
//            .process(TimestampedWindowedProcessor::new, STORE_NAME);

//        Properties props = props();
//        shouldMigrateWindowStoreToTimestampedWindowStoreUsingPapi(
//            new KafkaStreams(streamsBuilderForOldStore.build(), props),
//            new KafkaStreams(streamsBuilderForNewStore.build(), props),
//            true);
//    }

//    private void shouldMigrateWindowStoreToTimestampedWindowStoreUsingPapi(KafkaStreams kafkaStreamsOld,
//                                                                           KafkaStreams kafkaStreamsNew,
//                                                                           bool persistentStore) {// throws Exception
//        kafkaStreams = kafkaStreamsOld;
//        kafkaStreams.start();

//        processWindowedKeyValueAndVerifyPlainCount(1, singletonList(
//            KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0L, 1000L)), 1L)));

//        processWindowedKeyValueAndVerifyPlainCount(1, singletonList(
//            KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L)));
//        long lastUpdateKeyOne = persistentStore ? -1L : CLUSTER.time.milliseconds() - 1L;

//        processWindowedKeyValueAndVerifyPlainCount(2, asList(
//            KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
//            KeyValuePair.Create(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L)));
//        long lastUpdateKeyTwo = persistentStore ? -1L : CLUSTER.time.milliseconds() - 1L;

//        processWindowedKeyValueAndVerifyPlainCount(3, asList(
//            KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
//            KeyValuePair.Create(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L),
//            KeyValuePair.Create(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L)));
//        long lastUpdateKeyThree = persistentStore ? -1L : CLUSTER.time.milliseconds() - 1L;

//        processWindowedKeyValueAndVerifyPlainCount(4, asList(
//            KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
//            KeyValuePair.Create(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L),
//            KeyValuePair.Create(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L),
//            KeyValuePair.Create(new Windowed<>(4, new TimeWindow(0L, 1000L)), 1L)));

//        processWindowedKeyValueAndVerifyPlainCount(4, asList(
//            KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
//            KeyValuePair.Create(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L),
//            KeyValuePair.Create(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L),
//            KeyValuePair.Create(new Windowed<>(4, new TimeWindow(0L, 1000L)), 2L)));

//        processWindowedKeyValueAndVerifyPlainCount(4, asList(
//            KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
//            KeyValuePair.Create(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L),
//            KeyValuePair.Create(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L),
//            KeyValuePair.Create(new Windowed<>(4, new TimeWindow(0L, 1000L)), 3L)));
//        long lastUpdateKeyFour = persistentStore ? -1L : CLUSTER.time.milliseconds() - 1L;

//        kafkaStreams.close();
//        kafkaStreams = null;


//        kafkaStreams = kafkaStreamsNew;
//        kafkaStreams.start();

//        verifyWindowedCountWithTimestamp(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L, lastUpdateKeyOne);
//        verifyWindowedCountWithTimestamp(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L, lastUpdateKeyTwo);
//        verifyWindowedCountWithTimestamp(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L, lastUpdateKeyThree);
//        verifyWindowedCountWithTimestamp(new Windowed<>(4, new TimeWindow(0L, 1000L)), 3L, lastUpdateKeyFour);

//        long currentTime = CLUSTER.time.milliseconds();
//        processKeyValueAndVerifyWindowedCountWithTimestamp(1, currentTime + 42L, asList(
//            KeyValuePair.Create(
//                new Windowed<>(1, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(3L, currentTime + 42L)),
//            KeyValuePair.Create(
//                new Windowed<>(2, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(1L, lastUpdateKeyTwo)),
//            KeyValuePair.Create(
//                new Windowed<>(3, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
//            KeyValuePair.Create(
//                new Windowed<>(4, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(3L, lastUpdateKeyFour))));

//        processKeyValueAndVerifyWindowedCountWithTimestamp(2, currentTime + 45L, asList(
//            KeyValuePair.Create(
//                new Windowed<>(1, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(3L, currentTime + 42L)),
//            KeyValuePair.Create(
//                new Windowed<>(2, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(2L, currentTime + 45L)),
//            KeyValuePair.Create(
//                new Windowed<>(3, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
//            KeyValuePair.Create(
//                new Windowed<>(4, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(3L, lastUpdateKeyFour))));

//        // can process "out of order" record for different key
//        processKeyValueAndVerifyWindowedCountWithTimestamp(4, currentTime + 21L, asList(
//            KeyValuePair.Create(
//                new Windowed<>(1, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(3L, currentTime + 42L)),
//            KeyValuePair.Create(
//                new Windowed<>(2, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(2L, currentTime + 45L)),
//            KeyValuePair.Create(
//                new Windowed<>(3, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
//            KeyValuePair.Create(
//                new Windowed<>(4, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(4L, currentTime + 21L))));

//        processKeyValueAndVerifyWindowedCountWithTimestamp(4, currentTime + 42L, asList(
//            KeyValuePair.Create(
//                new Windowed<>(1, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(3L, currentTime + 42L)),
//            KeyValuePair.Create(
//                new Windowed<>(2, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(2L, currentTime + 45L)),
//            KeyValuePair.Create(
//                new Windowed<>(3, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
//            KeyValuePair.Create(
//                new Windowed<>(4, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(5L, currentTime + 42L))));

//        // out of order (same key) record should not reduce result timestamp
//        processKeyValueAndVerifyWindowedCountWithTimestamp(4, currentTime + 10L, asList(
//            KeyValuePair.Create(
//                new Windowed<>(1, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(3L, currentTime + 42L)),
//            KeyValuePair.Create(
//                new Windowed<>(2, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(2L, currentTime + 45L)),
//            KeyValuePair.Create(
//                new Windowed<>(3, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(1L, lastUpdateKeyThree)),
//            KeyValuePair.Create(
//                new Windowed<>(4, new TimeWindow(0L, 1000L)),
//                ValueAndTimestamp.make(6L, currentTime + 42L))));

//        // test new segment
//        processKeyValueAndVerifyWindowedCountWithTimestamp(10, currentTime + 100001L, singletonList(
//            KeyValuePair.Create(
//                new Windowed<>(10, new TimeWindow(100000L, 101000L)), ValueAndTimestamp.make(1L, currentTime + 100001L))));


//        kafkaStreams.close();
//    }

//    [Xunit.Fact]
//    public void shouldProxyWindowStoreToTimestampedWindowStoreUsingPapi() {// throws Exception
//        StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();

//        streamsBuilderForOldStore.addStateStore(
//            Stores.windowStoreBuilder(
//                Stores.persistentWindowStore(
//                    STORE_NAME,
//                    Duration.ofMillis(1000L),
//                    Duration.ofMillis(1000L),
//                    false),
//                Serdes.Int(),
//                Serdes.Long()))
//            .<int, int>stream(inputStream)
//            .process(WindowedProcessor::new, STORE_NAME);

//        Properties props = props();
//        kafkaStreams = new KafkaStreams(streamsBuilderForOldStore.build(), props);
//        kafkaStreams.start();

//        processWindowedKeyValueAndVerifyPlainCount(1, singletonList(
//            KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0L, 1000L)), 1L)));

//        processWindowedKeyValueAndVerifyPlainCount(1, singletonList(
//            KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L)));

//        processWindowedKeyValueAndVerifyPlainCount(2, asList(
//            KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
//            KeyValuePair.Create(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L)));

//        processWindowedKeyValueAndVerifyPlainCount(3, asList(
//            KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
//            KeyValuePair.Create(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L),
//            KeyValuePair.Create(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L)));

//        processWindowedKeyValueAndVerifyPlainCount(4, asList(
//            KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
//            KeyValuePair.Create(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L),
//            KeyValuePair.Create(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L),
//            KeyValuePair.Create(new Windowed<>(4, new TimeWindow(0L, 1000L)), 1L)));

//        processWindowedKeyValueAndVerifyPlainCount(4, asList(
//            KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
//            KeyValuePair.Create(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L),
//            KeyValuePair.Create(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L),
//            KeyValuePair.Create(new Windowed<>(4, new TimeWindow(0L, 1000L)), 2L)));

//        processWindowedKeyValueAndVerifyPlainCount(4, asList(
//            KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L),
//            KeyValuePair.Create(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L),
//            KeyValuePair.Create(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L),
//            KeyValuePair.Create(new Windowed<>(4, new TimeWindow(0L, 1000L)), 3L)));

//        kafkaStreams.close();
//        kafkaStreams = null;



//        StreamsBuilder streamsBuilderForNewStore = new StreamsBuilder();

//        streamsBuilderForNewStore.addStateStore(
//            Stores.timestampedWindowStoreBuilder(
//                Stores.persistentWindowStore(
//                    STORE_NAME,
//                    Duration.ofMillis(1000L),
//                    Duration.ofMillis(1000L),
//                    false),
//                Serdes.Int(),
//                Serdes.Long()))
//            .<int, int>stream(inputStream)
//            .process(TimestampedWindowedProcessor::new, STORE_NAME);

//        kafkaStreams = new KafkaStreams(streamsBuilderForNewStore.build(), props);
//        kafkaStreams.start();

//        verifyWindowedCountWithSurrogateTimestamp(new Windowed<>(1, new TimeWindow(0L, 1000L)), 2L);
//        verifyWindowedCountWithSurrogateTimestamp(new Windowed<>(2, new TimeWindow(0L, 1000L)), 1L);
//        verifyWindowedCountWithSurrogateTimestamp(new Windowed<>(3, new TimeWindow(0L, 1000L)), 1L);
//        verifyWindowedCountWithSurrogateTimestamp(new Windowed<>(4, new TimeWindow(0L, 1000L)), 3L);

//        processKeyValueAndVerifyWindowedCountWithTimestamp(1, 42L, asList(
//            KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(3L, -1L)),
//            KeyValuePair.Create(new Windowed<>(2, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(1L, -1L)),
//            KeyValuePair.Create(new Windowed<>(3, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(1L, -1L)),
//            KeyValuePair.Create(new Windowed<>(4, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(3L, -1L))));

//        processKeyValueAndVerifyWindowedCountWithTimestamp(2, 45L, asList(
//            KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(3L, -1L)),
//            KeyValuePair.Create(new Windowed<>(2, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(2L, -1L)),
//            KeyValuePair.Create(new Windowed<>(3, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(1L, -1L)),
//            KeyValuePair.Create(new Windowed<>(4, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(3L, -1L))));

//        // can process "out of order" record for different key
//        processKeyValueAndVerifyWindowedCountWithTimestamp(4, 21L, asList(
//            KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(3L, -1L)),
//            KeyValuePair.Create(new Windowed<>(2, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(2L, -1L)),
//            KeyValuePair.Create(new Windowed<>(3, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(1L, -1L)),
//            KeyValuePair.Create(new Windowed<>(4, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(4L, -1L))));

//        processKeyValueAndVerifyWindowedCountWithTimestamp(4, 42L, asList(
//            KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(3L, -1L)),
//            KeyValuePair.Create(new Windowed<>(2, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(2L, -1L)),
//            KeyValuePair.Create(new Windowed<>(3, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(1L, -1L)),
//            KeyValuePair.Create(new Windowed<>(4, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(5L, -1L))));

//        // out of order (same key) record should not reduce result timestamp
//        processKeyValueAndVerifyWindowedCountWithTimestamp(4, 10L, asList(
//            KeyValuePair.Create(new Windowed<>(1, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(3L, -1L)),
//            KeyValuePair.Create(new Windowed<>(2, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(2L, -1L)),
//            KeyValuePair.Create(new Windowed<>(3, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(1L, -1L)),
//            KeyValuePair.Create(new Windowed<>(4, new TimeWindow(0L, 1000L)), ValueAndTimestamp.make(6L, -1L))));

//        // test new segment
//        processKeyValueAndVerifyWindowedCountWithTimestamp(10, 100001L, singletonList(
//            KeyValuePair.Create(new Windowed<>(10, new TimeWindow(100000L, 101000L)), ValueAndTimestamp.make(1L, -1L))));


//        kafkaStreams.close();
//    }

//    private void processWindowedKeyValueAndVerifyPlainCount<K, V>(K key,
//                                                                   List<KeyValuePair<Windowed<int>, object>> expectedStoreContent)
//            //throws Exception {

//        IntegrationTestUtils.produceKeyValuesSynchronously(
//            inputStream,
//            singletonList(KeyValuePair.Create(key, 0)),
//            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
//                IntegerSerializer,
//                IntegerSerializer),
//            CLUSTER.time);

//        TestUtils.waitForCondition(
//            () => {
//                try {
//                    ReadOnlyWindowStore<K, V> store =
//                        kafkaStreams.store(STORE_NAME, QueryableStoreTypes.windowStore());
//                    try { 
// (KeyValueIterator<Windowed<K>, V> all = store.all());
//                        List<KeyValuePair<Windowed<K>, V>> storeContent = new LinkedList<>();
//                        while (all.hasNext()) {
//                            storeContent.add(all.next());
//                        }
//                        return storeContent.equals(expectedStoreContent);
//                    }
//                } catch (Exception swallow) {
//                    swallow.printStackTrace();
//                    System.Console.Error.println(swallow.getMessage());
//                    return false;
//                }
//            },
//            60_000L,
//            "Could not get expected result in time.");
//    }

//    private <K> void verifyWindowedCountWithSurrogateTimestamp(Windowed<K> key,
//                                                               long value) {// throws Exception
//        TestUtils.waitForCondition(
//            () => {
//                try {
//                    ReadOnlyWindowStore<K, ValueAndTimestamp<long>> store =
//                        kafkaStreams.store(STORE_NAME, QueryableStoreTypes.timestampedWindowStore());
//                    ValueAndTimestamp<long> count = store.fetch(key.Key, key.window().start());
//                    return count.Value == value && count.Timestamp == -1L;
//                } catch (Exception swallow) {
//                    swallow.printStackTrace();
//                    System.Console.Error.println(swallow.getMessage());
//                    return false;
//                }
//            },
//            60_000L,
//            "Could not get expected result in time.");
//    }

//    private <K> void verifyWindowedCountWithTimestamp(Windowed<K> key,
//                                                      long value,
//                                                      long timestamp) {// throws Exception
//        TestUtils.waitForCondition(
//            () => {
//                try {
//                    ReadOnlyWindowStore<K, ValueAndTimestamp<long>> store =
//                        kafkaStreams.store(STORE_NAME, QueryableStoreTypes.timestampedWindowStore());
//                    ValueAndTimestamp<long> count = store.fetch(key.Key, key.window().start());
//                    return count.Value == value && count.Timestamp == timestamp;
//                } catch (Exception swallow) {
//                    swallow.printStackTrace();
//                    System.Console.Error.println(swallow.getMessage());
//                    return false;
//                }
//            },
//            60_000L,
//            "Could not get expected result in time.");
//    }

//    private void processKeyValueAndVerifyWindowedCountWithTimestamp<K, V>(K key,
//                                                                           long timestamp,
//                                                                           List<KeyValuePair<Windowed<int>, object>> expectedStoreContent)
//            //throws Exception {

//        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
//            inputStream,
//            singletonList(KeyValuePair.Create(key, 0)),
//            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
//                IntegerSerializer,
//                IntegerSerializer),
//            timestamp);

//        TestUtils.waitForCondition(
//            () => {
//                try {
//                    ReadOnlyWindowStore<K, ValueAndTimestamp<V>> store =
//                        kafkaStreams.store(STORE_NAME, QueryableStoreTypes.timestampedWindowStore());
//                    try { 
// (KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> all = store.all());
//                        List<KeyValuePair<Windowed<K>, ValueAndTimestamp<V>>> storeContent = new LinkedList<>();
//                        while (all.hasNext()) {
//                            storeContent.add(all.next());
//                        }
//                        return storeContent.equals(expectedStoreContent);
//                    }
//                } catch (Exception swallow) {
//                    swallow.printStackTrace();
//                    System.Console.Error.println(swallow.getMessage());
//                    return false;
//                }
//            },
//            60_000L,
//            "Could not get expected result in time.");
//    }

//    private static class KeyValueProcessor : Processor<int, int> {
//        private KeyValueStore<int, long> store;



//        public void init(ProcessorContext context) {
//            store = (KeyValueStore<int, long>) context.getStateStore(STORE_NAME);
//        }


//        public void process(int key, int value) {
//            long newCount;

//            long oldCount = store.get(key);
//            if (oldCount != null) {
//                newCount = oldCount + 1L;
//            } else {
//                newCount = 1L;
//            }

//            store.put(key, newCount);
//        }


//        public void close() {}
//    }

//    private static class TimestampedKeyValueProcessor : Processor<int, int> {
//        private ProcessorContext context;
//        private TimestampedKeyValueStore<int, long> store;



//        public void init(ProcessorContext context) {
//            this.context = context;
//            store = (TimestampedKeyValueStore<int, long>) context.getStateStore(STORE_NAME);
//        }


//        public void process(int key, int value) {
//            long newCount;

//            ValueAndTimestamp<long> oldCountWithTimestamp = store.get(key);
//            long newTimestamp;

//            if (oldCountWithTimestamp == null) {
//                newCount = 1L;
//                newTimestamp = context.Timestamp;
//            } else {
//                newCount = oldCountWithTimestamp.Value + 1L;
//                newTimestamp = Math.max(oldCountWithTimestamp.Timestamp, context.Timestamp);
//            }

//            store.put(key, ValueAndTimestamp.make(newCount, newTimestamp));
//        }


//        public void close() {}
//    }

//    private static class WindowedProcessor : Processor<int, int> {
//        private WindowStore<int, long> store;



//        public void init(ProcessorContext context) {
//            store = (WindowStore<int, long>) context.getStateStore(STORE_NAME);
//        }


//        public void process(int key, int value) {
//            long newCount;

//            long oldCount = store.fetch(key, key < 10 ? 0L : 100000L);
//            if (oldCount != null) {
//                newCount = oldCount + 1L;
//            } else {
//                newCount = 1L;
//            }

//            store.put(key, newCount, key < 10 ? 0L : 100000L);
//        }


//        public void close() {}
//    }

//    private static class TimestampedWindowedProcessor : Processor<int, int> {
//        private ProcessorContext context;
//        private TimestampedWindowStore<int, long> store;



//        public void init(ProcessorContext context) {
//            this.context = context;
//            store = (TimestampedWindowStore<int, long>) context.getStateStore(STORE_NAME);
//        }


//        public void process(int key, int value) {
//            long newCount;

//            ValueAndTimestamp<long> oldCountWithTimestamp = store.fetch(key, key < 10 ? 0L : 100000L);
//            long newTimestamp;

//            if (oldCountWithTimestamp == null) {
//                newCount = 1L;
//                newTimestamp = context.Timestamp;
//            } else {
//                newCount = oldCountWithTimestamp.Value + 1L;
//                newTimestamp = Math.max(oldCountWithTimestamp.Timestamp, context.Timestamp);
//            }

//            store.put(key, ValueAndTimestamp.make(newCount, newTimestamp), key < 10 ? 0L : 100000L);
//        }


//        public void close() {}
//    }
//}