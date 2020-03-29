/*






 *

 *





 */



























































public class SuppressionIntegrationTest {
    
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
        1,
        mkProperties(mkMap()),
        0L
    );
    private static StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static Serde<string> STRING_SERDE = Serdes.String();
    private static int COMMIT_INTERVAL = 100;

    private static KTable<string, long> BuildCountsTable(string input, StreamsBuilder builder) {
        return builder
            .table(
                input,
                Consumed.with(STRING_SERDE, STRING_SERDE),
                Materialized.<string, string, KeyValueStore<Bytes, byte[]>>with(STRING_SERDE, STRING_SERDE)
                    .withCachingDisabled()
                    .withLoggingDisabled()
            )
            .groupBy((k, v) => new KeyValuePair<>(v, k), Grouped.with(STRING_SERDE, STRING_SERDE))
            .count(Materialized<string, long, KeyValueStore<Bytes, byte[]>>.As("counts").withCachingDisabled());
    }

    [Xunit.Fact]
    public void ShouldUseDefaultSerdes() {
        string testId = "-shouldInheritSerdes";
        string appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
        string input = "input" + testId;
        string outputSuppressed = "output-suppressed" + testId;
        string outputRaw = "output-raw" + testId;

        cleanStateBeforeTest(CLUSTER, input, outputRaw, outputSuppressed);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<string, string> inputStream = builder.stream(input);

        KTable<string, string> valueCounts = inputStream
            .groupByKey()
            .aggregate(() => "()", (key, value, aggregate) => aggregate + ",(" + key + ": " + value + ")");

        valueCounts
            .suppress(untilTimeLimit(ofMillis(MAX_VALUE), maxRecords(1L).emitEarlyWhenFull()))
            .toStream()
            .to(outputSuppressed);

        valueCounts
            .toStream()
            .to(outputRaw);

        Properties streamsConfig = GetStreamsConfig(appId);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde);
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde);

        KafkaStreams driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
        try {
            produceSynchronously(
                input,
                asList(
                    new KeyValueTimestamp<>("k1", "v1", ScaledTime(0L)),
                    new KeyValueTimestamp<>("k1", "v2", ScaledTime(1L)),
                    new KeyValueTimestamp<>("k2", "v1", ScaledTime(2L)),
                    new KeyValueTimestamp<>("x", "x", ScaledTime(3L))
                )
            );
            bool rawRecords = WaitForAnyRecord(outputRaw);
            bool suppressedRecords = WaitForAnyRecord(outputSuppressed);
            Assert.Equal(rawRecords, Matchers.is(true));
            Assert.Equal(suppressedRecords, is(true));
        } finally {
            driver.close();
            cleanStateAfterTest(CLUSTER, driver);
        }
    }

    [Xunit.Fact]
    public void ShouldInheritSerdes() {
        string testId = "-shouldInheritSerdes";
        string appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
        string input = "input" + testId;
        string outputSuppressed = "output-suppressed" + testId;
        string outputRaw = "output-raw" + testId;

        cleanStateBeforeTest(CLUSTER, input, outputRaw, outputSuppressed);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<string, string> inputStream = builder.stream(input);

        // count sets the serde to long
        KTable<string, long> valueCounts = inputStream
            .groupByKey()
            .count();

        valueCounts
            .suppress(untilTimeLimit(ofMillis(MAX_VALUE), maxRecords(1L).emitEarlyWhenFull()))
            .toStream()
            .to(outputSuppressed);

        valueCounts
            .toStream()
            .to(outputRaw);

        Properties streamsConfig = GetStreamsConfig(appId);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde);
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde);

        KafkaStreams driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
        try {
            produceSynchronously(
                input,
                asList(
                    new KeyValueTimestamp<>("k1", "v1", ScaledTime(0L)),
                    new KeyValueTimestamp<>("k1", "v2", ScaledTime(1L)),
                    new KeyValueTimestamp<>("k2", "v1", ScaledTime(2L)),
                    new KeyValueTimestamp<>("x", "x", ScaledTime(3L))
                )
            );
            bool rawRecords = WaitForAnyRecord(outputRaw);
            bool suppressedRecords = WaitForAnyRecord(outputSuppressed);
            Assert.Equal(rawRecords, Matchers.is(true));
            Assert.Equal(suppressedRecords, is(true));
        } finally {
            driver.close();
            cleanStateAfterTest(CLUSTER, driver);
        }
    }

    private static bool WaitForAnyRecord(string topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try { 
 (Consumer<object, object> consumer = new KafkaConsumer<>(properties));
            List<TopicPartition> partitions =
                consumer.partitionsFor(topic)
                        .stream()
                        .map(pi => new TopicPartition(pi.topic(), pi.partition()))
                        .collect(Collectors.toList());
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            long start = System.currentTimeMillis();
            while ((System.currentTimeMillis() - start) < DEFAULT_TIMEOUT) {
                ConsumerRecords<object, object> records = consumer.poll(ofMillis(500));

                if (!records.isEmpty()) {
                    return true;
                }
            }

            return false;
        }
    }

    [Xunit.Fact]
    public void ShouldShutdownWhenRecordConstraintIsViolated() {// throws InterruptedException
        string testId = "-shouldShutdownWhenRecordConstraintIsViolated";
        string appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
        string input = "input" + testId;
        string outputSuppressed = "output-suppressed" + testId;
        string outputRaw = "output-raw" + testId;

        cleanStateBeforeTest(CLUSTER, input, outputRaw, outputSuppressed);

        StreamsBuilder builder = new StreamsBuilder();
        KTable<string, long> valueCounts = BuildCountsTable(input, builder);

        valueCounts
            .suppress(untilTimeLimit(ofMillis(MAX_VALUE), maxRecords(1L).shutDownWhenFull()))
            .toStream()
            .to(outputSuppressed, Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .to(outputRaw, Produced.with(STRING_SERDE, Serdes.Long()));

        Properties streamsConfig = GetStreamsConfig(appId);
        KafkaStreams driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
        try {
            produceSynchronously(
                input,
                asList(
                    new KeyValueTimestamp<>("k1", "v1", ScaledTime(0L)),
                    new KeyValueTimestamp<>("k1", "v2", ScaledTime(1L)),
                    new KeyValueTimestamp<>("k2", "v1", ScaledTime(2L)),
                    new KeyValueTimestamp<>("x", "x", ScaledTime(3L))
                )
            );
            VerifyErrorShutdown(driver);
        } finally {
            driver.close();
            cleanStateAfterTest(CLUSTER, driver);
        }
    }

    [Xunit.Fact]
    public void ShouldShutdownWhenBytesConstraintIsViolated() {// throws InterruptedException
        string testId = "-shouldShutdownWhenBytesConstraintIsViolated";
        string appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
        string input = "input" + testId;
        string outputSuppressed = "output-suppressed" + testId;
        string outputRaw = "output-raw" + testId;

        cleanStateBeforeTest(CLUSTER, input, outputRaw, outputSuppressed);

        StreamsBuilder builder = new StreamsBuilder();
        KTable<string, long> valueCounts = BuildCountsTable(input, builder);

        valueCounts
            // this is a bit brittle, but I happen to know that the entries are a little over 100 bytes in size.
            .suppress(untilTimeLimit(ofMillis(MAX_VALUE), maxBytes(200L).shutDownWhenFull()))
            .toStream()
            .to(outputSuppressed, Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .to(outputRaw, Produced.with(STRING_SERDE, Serdes.Long()));

        Properties streamsConfig = GetStreamsConfig(appId);
        KafkaStreams driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
        try {
            produceSynchronously(
                input,
                asList(
                    new KeyValueTimestamp<>("k1", "v1", ScaledTime(0L)),
                    new KeyValueTimestamp<>("k1", "v2", ScaledTime(1L)),
                    new KeyValueTimestamp<>("k2", "v1", ScaledTime(2L)),
                    new KeyValueTimestamp<>("x", "x", ScaledTime(3L))
                )
            );
            VerifyErrorShutdown(driver);
        } finally {
            driver.close();
            cleanStateAfterTest(CLUSTER, driver);
        }
    }

    private static Properties GetStreamsConfig(string appId) {
        return mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(StreamsConfig.POLL_MS_CONFIG, int.toString(COMMIT_INTERVAL)),
            mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, int.toString(COMMIT_INTERVAL)),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, AT_LEAST_ONCE),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
        ));
    }

    /**
     * scaling to ensure that there are commits in between the various test events,
     * just to exercise that everything works properly in the presence of commits.
     */
    private static long ScaledTime(long unscaledTime) {
        return COMMIT_INTERVAL * 2 * unscaledTime;
    }

    private static void ProduceSynchronously(string topic, List<KeyValueTimestamp<string, string>> toProduce) {
        Properties producerConfig = mkProperties(mkMap(
            mkEntry(ProducerConfig.CLIENT_ID_CONFIG, "anything"),
            mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ((Serializer<string>) STRING_SERIALIZER).getClass().getName()),
            mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ((Serializer<string>) STRING_SERIALIZER).getClass().getName()),
            mkEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));
        IntegrationTestUtils.produceSynchronously(producerConfig, false, topic, Optional.empty(), toProduce);
    }

    private static void VerifyErrorShutdown(KafkaStreams driver) {// throws InterruptedException
        waitForCondition(() => !driver.state().isRunning(), DEFAULT_TIMEOUT, "Streams didn't shut down.");
        Assert.Equal(driver.state(), is(KafkaStreams.State.ERROR));
    }
}
