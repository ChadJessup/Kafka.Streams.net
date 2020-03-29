/*






 *

 *





 */




































































public class SuppressionDurabilityIntegrationTest {
    
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
        3,
        mkProperties(mkMap()),
        0L
    );
    private static StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
    private static StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static Serde<string> STRING_SERDE = Serdes.String();
    private static LongDeserializer LONG_DESERIALIZER = new LongDeserializer();
    private static int COMMIT_INTERVAL = 100;
    private bool eosEnabled;

    @Parameters(name = "{index}: eosEnabled={0}")
    public static Collection<object[]> parameters() {
        return asList(
            new object[] {false},
            new object[] {true}
        );
    }

    public SuppressionDurabilityIntegrationTest(bool eosEnabled) {
        this.eosEnabled = eosEnabled;
    }

    [Xunit.Fact]
    public void shouldRecoverBufferAfterShutdown() {
        string testId = "-shouldRecoverBufferAfterShutdown";
        string appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
        string input = "input" + testId;
        string storeName = "counts";
        string outputSuppressed = "output-suppressed" + testId;
        string outputRaw = "output-raw" + testId;

        // create multiple partitions as a trap, in case the buffer doesn't properly set the
        // partition on the records, but instead relies on the default key partitioner
        cleanStateBeforeTest(CLUSTER, 2, input, outputRaw, outputSuppressed);

        StreamsBuilder builder = new StreamsBuilder();
        KTable<string, long> valueCounts = builder
            .stream(
                input,
                Consumed.with(STRING_SERDE, STRING_SERDE))
            .groupByKey()
            .count(Materialized<string, long, KeyValueStore<Bytes, byte[]>>.As(storeName).withCachingDisabled());

        KStream<string, long> suppressedCounts = valueCounts
            .suppress(untilTimeLimit(ofMillis(MAX_VALUE), maxRecords(3L).emitEarlyWhenFull()))
            .toStream();

        AtomicInteger eventCount = new AtomicInteger(0);
        suppressedCounts.foreach((key, value) => eventCount.incrementAndGet());

        // expect all post-suppress records to keep the right input topic
        MetadataValidator metadataValidator = new MetadataValidator(input);

        suppressedCounts
            .transform(metadataValidator)
            .to(outputSuppressed, Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .transform(metadataValidator)
            .to(outputRaw, Produced.with(STRING_SERDE, Serdes.Long()));

        Properties streamsConfig = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(StreamsConfig.POLL_MS_CONFIG, int.toString(COMMIT_INTERVAL)),
            mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, int.toString(COMMIT_INTERVAL)),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, eosEnabled ? EXACTLY_ONCE : AT_LEAST_ONCE),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
        ));

        KafkaStreams driver = getStartedStreams(streamsConfig, builder, true);
        try {
            // start by putting some stuff in the buffer
            // note, we send all input records to partition 0
            // to make sure that supppress doesn't erroneously send records to other partitions.
            produceSynchronouslyToPartitionZero(
                input,
                asList(
                    new KeyValueTimestamp<>("k1", "v1", scaledTime(1L)),
                    new KeyValueTimestamp<>("k2", "v2", scaledTime(2L)),
                    new KeyValueTimestamp<>("k3", "v3", scaledTime(3L))
                )
            );
            verifyOutput(
                outputRaw,
                new HashSet<>(asList(
                    new KeyValueTimestamp<>("k1", 1L, scaledTime(1L)),
                    new KeyValueTimestamp<>("k2", 1L, scaledTime(2L)),
                    new KeyValueTimestamp<>("k3", 1L, scaledTime(3L))
                ))
            );
            Assert.Equal(eventCount.get(), is(0));

            // flush two of the first three events System.Console.Out.
            produceSynchronouslyToPartitionZero(
                input,
                asList(
                    new KeyValueTimestamp<>("k4", "v4", scaledTime(4L)),
                    new KeyValueTimestamp<>("k5", "v5", scaledTime(5L))
                )
            );
            verifyOutput(
                outputRaw,
                new HashSet<>(asList(
                    new KeyValueTimestamp<>("k4", 1L, scaledTime(4L)),
                    new KeyValueTimestamp<>("k5", 1L, scaledTime(5L))
                ))
            );
            Assert.Equal(eventCount.get(), is(2));
            verifyOutput(
                outputSuppressed,
                asList(
                    new KeyValueTimestamp<>("k1", 1L, scaledTime(1L)),
                    new KeyValueTimestamp<>("k2", 1L, scaledTime(2L))
                )
            );

            // bounce to ensure that the history, including retractions,
            // get restored properly. (i.e., we shouldn't see those first events again)

            // restart the driver
            driver.close();
            Assert.Equal(driver.state(), is(KafkaStreams.State.NOT_RUNNING));
            driver = getStartedStreams(streamsConfig, builder, false);


            // flush those recovered buffered events System.Console.Out.
            produceSynchronouslyToPartitionZero(
                input,
                asList(
                    new KeyValueTimestamp<>("k6", "v6", scaledTime(6L)),
                    new KeyValueTimestamp<>("k7", "v7", scaledTime(7L)),
                    new KeyValueTimestamp<>("k8", "v8", scaledTime(8L))
                )
            );
            verifyOutput(
                outputRaw,
                new HashSet<>(asList(
                    new KeyValueTimestamp<>("k6", 1L, scaledTime(6L)),
                    new KeyValueTimestamp<>("k7", 1L, scaledTime(7L)),
                    new KeyValueTimestamp<>("k8", 1L, scaledTime(8L))
                ))
            );
            Assert.Equal("suppress has apparently produced some duplicates. There should only be 5 output events.",
                       eventCount.get(), is(5));

            verifyOutput(
                outputSuppressed,
                asList(
                    new KeyValueTimestamp<>("k3", 1L, scaledTime(3L)),
                    new KeyValueTimestamp<>("k4", 1L, scaledTime(4L)),
                    new KeyValueTimestamp<>("k5", 1L, scaledTime(5L))
                )
            );

            metadataValidator.raiseExceptionIfAny();

        } finally {
            driver.close();
            cleanStateAfterTest(CLUSTER, driver);
        }
    }

    private static class MetadataValidator : TransformerSupplier<string, long, KeyValuePair<string, long>> {
        private static Logger LOG = LoggerFactory.getLogger(MetadataValidator);
        private AtomicReference<Throwable> firstException = new AtomicReference<>();
        private string topic;

        public MetadataValidator(string topic) {
            this.topic = topic;
        }

        
        public Transformer<string, long, KeyValuePair<string, long>> get() {
            return new Transformer<string, long, KeyValuePair<string, long>>() {
                private ProcessorContext context;

                
                public void init(ProcessorContext context) {
                    this.context = context;
                }

                
                public KeyValuePair<string, long> transform(string key, long value) {
                    try {
                        Assert.Equal(context.topic(), (topic));
                    } catch (Throwable e) {
                        firstException.compareAndSet(null, e);
                        LOG.error("Validation Failed", e);
                    }
                    return new KeyValuePair<>(key, value);
                }

                
                public void close() {

                }
            };
        }

        void raiseExceptionIfAny() {
            Throwable exception = firstException.get();
            if (exception != null) {
                throw new AssertionError("Got an exception during run", exception);
            }
        }
    }

    private void verifyOutput(string topic, List<KeyValueTimestamp<string, long>> keyValueTimestamps) {
        Properties properties = mkProperties(
            mkMap(
                mkEntry(ConsumerConfig.GROUP_ID_CONFIG, "test-group"),
                mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ((Deserializer<string>) STRING_DESERIALIZER).getClass().getName()),
                mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ((Deserializer<long>) LONG_DESERIALIZER).getClass().getName())
            )
        );
        IntegrationTestUtils.verifyKeyValueTimestamps(properties, topic, keyValueTimestamps);
    }

    private void verifyOutput(string topic, HashSet<KeyValueTimestamp<string, long>> keyValueTimestamps) {
        Properties properties = mkProperties(
            mkMap(
                mkEntry(ConsumerConfig.GROUP_ID_CONFIG, "test-group"),
                mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ((Deserializer<string>) STRING_DESERIALIZER).getClass().getName()),
                mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ((Deserializer<long>) LONG_DESERIALIZER).getClass().getName())
            )
        );
        IntegrationTestUtils.verifyKeyValueTimestamps(properties, topic, keyValueTimestamps);
    }

    /**
     * scaling to ensure that there are commits in between the various test events,
     * just to exercise that everything works properly in the presence of commits.
     */
    private long scaledTime(long unscaledTime) {
        return COMMIT_INTERVAL * 2 * unscaledTime;
    }

    private static void produceSynchronouslyToPartitionZero(string topic, List<KeyValueTimestamp<string, string>> toProduce) {
        Properties producerConfig = mkProperties(mkMap(
            mkEntry(ProducerConfig.CLIENT_ID_CONFIG, "anything"),
            mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ((Serializer<string>) STRING_SERIALIZER).getClass().getName()),
            mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ((Serializer<string>) STRING_SERIALIZER).getClass().getName()),
            mkEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));
        IntegrationTestUtils.produceSynchronously(producerConfig, false, topic, Optional.of(0), toProduce);
    }
}