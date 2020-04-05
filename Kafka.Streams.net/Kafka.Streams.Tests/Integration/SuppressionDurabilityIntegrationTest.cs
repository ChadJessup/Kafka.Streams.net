//namespace Kafka.Streams.Tests.Integration
//{
//    /*






//    *

//    *





//    */




































































//    public class SuppressionDurabilityIntegrationTest
//    {

//        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
//            3,
//            mkProperties(mkMap()),
//            0L
//        );
//        private static Serdes.String().Deserializer STRING_DESERIALIZER = new Serdes.String().Deserializer();
//        private static Serdes.String().Serializer STRING_SERIALIZER = new Serdes.String().Serializer();
//        private static Serde<string> STRING_SERDE = Serdes.String();
//        private static LongDeserializer LONG_DESERIALIZER = new LongDeserializer();
//        private const int COMMIT_INTERVAL = 100;
//        private readonly bool eosEnabled;

//        //@Parameters(name = "{index}: eosEnabled={0}")
//        public static Collection<object[]> Parameters()
//        {
//            return asList(
//                new object[] { false },
//                new object[] { true }
//            );
//        }

//        public SuppressionDurabilityIntegrationTest(bool eosEnabled)
//        {
//            this.eosEnabled = eosEnabled;
//        }

//        [Xunit.Fact]
//        public void ShouldRecoverBufferAfterShutdown()
//        {
//            string testId = "-shouldRecoverBufferAfterShutdown";
//            string appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
//            string input = "input" + testId;
//            string storeName = "counts";
//            string outputSuppressed = "output-suppressed" + testId;
//            string outputRaw = "output-raw" + testId;

//            // Create multiple partitions as a trap, in case the buffer doesn't properly set the
//            // partition on the records, but instead relies on the default key partitioner
//            cleanStateBeforeTest(CLUSTER, 2, input, outputRaw, outputSuppressed);

//            StreamsBuilder builder = new StreamsBuilder();
//            KTable<string, long> valueCounts = builder
//                .Stream(
//                    input,
//                    Consumed.With(STRING_SERDE, STRING_SERDE))
//                .groupByKey()
//                .count(Materialized<string, long, IKeyValueStore<Bytes, byte[]>>.As(storeName).withCachingDisabled());

//            KStream<string, long> suppressedCounts = valueCounts
//                .suppress(untilTimeLimit(FromMilliseconds(MAX_VALUE), maxRecords(3L).emitEarlyWhenFull()))
//                .toStream();

//            AtomicInteger eventCount = new AtomicInteger(0);
//            suppressedCounts.ForEach((key, value) => eventCount.incrementAndGet());

//            // expect all post-suppress records to keep the right input topic
//            MetadataValidator metadataValidator = new MetadataValidator(input);

//            suppressedCounts
//                .transform(metadataValidator)
//                .To(outputSuppressed, Produced.With(STRING_SERDE, Serdes.Long()));

//            valueCounts
//                .toStream()
//                .transform(metadataValidator)
//                .To(outputRaw, Produced.With(STRING_SERDE, Serdes.Long()));

//            StreamsConfig streamsConfig = mkProperties(mkMap(
//                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
//                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
//                mkEntry(StreamsConfig.POLL_MS_CONFIG, int.toString(COMMIT_INTERVAL)),
//                mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, int.toString(COMMIT_INTERVAL)),
//                mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, eosEnabled ? EXACTLY_ONCE : AT_LEAST_ONCE),
//                mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath())
//            ));

//            KafkaStreams driver = getStartedStreams(streamsConfig, builder, true);
//            try
//            {
//                // start by putting some stuff in the buffer
//                // note, we send all input records to partition 0
//                // to make sure that supppress doesn't erroneously send records to other partitions.
//                produceSynchronouslyToPartitionZero(
//                    input,
//                    asList(
//                        new KeyValueTimestamp<>("k1", "v1", scaledTime(1L)),
//                        new KeyValueTimestamp<>("k2", "v2", scaledTime(2L)),
//                        new KeyValueTimestamp<>("k3", "v3", scaledTime(3L))
//                    )
//                );
//                verifyOutput(
//                    outputRaw,
//                    new HashSet<>(asList(
//                        new KeyValueTimestamp<>("k1", 1L, scaledTime(1L)),
//                        new KeyValueTimestamp<>("k2", 1L, scaledTime(2L)),
//                        new KeyValueTimestamp<>("k3", 1L, scaledTime(3L))
//                    ))
//                );
//                Assert.Equal(eventCount.Get(), (0));

//                // flush two of the first three events System.Console.Out.
//                produceSynchronouslyToPartitionZero(
//                    input,
//                    asList(
//                        new KeyValueTimestamp<>("k4", "v4", scaledTime(4L)),
//                        new KeyValueTimestamp<>("k5", "v5", scaledTime(5L))
//                    )
//                );
//                verifyOutput(
//                    outputRaw,
//                    new HashSet<>(asList(
//                        new KeyValueTimestamp<>("k4", 1L, scaledTime(4L)),
//                        new KeyValueTimestamp<>("k5", 1L, scaledTime(5L))
//                    ))
//                );
//                Assert.Equal(eventCount.Get(), (2));
//                verifyOutput(
//                    outputSuppressed,
//                    asList(
//                        new KeyValueTimestamp<>("k1", 1L, scaledTime(1L)),
//                        new KeyValueTimestamp<>("k2", 1L, scaledTime(2L))
//                    )
//                );

//                // bounce to ensure that the history, including retractions,
//                // get restored properly. (i.e., we shouldn't see those first events again)

//                // restart the driver
//                driver.close();
//                Assert.Equal(driver.state(), (KafkaStreams.State.NOT_RUNNING));
//                driver = getStartedStreams(streamsConfig, builder, false);


//                // flush those recovered buffered events System.Console.Out.
//                produceSynchronouslyToPartitionZero(
//                    input,
//                    asList(
//                        new KeyValueTimestamp<>("k6", "v6", scaledTime(6L)),
//                        new KeyValueTimestamp<>("k7", "v7", scaledTime(7L)),
//                        new KeyValueTimestamp<>("k8", "v8", scaledTime(8L))
//                    )
//                );
//                verifyOutput(
//                    outputRaw,
//                    new HashSet<>(asList(
//                        new KeyValueTimestamp<>("k6", 1L, scaledTime(6L)),
//                        new KeyValueTimestamp<>("k7", 1L, scaledTime(7L)),
//                        new KeyValueTimestamp<>("k8", 1L, scaledTime(8L))
//                    ))
//                );
//                Assert.Equal("suppress has apparently produced some duplicates. There should only be 5 output events.",
//                           eventCount.Get(), (5));

//                verifyOutput(
//                    outputSuppressed,
//                    asList(
//                        new KeyValueTimestamp<>("k3", 1L, scaledTime(3L)),
//                        new KeyValueTimestamp<>("k4", 1L, scaledTime(4L)),
//                        new KeyValueTimestamp<>("k5", 1L, scaledTime(5L))
//                    )
//                );

//                metadataValidator.raiseExceptionIfAny();

//            }
//            finally
//            {
//                driver.close();
//                cleanStateAfterTest(CLUSTER, driver);
//            }
//        }

//        private static class MetadataValidator : TransformerSupplier<string, long, KeyValuePair<string, long>>
//        {
//            private static Logger LOG = LoggerFactory.getLogger(MetadataValidator);
//            private AtomicReference<Throwable> firstException = new AtomicReference<>();
//            private readonly string topic;

//            public MetadataValidator(string topic)
//            {
//                this.topic = topic;
//            }


//            public Transformer<string, long, KeyValuePair<string, long>> Get()
//            {
//                return new Transformer<string, long, KeyValuePair<string, long>>()
//                {
//                private ProcessorContext context;


//            public void Init(ProcessorContext context)
//            {
//                this.context = context;
//            }


//            public KeyValuePair<string, long> Transform(string key, long value)
//            {
//                try
//                {
//                    Assert.Equal(context.Topic, (topic));
//                }
//                catch (Throwable e)
//                {
//                    firstException.compareAndSet(null, e);
//                    LOG.error("Validation Failed", e);
//                }
//                return KeyValuePair.Create(key, value);
//            }


//            public void Close()
//            {

//            }
//        };
//    }

//    void RaiseExceptionIfAny()
//    {
//        Throwable exception = firstException.Get();
//        if (exception != null)
//        {
//            throw new AssertionError("Got an exception during run", exception);
//        }
//    }
//    }

//    private void VerifyOutput(string topic, List<KeyValueTimestamp<string, long>> keyValueTimestamps)
//    {
//        StreamsConfig properties = mkProperties(
//            mkMap(
//                mkEntry(ConsumerConfig.GROUP_ID_CONFIG, "test-group"),
//                mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
//                mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ((Deserializer<string>)STRING_DESERIALIZER).GetType().FullName),
//                mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ((Deserializer<long>)LONG_DESERIALIZER).GetType().FullName)
//            )
//        );
//        IntegrationTestUtils.verifyKeyValueTimestamps(properties, topic, keyValueTimestamps);
//    }

//    private void VerifyOutput(string topic, HashSet<KeyValueTimestamp<string, long>> keyValueTimestamps)
//    {
//        StreamsConfig properties = mkProperties(
//            mkMap(
//                mkEntry(ConsumerConfig.GROUP_ID_CONFIG, "test-group"),
//                mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
//                mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ((Deserializer<string>)STRING_DESERIALIZER).GetType().FullName),
//                mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ((Deserializer<long>)LONG_DESERIALIZER).GetType().FullName)
//            )
//        );
//        IntegrationTestUtils.verifyKeyValueTimestamps(properties, topic, keyValueTimestamps);
//    }

//    /**
//     * scaling to ensure that there are commits in between the various test events,
//     * just to exercise that everything works properly in the presence of commits.
//     */
//    private long ScaledTime(long unscaledTime)
//    {
//        return COMMIT_INTERVAL * 2 * unscaledTime;
//    }

//    private static void ProduceSynchronouslyToPartitionZero(string topic, List<KeyValueTimestamp<string, string>> toProduce)
//    {
//        StreamsConfig producerConfig = mkProperties(mkMap(
//            mkEntry(ProducerConfig.CLIENT_ID_CONFIG, "anything"),
//            mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ((Serializer<string>)STRING_SERIALIZER).GetType().FullName),
//            mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ((Serializer<string>)STRING_SERIALIZER).GetType().FullName),
//            mkEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
//        ));
//        IntegrationTestUtils.produceSynchronously(producerConfig, false, topic, Optional.of(0), toProduce);
//    }
//}}
///*






//*

//*





//*/








































































///**
// * scaling to ensure that there are commits in between the various test events,
// * just to exercise that everything works properly in the presence of commits.
// */

