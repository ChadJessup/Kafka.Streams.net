namespace Kafka.Streams.Tests.Integration
{
    public class SuppressionIntegrationTest
    {

        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
            1,
            mkProperties(mkMap()),
            0L
        );
        private static Serdes.String().Serializer STRING_SERIALIZER = new Serdes.String().Serializer();
        private static Serde<string> STRING_SERDE = Serdes.String();
        private const int COMMIT_INTERVAL = 100;

        private static KTable<string, long> BuildCountsTable(string input, StreamsBuilder builder)
        {
            return builder
                .table(
                    input,
                    Consumed.With(STRING_SERDE, STRING_SERDE),
                    Materialized.< string, string, IKeyValueStore<Bytes, byte[]> > with(STRING_SERDE, STRING_SERDE)
                        .withCachingDisabled()
                        .withLoggingDisabled()
                )
                .GroupBy((k, v) => KeyValuePair.Create(v, k), Grouped.With(STRING_SERDE, STRING_SERDE))
                .Count(Materialized<string, long, IKeyValueStore<Bytes, byte[]>>.As("counts").withCachingDisabled());
        }

        [Fact]
        public void ShouldUseDefaultSerdes()
        {
            string testId = "-shouldInheritSerdes";
            string appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
            string input = "input" + testId;
            string outputSuppressed = "output-suppressed" + testId;
            string outputRaw = "output-raw" + testId;

            cleanStateBeforeTest(CLUSTER, input, outputRaw, outputSuppressed);

            StreamsBuilder builder = new StreamsBuilder();

            IKStream<K, V> inputStream = builder.Stream(input);

            KTable<string, string> valueCounts = inputStream
                .GroupByKey()
                .Aggregate(() => "()", (key, value, aggregate) => aggregate + ",(" + key + ": " + value + ")");

            valueCounts
                .suppress(untilTimeLimit(TimeSpan.FromMilliseconds(MAX_VALUE), maxRecords(1L).emitEarlyWhenFull()))
                .ToStream()
                .To(outputSuppressed);

            valueCounts
                .ToStream()
                .To(outputRaw);

            StreamsConfig streamsConfig = GetStreamsConfig(appId);
            streamsConfig.Put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde);
            streamsConfig.Put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde);

            KafkaStreams driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
            try
            {
                produceSynchronously(
                    input,
                    Arrays.asList(
                        new KeyValueTimestamp<>("k1", "v1", ScaledTime(0L)),
                        new KeyValueTimestamp<>("k1", "v2", ScaledTime(1L)),
                        new KeyValueTimestamp<>("k2", "v1", ScaledTime(2L)),
                        new KeyValueTimestamp<>("x", "x", ScaledTime(3L))
                    )
                );
                bool rawRecords = WaitForAnyRecord(outputRaw);
                bool suppressedRecords = WaitForAnyRecord(outputSuppressed);
                Assert.Equal(rawRecords, Matchers.Is(true));
                Assert.Equal(suppressedRecords, true);
            }
            finally
            {
                driver.Close();
                cleanStateAfterTest(CLUSTER, driver);
            }
        }

        [Fact]
        public void ShouldInheritSerdes()
        {
            string testId = "-shouldInheritSerdes";
            string appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
            string input = "input" + testId;
            string outputSuppressed = "output-suppressed" + testId;
            string outputRaw = "output-raw" + testId;

            cleanStateBeforeTest(CLUSTER, input, outputRaw, outputSuppressed);

            StreamsBuilder builder = new StreamsBuilder();

            IKStream<K, V> inputStream = builder.Stream(input);

            // count sets the serde to long
            KTable<string, long> valueCounts = inputStream
                .GroupByKey()
                .Count();

            valueCounts
                .suppress(untilTimeLimit(TimeSpan.FromMilliseconds(MAX_VALUE), maxRecords(1L).emitEarlyWhenFull()))
                .ToStream()
                .To(outputSuppressed);

            valueCounts
                .ToStream()
                .To(outputRaw);

            StreamsConfig streamsConfig = GetStreamsConfig(appId);
            streamsConfig.Put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde);
            streamsConfig.Put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde);

            KafkaStreams driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
            try
            {
                produceSynchronously(
                    input,
                    Arrays.asList(
                        new KeyValueTimestamp<>("k1", "v1", ScaledTime(0L)),
                        new KeyValueTimestamp<>("k1", "v2", ScaledTime(1L)),
                        new KeyValueTimestamp<>("k2", "v1", ScaledTime(2L)),
                        new KeyValueTimestamp<>("x", "x", ScaledTime(3L))
                    )
                );
                bool rawRecords = WaitForAnyRecord(outputRaw);
                bool suppressedRecords = WaitForAnyRecord(outputSuppressed);
                Assert.Equal(rawRecords, Matchers.(true));
                Assert.Equal(suppressedRecords, true);
            }
            finally
            {
                driver.Close();
                cleanStateAfterTest(CLUSTER, driver);
            }
        }

        private static bool WaitForAnyRecord(string topic)
        {
            StreamsConfig properties = new StreamsConfig();
            properties.Put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
            properties.Put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().Deserializer);
            properties.Put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().Deserializer);
            properties.Put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

            IConsumer<object, object> consumer = new KafkaConsumer<>(properties);
            List<TopicPartition> partitions =
                consumer.partitionsFor(topic)
                        .Stream()
                        .map(pi => new TopicPartition(pi.Topic, pi.Partition))
                        .collect(Collectors.toList());
            consumer.Assign(partitions);
            consumer.seekToBeginning(partitions);
            long start = System.currentTimeMillis();
            while ((System.currentTimeMillis() - start) < DEFAULT_TIMEOUT)
            {
                ConsumeResult<object, object> records = consumer.poll(TimeSpan.FromMilliseconds(500));

                if (!records.IsEmpty())
                {
                    return true;
                }
            }

            return false;
        }

        [Fact]
        public void ShouldShutdownWhenRecordConstraintIsViolated()
        {// throws InterruptedException
            string testId = "-shouldShutdownWhenRecordConstraintIsViolated";
            string appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
            string input = "input" + testId;
            string outputSuppressed = "output-suppressed" + testId;
            string outputRaw = "output-raw" + testId;

            cleanStateBeforeTest(CLUSTER, input, outputRaw, outputSuppressed);

            StreamsBuilder builder = new StreamsBuilder();
            KTable<string, long> valueCounts = BuildCountsTable(input, builder);

            valueCounts
                .suppress(untilTimeLimit(TimeSpan.FromMilliseconds(MAX_VALUE), maxRecords(1L).shutDownWhenFull()))
                .ToStream()
                .To(outputSuppressed, Produced.With(STRING_SERDE, Serdes.Long()));

            valueCounts
                .ToStream()
                .To(outputRaw, Produced.With(STRING_SERDE, Serdes.Long()));

            StreamsConfig streamsConfig = GetStreamsConfig(appId);
            KafkaStreams driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
            try
            {
                produceSynchronously(
                    input,
                    Arrays.asList(
                        new KeyValueTimestamp<>("k1", "v1", ScaledTime(0L)),
                        new KeyValueTimestamp<>("k1", "v2", ScaledTime(1L)),
                        new KeyValueTimestamp<>("k2", "v1", ScaledTime(2L)),
                        new KeyValueTimestamp<>("x", "x", ScaledTime(3L))
                    )
                );
                VerifyErrorShutdown(driver);
            }
            finally
            {
                driver.Close();
                cleanStateAfterTest(CLUSTER, driver);
            }
        }

        [Fact]
        public void ShouldShutdownWhenBytesConstraintIsViolated()
        {// throws InterruptedException
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
                .suppress(untilTimeLimit(TimeSpan.FromMilliseconds(MAX_VALUE), maxBytes(200L).shutDownWhenFull()))
                .ToStream()
                .To(outputSuppressed, Produced.With(STRING_SERDE, Serdes.Long()));

            valueCounts
                .ToStream()
                .To(outputRaw, Produced.With(STRING_SERDE, Serdes.Long()));

            StreamsConfig streamsConfig = GetStreamsConfig(appId);
            KafkaStreams driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
            try
            {
                produceSynchronously(
                    input,
                    Arrays.asList(
                        new KeyValueTimestamp<>("k1", "v1", ScaledTime(0L)),
                        new KeyValueTimestamp<>("k1", "v2", ScaledTime(1L)),
                        new KeyValueTimestamp<>("k2", "v1", ScaledTime(2L)),
                        new KeyValueTimestamp<>("x", "x", ScaledTime(3L))
                    )
                );
                VerifyErrorShutdown(driver);
            }
            finally
            {
                driver.Close();
                cleanStateAfterTest(CLUSTER, driver);
            }
        }

        private static StreamsConfig GetStreamsConfig(string appId)
        {
            return mkProperties(mkMap(
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(StreamsConfig.POLL_MS_CONFIG, int.ToString(COMMIT_INTERVAL)),
                mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, int.ToString(COMMIT_INTERVAL)),
                mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, AT_LEAST_ONCE),
                mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath())
            ));
        }

        /**
         * scaling to ensure that there are commits in between the various test events,
         * just to exercise that everything works properly in the presence of commits.
         */
        private static long ScaledTime(long unscaledTime)
        {
            return COMMIT_INTERVAL * 2 * unscaledTime;
        }

        private static void ProduceSynchronously(string topic, List<KeyValueTimestamp<string, string>> toProduce)
        {
            StreamsConfig producerConfig = mkProperties(mkMap(
                mkEntry(ProducerConfig.CLIENT_ID_CONFIG, "anything"),
                mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ((Serializer<string>)STRING_SERIALIZER).GetType().FullName),
                mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ((Serializer<string>)STRING_SERIALIZER).GetType().FullName),
                mkEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
            ));
            IntegrationTestUtils.produceSynchronously(producerConfig, false, topic, Optional.empty(), toProduce);
        }

        private static void VerifyErrorShutdown(KafkaStreams driver)
        {// throws InterruptedException
            WaitForCondition(() => !driver.state().isRunning(), DEFAULT_TIMEOUT, "Streams didn't shut down.");
            Assert.Equal(driver.state(), KafkaStreams.State.ERROR);
        }
    }
}
/*






*

*





*/



























































