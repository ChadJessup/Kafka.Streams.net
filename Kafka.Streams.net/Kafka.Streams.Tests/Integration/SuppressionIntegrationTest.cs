//using Confluent.Kafka;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.Temporary;
//using Kafka.Streams.Threads.KafkaStreams;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Integration
//{
//    public class SuppressionIntegrationTest
//    {

//        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
//            1,
//            mkProperties(mkMap()),
//            0L
//        );
//        private static ISerializer<string> STRING_SERIALIZER = Serdes.String().Serializer;
//        private static ISerde<string> STRING_SERDE = Serdes.String();
//        private const int COMMIT_INTERVAL = 100;

//        private static IKTable<string, long> BuildCountsTable(string input, StreamsBuilder builder)
//        {
//            return builder
//                .Table(
//                    input,
//                    Consumed.With(STRING_SERDE, STRING_SERDE),
//                    Materialized.With<string, string, IKeyValueStore<Bytes, byte[]>>(STRING_SERDE, STRING_SERDE)
//                        .WithCachingDisabled()
//                        .WithLoggingDisabled()
//                )
//                .GroupBy((k, v) => KeyValuePair.Create(v, k), Grouped.With(STRING_SERDE, STRING_SERDE))
//                .Count(Materialized.As<string, long, IKeyValueStore<Bytes, byte[]>>("counts").WithCachingDisabled());
//        }

//        [Fact]
//        public void ShouldUseDefaultSerdes()
//        {
//            string testId = "-shouldInheritSerdes";
//            string appId = this.GetType().FullName.ToLower() + testId;
//            string input = "input" + testId;
//            string outputSuppressed = "output-suppressed" + testId;
//            string outputRaw = "output-raw" + testId;

//            cleanStateBeforeTest(CLUSTER, input, outputRaw, outputSuppressed);

//            StreamsBuilder builder = new StreamsBuilder();

//            IKStream<string, string> inputStream = builder.Stream(input);

//            IKTable<string, string> valueCounts = inputStream
//                .GroupByKey()
//                .Aggregate(() => "()", (key, value, aggregate) => aggregate + ",(" + key + ": " + value + ")");

//            valueCounts
//                .suppress(untilTimeLimit(TimeSpan.FromMilliseconds(MAX_VALUE), maxRecords(1L).emitEarlyWhenFull()))
//                .ToStream()
//                .To(outputSuppressed);

//            valueCounts
//                .ToStream()
//                .To(outputRaw);

//            StreamsConfig streamsConfig = GetStreamsConfig(appId);
//            streamsConfig.Put(StreamsConfig.DefaultKeySerdeClassConfig, Serdes.StringSerde);
//            streamsConfig.Put(StreamsConfig.DefaultValueSerdeClassConfig, Serdes.StringSerde);

//            IKafkaStreamsThread driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
//            try
//            {
//                produceSynchronously(
//                    input,
//                    Arrays.asList(
//                        new KeyValueTimestamp<string, string>("k1", "v1", ScaledTime(0L)),
//                        new KeyValueTimestamp<string, string>("k1", "v2", ScaledTime(1L)),
//                        new KeyValueTimestamp<string, string>("k2", "v1", ScaledTime(2L)),
//                        new KeyValueTimestamp<string, string>("x", "x", ScaledTime(3L))
//                    )
//                );
//                bool rawRecords = WaitForAnyRecord(outputRaw);
//                bool suppressedRecords = WaitForAnyRecord(outputSuppressed);
//                Assert.Equal(rawRecords, Matchers.Is(true));
//                Assert.Equal(true, suppressedRecords);
//            }
//            finally
//            {
//                driver.Close();
//                cleanStateAfterTest(CLUSTER, driver);
//            }
//        }

//        [Fact]
//        public void ShouldInheritSerdes()
//        {
//            string testId = "-shouldInheritSerdes";
//            string appId = this.GetType().FullName.ToLower() + testId;
//            string input = "input" + testId;
//            string outputSuppressed = "output-suppressed" + testId;
//            string outputRaw = "output-raw" + testId;

//            cleanStateBeforeTest(CLUSTER, input, outputRaw, outputSuppressed);

//            StreamsBuilder builder = new StreamsBuilder();

//            IKStream<K, V> inputStream = builder.Stream(input);

//            // count sets the serde to long
//            KTable<string, long> valueCounts = inputStream
//                .GroupByKey()
//                .Count();

//            valueCounts
//                .suppress(untilTimeLimit(TimeSpan.FromMilliseconds(MAX_VALUE), maxRecords(1L).emitEarlyWhenFull()))
//                .ToStream()
//                .To(outputSuppressed);

//            valueCounts
//                .ToStream()
//                .To(outputRaw);

//            StreamsConfig streamsConfig = GetStreamsConfig(appId);
//            streamsConfig.Put(StreamsConfig.DefaultKeySerdeClassConfig, Serdes.StringSerde);
//            streamsConfig.Put(StreamsConfig.DefaultValueSerdeClassConfig, Serdes.StringSerde);

//            KafkaStreamsThread driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
//            try
//            {
//                produceSynchronously(
//                    input,
//                    Arrays.asList(
//                        new KeyValueTimestamp<string, string>("k1", "v1", ScaledTime(0L)),
//                        new KeyValueTimestamp<string, string>("k1", "v2", ScaledTime(1L)),
//                        new KeyValueTimestamp<string, string>("k2", "v1", ScaledTime(2L)),
//                        new KeyValueTimestamp<string, string>("x", "x", ScaledTime(3L))
//                    )
//                );
//                bool rawRecords = WaitForAnyRecord(outputRaw);
//                bool suppressedRecords = WaitForAnyRecord(outputSuppressed);
//                Assert.Equal(rawRecords, Matchers());
//                Assert.Equal(true, suppressedRecords);
//            }
//            finally
//            {
//                driver.Close();
//                cleanStateAfterTest(CLUSTER, driver);
//            }
//        }

//        private static bool WaitForAnyRecord(string topic)
//        {
//            StreamsConfig properties = new StreamsConfig();
//            properties.Put(ConsumerConfig.BootstrapServersConfig, CLUSTER.bootstrapServers());
//            properties.Put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().Deserializer);
//            properties.Put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().Deserializer);
//            properties.Put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

//            IConsumer<object, object> consumer = new KafkaConsumer<>(properties);
//            List<TopicPartition> partitions =
//                consumer.partitionsFor(topic)
//                        .Stream()
//                        .Map(pi => new TopicPartition(pi.Topic, pi.Partition))
//                        .collect(Collectors.toList());
//            consumer.Assign(partitions);
//            consumer.seekToBeginning(partitions);
//            long start = System.currentTimeMillis();
//            while ((System.currentTimeMillis() - start) < DEFAULT_TIMEOUT)
//            {
//                ConsumeResult<object, object> records = consumer.poll(TimeSpan.FromMilliseconds(500));

//                if (!records.IsEmpty())
//                {
//                    return true;
//                }
//            }

//            return false;
//        }

//        [Fact]
//        public void ShouldShutdownWhenRecordConstraintIsViolated()
//        {// throws InterruptedException
//            string testId = "-shouldShutdownWhenRecordConstraintIsViolated";
//            string appId = this.GetType().FullName.ToLower() + testId;
//            string input = "input" + testId;
//            string outputSuppressed = "output-suppressed" + testId;
//            string outputRaw = "output-raw" + testId;

//            cleanStateBeforeTest(CLUSTER, input, outputRaw, outputSuppressed);

//            StreamsBuilder builder = new StreamsBuilder();
//            IKTable<string, long> valueCounts = BuildCountsTable(input, builder);

//            valueCounts
//                .suppress(untilTimeLimit(TimeSpan.FromMilliseconds(MAX_VALUE), maxRecords(1L).shutDownWhenFull()))
//                .ToStream()
//                .To(outputSuppressed, Produced.With(STRING_SERDE, Serdes.Long()));

//            valueCounts
//                .ToStream()
//                .To(outputRaw, Produced.With(STRING_SERDE, Serdes.Long()));

//            StreamsConfig streamsConfig = GetStreamsConfig(appId);
//            KafkaStreamsThread driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
//            try
//            {
//                produceSynchronously(
//                    input,
//                    Arrays.asList(
//                        new KeyValueTimestamp<string, string>("k1", "v1", ScaledTime(0L)),
//                        new KeyValueTimestamp<string, string>("k1", "v2", ScaledTime(1L)),
//                        new KeyValueTimestamp<string, string>("k2", "v1", ScaledTime(2L)),
//                        new KeyValueTimestamp<string, string>("x", "x", ScaledTime(3L))
//                    )
//                );
//                VerifyErrorShutdown(driver);
//            }
//            finally
//            {
//                driver.Close();
//                cleanStateAfterTest(CLUSTER, driver);
//            }
//        }

//        [Fact]
//        public void ShouldShutdownWhenBytesConstraintIsViolated()
//        {// throws InterruptedException
//            string testId = "-shouldShutdownWhenBytesConstraintIsViolated";
//            string appId = this.GetType().FullName.ToLower() + testId;
//            string input = "input" + testId;
//            string outputSuppressed = "output-suppressed" + testId;
//            string outputRaw = "output-raw" + testId;

//            cleanStateBeforeTest(CLUSTER, input, outputRaw, outputSuppressed);

//            StreamsBuilder builder = new StreamsBuilder();
//            IKTable<string, long> valueCounts = BuildCountsTable(input, builder);

//            valueCounts
//                // this is a bit brittle, but I happen to know that the entries are a little over 100 bytes in size.
//                .suppress(untilTimeLimit(TimeSpan.FromMilliseconds(MAX_VALUE), maxBytes(200L).shutDownWhenFull()))
//                .ToStream()
//                .To(outputSuppressed, Produced.With(STRING_SERDE, Serdes.Long()));

//            valueCounts
//                .ToStream()
//                .To(outputRaw, Produced.With(STRING_SERDE, Serdes.Long()));

//            StreamsConfig streamsConfig = GetStreamsConfig(appId);
//            KafkaStreamsThread driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
//            try
//            {
//                produceSynchronously(
//                    input,
//                    Arrays.asList(
//                        new KeyValueTimestamp<string, string>("k1", "v1", ScaledTime(0L)),
//                        new KeyValueTimestamp<string, string>("k1", "v2", ScaledTime(1L)),
//                        new KeyValueTimestamp<string, string>("k2", "v1", ScaledTime(2L)),
//                        new KeyValueTimestamp<string, string>("x", "x", ScaledTime(3L))
//                    )
//                );
//                VerifyErrorShutdown(driver);
//            }
//            finally
//            {
//                driver.Close();
//                cleanStateAfterTest(CLUSTER, driver);
//            }
//        }

//        private static StreamsConfig GetStreamsConfig(string appId)
//        {
//            return mkProperties(mkMap(
//                mkEntry(StreamsConfig.ApplicationIdConfig, appId),
//                mkEntry(StreamsConfig.BootstrapServersConfig, CLUSTER.bootstrapServers()),
//                mkEntry(StreamsConfig.PollMsConfig, int.ToString(COMMIT_INTERVAL)),
//                mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, int.ToString(COMMIT_INTERVAL)),
//                mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, AT_LEAST_ONCE),
//                mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath())
//            ));
//        }

//        /**
//         * scaling to ensure that there are commits in between the various test events,
//         * just to exercise that everything works properly in the presence of commits.
//         */
//        private static long ScaledTime(long unscaledTime)
//        {
//            return COMMIT_INTERVAL * 2 * unscaledTime;
//        }

//        private static void ProduceSynchronously(string topic, List<KeyValueTimestamp<string, string>> toProduce)
//        {
//            StreamsConfig ProducerConfig = mkProperties(mkMap(
//                mkEntry(ProducerConfig.CLIENT_ID_CONFIG, "anything"),
//                mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ((Serializer<string>)STRING_SERIALIZER).GetType().FullName),
//                mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIGConfig, ((Serializer<string>)STRING_SERIALIZER).GetType().FullName),
//                mkEntry(ProducerConfig.BootstrapServersConfig, CLUSTER.bootstrapServers())
//            ));
//            IntegrationTestUtils.produceSynchronously(ProducerConfig, false, topic, Optional.empty(), toProduce);
//        }

//        private static void VerifyErrorShutdown(KafkaStreamsThread driver)
//        {// throws InterruptedException
//            WaitForCondition(() => !driver.state().isRunning(), DEFAULT_TIMEOUT, "Streams didn't shut down.");
//            Assert.Equal(driver.state(), KafkaStreamsThreadStates.ERROR);
//        }
//    }
//}
