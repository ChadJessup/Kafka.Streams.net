using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.Temporary;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Threads.KafkaStreams;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Xunit;

namespace Kafka.Streams.Tests.Integration
{
    public class SuppressionDurabilityIntegrationTest
    {

        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
            3,
            0L);

        private static IDeserializer<string> STRING_DESERIALIZER = Serdes.String().Deserializer;
        private static ISerializer<string> STRING_SERIALIZER = Serdes.String().Serializer;
        private static ISerde<string> STRING_SERDE = Serdes.String();
        private static IDeserializer<long> LONG_DESERIALIZER = Serdes.Long().Deserializer;
        private const int COMMIT_INTERVAL = 100;
        private readonly bool eosEnabled;

        //@Parameters(Name = "{index}: eosEnabled={0}")
        public static List<object[]> Parameters()
        {
            return Arrays.asList(
                new object[] { false },
                new object[] { true });
        }

        public SuppressionDurabilityIntegrationTest(bool eosEnabled)
        {
            this.eosEnabled = eosEnabled;
        }

        [Fact]
        public void ShouldRecoverBufferAfterShutdown()
        {
            string testId = "-shouldRecoverBufferAfterShutdown";
            string appId = this.GetType().FullName.ToLower() + testId;
            string input = "input" + testId;
            string storeName = "counts";
            string outputSuppressed = "output-suppressed" + testId;
            string outputRaw = "output-raw" + testId;

            // Create multiple partitions as a trap, in case the buffer doesn't properly set the
            // partition on the records, but instead relies on the default key partitioner
            CleanStateBeforeTest(CLUSTER, 2, input, outputRaw, outputSuppressed);

            StreamsBuilder builder = new StreamsBuilder();
            IKTable<string, long> valueCounts = builder
                .Stream(input, Consumed.With(STRING_SERDE, STRING_SERDE))
                .GroupByKey()
                .Count(Materialized.As<string, long, IKeyValueStore<Bytes, byte[]>>(storeName).WithCachingDisabled());

            IKStream<string, long> suppressedCounts = valueCounts
                .Suppress(UntilTimeLimit(TimeSpan.FromMilliseconds(MAX_VALUE), maxRecords(3L).emitEarlyWhenFull()))
                .ToStream();

            int eventCount = 0;
            suppressedCounts.ForEach((key, value) => eventCount++);

            // expect All post-suppress records to keep the right input topic
            MetadataValidator metadataValidator = new MetadataValidator(input);

            suppressedCounts
                .Transform(metadataValidator)
                .To(outputSuppressed, Produced.With(STRING_SERDE, Serdes.Long()));

            valueCounts
                .ToStream()
                .Transform(metadataValidator)
                .To(outputRaw, Produced.With(STRING_SERDE, Serdes.Long()));

            StreamsConfig streamsConfig = new StreamsConfig()
            {
                ApplicationId = appId,
                BootstrapServers = CLUSTER.bootstrapServers(),
                PollMs = COMMIT_INTERVAL,
                StateStoreDirectory = TestUtils.GetTempDirectory(),
            };

            streamsConfig.Set(StreamsConfig.ProcessingGuaranteeConfig, eosEnabled ? StreamsConfig.ExactlyOnceConfig : StreamsConfig.AtLeastOnceConfig),

            IKafkaStreamsThread driver = GetStartedStreams(streamsConfig, builder, true);
            try
            {
                // start by putting some stuff in the buffer
                // note, we send All input records to partition 0
                // to make sure that supppress doesn't erroneously send records to other partitions.
                ProduceSynchronouslyToPartitionZero(
                    input,
                    Arrays.asList(
                        new KeyValueTimestamp<string, string>("k1", "v1", scaledTime(1L)),
                        new KeyValueTimestamp<string, string>("k2", "v2", scaledTime(2L)),
                        new KeyValueTimestamp<string, string>("k3", "v3", scaledTime(3L))
                    )
                );

                VerifyOutput(
                    outputRaw,
                    new HashSet<KeyValueTimestamp<string, long>>(Arrays.asList(
                        new KeyValueTimestamp<string, long>("k1", 1L, scaledTime(1L)),
                        new KeyValueTimestamp<string, long>("k2", 1L, scaledTime(2L)),
                        new KeyValueTimestamp<string, long>("k3", 1L, scaledTime(3L))
                    ))
                );
                Assert.Equal(0, eventCount);

                // Flush two of the first three events System.Console.Out.
                ProduceSynchronouslyToPartitionZero(
                    input,
                    Arrays.asList(
                        new KeyValueTimestamp<string, string>("k4", "v4", scaledTime(4L)),
                        new KeyValueTimestamp<string, string>("k5", "v5", scaledTime(5L))
                    )
                );
                verifyOutput(
                    outputRaw,
                    new HashSet<KeyValueTimestamp<string, long>>(Arrays.asList(
                        new KeyValueTimestamp<string, long>("k4", 1L, scaledTime(4L)),
                        new KeyValueTimestamp<string, long>("k5", 1L, scaledTime(5L))
                    ))
                );
                Assert.Equal(2, eventCount);
                verifyOutput(
                    outputSuppressed,
                    Arrays.asList(
                        new KeyValueTimestamp<string, long>("k1", 1L, scaledTime(1L)),
                        new KeyValueTimestamp<string, long>("k2", 1L, scaledTime(2L))
                    )
                );

                // bounce to ensure that the history, including retractions,
                // get restored properly. (i.e., we shouldn't see those first events again)

                // restart the driver
                driver.Close();
                Assert.Equal(KafkaStreamsThreadStates.NOT_RUNNING, driver.State.CurrentState);
                driver = getStartedStreams(streamsConfig, builder, false);


                // Flush those recovered buffered events System.Console.Out.
                ProduceSynchronouslyToPartitionZero(
                    input,
                    Arrays.asList(
                                new KeyValueTimestamp<string, string>("k6", "v6", scaledTime(6L)),
                                new KeyValueTimestamp<string, string>("k7", "v7", scaledTime(7L)),
                                new KeyValueTimestamp<string, string>("k8", "v8", scaledTime(8L))
                            )
                        );
                verifyOutput(
                    outputRaw,
                    new HashSet<KeyValueTimestamp<string, long>>(Arrays.asList(
                        new KeyValueTimestamp<string, long>("k6", 1L, scaledTime(6L)),
                        new KeyValueTimestamp<string, long>("k7", 1L, scaledTime(7L)),
                        new KeyValueTimestamp<string, long>("k8", 1L, scaledTime(8L))
                    ))
                );
                Assert.Equal(5, eventCount);

                verifyOutput(
                    outputSuppressed,
                    Arrays.asList(
                        new KeyValueTimestamp<string, long>("k3", 1L, scaledTime(3L)),
                        new KeyValueTimestamp<string, long>("k4", 1L, scaledTime(4L)),
                        new KeyValueTimestamp<string, long>("k5", 1L, scaledTime(5L))
                    )
                );

                metadataValidator.RaiseExceptionIfAny();

            }
            finally
            {
                driver.Close();
                cleanStateAfterTest(CLUSTER, driver);
            }
        }

        private class MetadataValidator : ITransformerSupplier<string, long, KeyValuePair<string, long>>
        {
            //private AtomicReference<Throwable> firstException = new AtomicReference<>();
            private readonly string topic;

            public MetadataValidator(string topic)
            {
                this.topic = topic;
            }


            public ITransformer<string, long, KeyValuePair<string, long>> Get()
            {
                return null;
            }
            //            {
            //                return new Transformer<string, long, KeyValuePair<string, long>>()
            //                {
            //                private ProcessorContext context;
            //
            //
            //            public void Init(IProcessorContext context)
            //            {
            //                this.context = context;
            //            }
            //
            //
            //            public KeyValuePair<string, long> Transform(string key, long value)
            //            {
            //                try
            //                {
            //                    Assert.Equal(context.Topic, topic);
            //                }
            //                catch (Throwable e)
            //                {
            //                    firstException.compareAndSet(null, e);
            //                    LOG.error("Validation Failed", e);
            //                }
            //                return KeyValuePair.Create(key, value);
            //            }
            //
            //
            //            public void Close()
            //            {
            //
            //            }
            //        };


            void RaiseExceptionIfAny()
            {
                Throwable exception = firstException.Get();
                if (exception != null)
                {
                    throw new Exception("Got an exception during run", exception);
                }
            }

            private void VerifyOutput(string topic, List<KeyValueTimestamp<string, long>> keyValueTimestamps)
            {
                StreamsConfig properties = mkProperties(
                    mkMap(
                        mkEntry(ConsumerConfig.GROUP_ID_CONFIG, "test-group"),
                        mkEntry(ConsumerConfig.BootstrapServersConfig, CLUSTER.bootstrapServers()),
                        mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ((IDeserializer<string>)STRING_DESERIALIZER).GetType().FullName),
                        mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ((IDeserializer<long>)LONG_DESERIALIZER).GetType().FullName)
                    ));

                IntegrationTestUtils.VerifyKeyValueTimestamps(properties, topic, keyValueTimestamps);
            }

            private void VerifyOutput(string topic, HashSet<KeyValueTimestamp<string, long>> keyValueTimestamps)
            {
                StreamsConfig properties = mkProperties(
                    mkMap(
                        mkEntry(ConsumerConfig.GROUP_ID_CONFIG, "test-group"),
                        mkEntry(ConsumerConfig.BootstrapServersConfig, CLUSTER.bootstrapServers()),
                        mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ((IDeserializer<string>)STRING_DESERIALIZER).GetType().FullName),
                        mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ((IDeserializer<long>)LONG_DESERIALIZER).GetType().FullName)
                    ));

                IntegrationTestUtils.VerifyKeyValueTimestamps(properties, topic, keyValueTimestamps);
            }

            /**
             * scaling to ensure that there are commits in between the various test events,
             * just to exercise that everything works properly in the presence of commits.
             */
            private long ScaledTime(long unscaledTime)
            {
                return COMMIT_INTERVAL * 2 * unscaledTime;
            }

            internal static void ProduceSynchronouslyToPartitionZero(string topic, List<KeyValueTimestamp<string, string>> toProduce)
            {
                StreamsConfig ProducerConfig = mkProperties(mkMap(
                    mkEntry(ProducerConfig.CLIENT_ID_CONFIG, "anything"),
                    mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ((ISerializer<string>)STRING_SERIALIZER).GetType().FullName),
                    mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ((ISerializer<string>)STRING_SERIALIZER).GetType().FullName),
                    mkEntry(ProducerConfig.BootstrapServersConfig, CLUSTER.bootstrapServers())
                ));

                IntegrationTestUtils.ProduceSynchronously(ProducerConfig, false, topic, 0, toProduce);
            }
        }
    }
}
