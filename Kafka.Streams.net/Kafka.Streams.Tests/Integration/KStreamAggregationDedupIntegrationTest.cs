using Confluent.Kafka;
using Kafka.Streams;
using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.KStream.Mappers;
using Kafka.Streams.Tests.Mocks;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Integration
{
    /**
    * Similar to KStreamAggregationIntegrationTest but with dedupping enabled
    * by virtue of having a large commit interval
*/
    public class KStreamAggregationDedupIntegrationTest
    {
        private const int NUM_BROKERS = 1;
        private const long COMMIT_INTERVAL_MS = 300L;


        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

        private readonly MockTime mockTime = CLUSTER.time;
        private static volatile int testNo = new int(0);
        private StreamsBuilder builder;
        private StreamsConfig streamsConfiguration;
        private KafkaStreamsThread kafkaStreams;
        private string streamOneInput;
        private string outputTopic;
        private KGroupedStream<string, string> groupedStream;
        private Reducer<string> reducer;
        private KStream<K, V> stream;


        public void Before()
        {// throws InterruptedException
            builder = new StreamsBuilder();
            CreateTopics();
            streamsConfiguration = new StreamsConfig();
            string applicationId = "kgrouped-stream-test-" + testNo.incrementAndGet();
            streamsConfiguration.Put(StreamsConfig.ApplicationIdConfig, applicationId);
            streamsConfiguration.Put(StreamsConfig.BootstrapServersConfig, CLUSTER.bootstrapServers());
            streamsConfiguration.Put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            streamsConfiguration.Put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath());
            streamsConfiguration.Put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS);
            streamsConfiguration.Put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);

            KeyValueMapper<int, string, string> mapper = MockMapper.selectValueMapper();
            stream = builder.Stream(streamOneInput, Consumed.With(Serdes.Int(), Serdes.String()));
            groupedStream = stream.GroupBy(mapper, Grouped.With(Serdes.String(), Serdes.String()));

            reducer = (value1, value2) => value1 + ":" + value2;
        }


        public void WhenShuttingDown()
        { //throws IOException
            if (kafkaStreams != null)
            {
                kafkaStreams.Close();
            }
            IntegrationTestUtils.PurgeLocalStreamsState(streamsConfiguration);
        }


        [Fact]
        public void ShouldReduce()
        {// throws Exception
            produceMessages(System.currentTimeMillis());
            groupedStream
                    .Reduce(reducer, Materialized.As("reduce-by-key"))
                    .ToStream()
                    .To(outputTopic, Produced.With(Serdes.String(), Serdes.String()));

            StartStreams();

            long timestamp = System.currentTimeMillis();
            ProduceMessages(timestamp);

            validateReceivedMessages(
                    new Serdes.String().Deserializer(),
                    new Serdes.String().Deserializer(),
                    Arrays.asList(
                        new KeyValueTimestamp<>("A", "A:A", timestamp),
                        new KeyValueTimestamp<>("B", "B:B", timestamp),
                        new KeyValueTimestamp<>("C", "C:C", timestamp),
                        new KeyValueTimestamp<>("D", "D:D", timestamp),
                        new KeyValueTimestamp<>("E", "E:E", timestamp)));
        }

        [Fact]
        public void ShouldReduceWindowed()
        {// throws Exception
            long firstBatchTimestamp = System.currentTimeMillis() - 1000;
            ProduceMessages(firstBatchTimestamp);
            long secondBatchTimestamp = System.currentTimeMillis();
            ProduceMessages(secondBatchTimestamp);
            ProduceMessages(secondBatchTimestamp);

            groupedStream
                .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(500L)))
                .Reduce(reducer, Materialized.As("reduce-time-windows"))
                    .toStream((windowedKey, value) => windowedKey.Key + "@" + windowedKey.window().Start())
                    .To(outputTopic, Produced.With(Serdes.String(), Serdes.String()));

            StartStreams();

            long firstBatchWindow = firstBatchTimestamp / 500 * 500;
            long secondBatchWindow = secondBatchTimestamp / 500 * 500;

            validateReceivedMessages(
                    new Serdes.String().Deserializer(),
                    new Serdes.String().Deserializer(),
                    Arrays.asList(
                        new KeyValueTimestamp<>("A@" + firstBatchWindow, "A", firstBatchTimestamp),
                        new KeyValueTimestamp<>("A@" + secondBatchWindow, "A:A", secondBatchTimestamp),
                        new KeyValueTimestamp<>("B@" + firstBatchWindow, "B", firstBatchTimestamp),
                        new KeyValueTimestamp<>("B@" + secondBatchWindow, "B:B", secondBatchTimestamp),
                        new KeyValueTimestamp<>("C@" + firstBatchWindow, "C", firstBatchTimestamp),
                        new KeyValueTimestamp<>("C@" + secondBatchWindow, "C:C", secondBatchTimestamp),
                        new KeyValueTimestamp<>("D@" + firstBatchWindow, "D", firstBatchTimestamp),
                        new KeyValueTimestamp<>("D@" + secondBatchWindow, "D:D", secondBatchTimestamp),
                        new KeyValueTimestamp<>("E@" + firstBatchWindow, "E", firstBatchTimestamp),
                        new KeyValueTimestamp<>("E@" + secondBatchWindow, "E:E", secondBatchTimestamp)
                    )
            );
        }

        [Fact]
        public void ShouldGroupByKey()
        {// throws Exception
            long timestamp = mockTime.NowAsEpochMilliseconds; ;
            ProduceMessages(timestamp);
            ProduceMessages(timestamp);

            stream.GroupByKey(Grouped.With(Serdes.Int(), Serdes.String()))
                .WindowedBy(TimeWindow.Of(TimeSpan.FromMilliseconds(500L)))
                .Count(Materialized.As("count-windows"))
                    .toStream((windowedKey, value) => windowedKey.Key + "@" + windowedKey.window().Start())
                    .To(outputTopic, Produced.With(Serdes.String(), Serdes.Long()));

            StartStreams();

            long window = timestamp / 500 * 500;

            validateReceivedMessages(
                    new Serdes.String().Deserializer(),
                    new LongDeserializer(),
                    Arrays.asList(
                        new KeyValueTimestamp<>("1@" + window, 2L, timestamp),
                        new KeyValueTimestamp<>("2@" + window, 2L, timestamp),
                        new KeyValueTimestamp<>("3@" + window, 2L, timestamp),
                        new KeyValueTimestamp<>("4@" + window, 2L, timestamp),
                        new KeyValueTimestamp<>("5@" + window, 2L, timestamp)
                    )
            );
        }


        private void ProduceMessages(long timestamp)
        {// throws Exception
            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
                streamOneInput,
                Arrays.asList(
                    KeyValuePair.Create(1, "A"),
                    KeyValuePair.Create(2, "B"),
                    KeyValuePair.Create(3, "C"),
                    KeyValuePair.Create(4, "D"),
                    KeyValuePair.Create(5, "E")),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    Serdes.Int().Serializer,
                    Serdes.String().Serializer,
                    new StreamsConfig()),
                timestamp);
        }


        private void CreateTopics()
        {// throws InterruptedException
            streamOneInput = "stream-one-" + testNo;
            outputTopic = "output-" + testNo;
            CLUSTER.CreateTopic(streamOneInput, 3, 1);
            CLUSTER.CreateTopic(outputTopic);
        }

        private void StartStreams()
        {
            kafkaStreams = new KafkaStreamsThread(builder.Build(), streamsConfiguration);
            kafkaStreams.Start();
        }


        private void ValidateReceivedMessages<K, V>(IDeserializer<K> keyDeserializer,
                                                     IDeserializer<V> valueDeserializer,
                                                     List<KeyValueTimestamp<K, V>> expectedRecords)
        // throws InterruptedException
        {
            StreamsConfig consumerProperties = new StreamsConfig();
            consumerProperties.Set(ConsumerConfig.BootstrapServersConfig, CLUSTER.bootstrapServers());
            consumerProperties.Set(ConsumerConfig.GROUP_ID_CONFIG, "kgroupedstream-test-" + testNo);
            consumerProperties.Set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProperties.Set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.GetType().FullName);
            consumerProperties.Set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.GetType().FullName);

            IntegrationTestUtils.waitUntilFinalKeyValueTimestampRecordsReceived(
                consumerProperties,
                outputTopic,
                expectedRecords);
        }

    }
}
