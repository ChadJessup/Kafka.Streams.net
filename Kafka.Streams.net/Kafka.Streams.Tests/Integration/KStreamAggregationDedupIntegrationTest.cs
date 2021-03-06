//using Confluent.Kafka;
//using Kafka.Streams;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Temporary;
//using Kafka.Streams.Tests.Helpers;
//using Kafka.Streams.Tests.Mocks;
//using Kafka.Streams.Threads.KafkaStreams;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Integration
//{
//    /**
//    * Similar to KStreamAggregationIntegrationTest but with dedupping enabled
//    * by virtue of having a large commit interval
//*/
//    public class KStreamAggregationDedupIntegrationTest
//    {
//        private const int NUM_BROKERS = 1;
//        private const long COMMIT_INTERVAL_MS = 300L;


//        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

//        private readonly MockTime mockTime = CLUSTER.time;
//        private static volatile int testNo = 0;
//        private StreamsBuilder builder;
//        private StreamsConfig streamsConfiguration;
//        private IKafkaStreamsThread kafkaStreams;
//        private string streamOneInput;
//        private string outputTopic;
//        private KGroupedStream<string, string> groupedStream;
//        private Reducer<string> reducer;
//        private IKStream<string, string> stream;

//        public KStreamAggregationDedupIntegrationTest()
//        {// throws InterruptedException
//            builder = new StreamsBuilder();
//            CreateTopics();
//            streamsConfiguration = new StreamsConfig();
//            string applicationId = "kgrouped-stream-test-" + ++testNo;
//            streamsConfiguration.ApplicationId = applicationId;
//            streamsConfiguration.BootstrapServers = CLUSTER.bootstrapServers();
//            streamsConfiguration.AutoOffsetReset = AutoOffsetReset.Earliest;
//            streamsConfiguration.StateStoreDirectory = TestUtils.GetTempDirectory();
//            streamsConfiguration.CommitIntervalMs = COMMIT_INTERVAL_MS;
//            streamsConfiguration.CacheMaxBytesBuffering = 10 * 1024 * 1024L;

//            KeyValueMapper<int, string, string> mapper = MockMapper.GetSelectValueMapper<int, string>();
//            stream = builder.Stream(streamOneInput, Consumed.With(Serdes.Int(), Serdes.String()));
//            groupedStream = stream.GroupBy(mapper, Grouped.With(Serdes.String(), Serdes.String()));

//            reducer = (value1, value2) => value1 + ":" + value2;
//        }

//        private void WhenShuttingDown()
//        { //throws IOException
//            if (kafkaStreams != null)
//            {
//                kafkaStreams.Close();
//            }
//            IntegrationTestUtils.PurgeLocalStreamsState(streamsConfiguration);
//        }


//        [Fact]
//        public void ShouldReduce()
//        {// throws Exception
//            ProduceMessages(System.currentTimeMillis());
//            groupedStream
//                    .Reduce(reducer, Materialized.As("reduce-by-key"))
//                    .ToStream()
//                    .To(outputTopic, Produced.With(Serdes.String(), Serdes.String()));

//            StartStreams();

//            long timestamp = System.currentTimeMillis();
//            ProduceMessages(timestamp);

//            ValidateReceivedMessages(
//                    Serdes.String().Deserializer,
//                    Serdes.String().Deserializer,
//                    Arrays.asList(
//                        new KeyValueTimestamp<string, string>("A", "A:A", timestamp),
//                        new KeyValueTimestamp<string, string>("B", "B:B", timestamp),
//                        new KeyValueTimestamp<string, string>("C", "C:C", timestamp),
//                        new KeyValueTimestamp<string, string>("D", "D:D", timestamp),
//                        new KeyValueTimestamp<string, string>("E", "E:E", timestamp)));
//        }

//        [Fact]
//        public void ShouldReduceWindowed()
//        {// throws Exception
//            long firstBatchTimestamp = System.currentTimeMillis() - 1000;
//            ProduceMessages(firstBatchTimestamp);
//            long secondBatchTimestamp = System.currentTimeMillis();
//            ProduceMessages(secondBatchTimestamp);
//            ProduceMessages(secondBatchTimestamp);

//            groupedStream
//                .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(500L)))
//                .Reduce(reducer, Materialized.As("reduce-time-windows"))
//                    .toStream((windowedKey, value) => windowedKey.Key + "@" + windowedKey.window().Start())
//                    .To(outputTopic, Produced.With(Serdes.String(), Serdes.String()));

//            StartStreams();

//            long firstBatchWindow = firstBatchTimestamp / 500 * 500;
//            long secondBatchWindow = secondBatchTimestamp / 500 * 500;

//            ValidateReceivedMessages(
//                    Serdes.String().Deserializer,
//                    Serdes.String().Deserializer,
//                    Arrays.asList(
//                        new KeyValueTimestamp<string, string>("A@" + firstBatchWindow, "A", firstBatchTimestamp),
//                        new KeyValueTimestamp<string, string>("A@" + secondBatchWindow, "A:A", secondBatchTimestamp),
//                        new KeyValueTimestamp<string, string>("B@" + firstBatchWindow, "B", firstBatchTimestamp),
//                        new KeyValueTimestamp<string, string>("B@" + secondBatchWindow, "B:B", secondBatchTimestamp),
//                        new KeyValueTimestamp<string, string>("C@" + firstBatchWindow, "C", firstBatchTimestamp),
//                        new KeyValueTimestamp<string, string>("C@" + secondBatchWindow, "C:C", secondBatchTimestamp),
//                        new KeyValueTimestamp<string, string>("D@" + firstBatchWindow, "D", firstBatchTimestamp),
//                        new KeyValueTimestamp<string, string>("D@" + secondBatchWindow, "D:D", secondBatchTimestamp),
//                        new KeyValueTimestamp<string, string>("E@" + firstBatchWindow, "E", firstBatchTimestamp),
//                        new KeyValueTimestamp<string, string>("E@" + secondBatchWindow, "E:E", secondBatchTimestamp)));
//        }

//        [Fact]
//        public void ShouldGroupByKey()
//        {// throws Exception
//            long timestamp = mockTime.NowAsEpochMilliseconds;
//            ProduceMessages(timestamp);
//            ProduceMessages(timestamp);

//            stream.GroupByKey(Grouped.With(Serdes.Int(), Serdes.String()))
//                .WindowedBy(TimeWindow.Of(TimeSpan.FromMilliseconds(500L)))
//                .Count(Materialized.As("count-windows"))
//                    .toStream((windowedKey, value) => windowedKey.Key + "@" + windowedKey.window().Start())
//                    .To(outputTopic, Produced.With(Serdes.String(), Serdes.Long()));

//            StartStreams();

//            long window = timestamp / 500 * 500;

//            ValidateReceivedMessages(
//                    Serdes.String().Deserializer,
//                    Serdes.Long().Deserializer,
//                    Arrays.asList(
//                        new KeyValueTimestamp<string, string>("1@" + window, 2L, timestamp),
//                        new KeyValueTimestamp<string, string>("2@" + window, 2L, timestamp),
//                        new KeyValueTimestamp<string, string>("3@" + window, 2L, timestamp),
//                        new KeyValueTimestamp<string, string>("4@" + window, 2L, timestamp),
//                        new KeyValueTimestamp<string, string>("5@" + window, 2L, timestamp)
//                    )
//            );
//        }

//        private void ProduceMessages(long timestamp)
//        {// throws Exception
//            IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(
//                streamOneInput,
//                Arrays.asList(
//                    KeyValuePair.Create(1, "A"),
//                    KeyValuePair.Create(2, "B"),
//                    KeyValuePair.Create(3, "C"),
//                    KeyValuePair.Create(4, "D"),
//                    KeyValuePair.Create(5, "E")),
//                TestUtils.ProducerConfig(
//                    CLUSTER.bootstrapServers(),
//                    Serdes.Int().Serializer,
//                    Serdes.String().Serializer,
//                    new StreamsConfig()),
//                timestamp);
//        }


//        private void CreateTopics()
//        {// throws InterruptedException
//            streamOneInput = "stream-one-" + testNo;
//            outputTopic = "output-" + testNo;
//            CLUSTER.CreateTopic(streamOneInput, 3, 1);
//            CLUSTER.CreateTopic(outputTopic);
//        }

//        private void StartStreams()
//        {
//            kafkaStreams = new KafkaStreamsThread(builder.Build(), streamsConfiguration);
//            kafkaStreams.Start();
//        }

//        private void ValidateReceivedMessages<K, V>(
//            IDeserializer<K> keyDeserializer,
//            IDeserializer<V> valueDeserializer,
//            List<KeyValueTimestamp<K, V>> expectedRecords)
//        // throws InterruptedException
//        {
//            var consumerProperties = new StreamsConfig
//            {
//                BootstrapServers = CLUSTER.bootstrapServers(),
//                GroupId = "kgroupedstream-test-" + testNo,
//                AutoOffsetReset = AutoOffsetReset.Earliest,
//                DefaultKeySerdeType = keyDeserializer.GetType(),
//                DefaultValueSerdeType = valueDeserializer.GetType()
//            };

//            IntegrationTestUtils.WaitUntilFinalKeyValueTimestampRecordsReceived(
//                consumerProperties,
//                outputTopic,
//                expectedRecords);
//        }
//    }
//}
