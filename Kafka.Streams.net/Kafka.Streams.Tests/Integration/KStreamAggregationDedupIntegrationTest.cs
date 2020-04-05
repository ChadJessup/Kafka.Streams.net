//using Confluent.Kafka;
//using Kafka.Streams;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.KStream.Mappers;
//using Kafka.Streams.Tests.Mocks;
//using System.Collections.Generic;

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
//        private static volatile AtomicInteger testNo = new AtomicInteger(0);
//        private StreamsBuilder builder;
//        private StreamsConfig streamsConfiguration;
//        private KafkaStreams kafkaStreams;
//        private string streamOneInput;
//        private string outputTopic;
//        private KGroupedStream<string, string> groupedStream;
//        private Reducer<string> reducer;
//        private KStream<int, string> stream;


//        public void Before()
//        {// throws InterruptedException
//            builder = new StreamsBuilder();
//            CreateTopics();
//            streamsConfiguration = new StreamsConfig();
//            string applicationId = "kgrouped-stream-test-" + testNo.incrementAndGet();
//            streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
//            streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//            streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath());
//            streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS);
//            streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);

//            KeyValueMapper<int, string, string> mapper = MockMapper.selectValueMapper();
//            stream = builder.Stream(streamOneInput, Consumed.With(Serdes.Int(), Serdes.String()));
//            groupedStream = stream.groupBy(mapper, Grouped.with(Serdes.String(), Serdes.String()));

//            reducer = (value1, value2) => value1 + ":" + value2;
//        }


//        public void WhenShuttingDown()
//        { //throws IOException
//            if (kafkaStreams != null)
//            {
//                kafkaStreams.close();
//            }
//            IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
//        }


//        [Fact]
//        public void ShouldReduce()
//        {// throws Exception
//            produceMessages(System.currentTimeMillis());
//            groupedStream
//                    .reduce(reducer, Materialized.As("reduce-by-key"))
//                    .toStream()
//                    .To(outputTopic, Produced.With(Serdes.String(), Serdes.String()));

//            StartStreams();

//            long timestamp = System.currentTimeMillis();
//            ProduceMessages(timestamp);

//            validateReceivedMessages(
//                    new Serdes.String().Deserializer(),
//                    new Serdes.String().Deserializer(),
//                    Array.asList(
//                        new KeyValueTimestamp<>("A", "A:A", timestamp),
//                        new KeyValueTimestamp<>("B", "B:B", timestamp),
//                        new KeyValueTimestamp<>("C", "C:C", timestamp),
//                        new KeyValueTimestamp<>("D", "D:D", timestamp),
//                        new KeyValueTimestamp<>("E", "E:E", timestamp)));
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
//                .windowedBy(TimeWindows.of(FromMilliseconds(500L)))
//                .reduce(reducer, Materialized.As("reduce-time-windows"))
//                    .toStream((windowedKey, value) => windowedKey.Key + "@" + windowedKey.window().start())
//                    .To(outputTopic, Produced.With(Serdes.String(), Serdes.String()));

//            StartStreams();

//            long firstBatchWindow = firstBatchTimestamp / 500 * 500;
//            long secondBatchWindow = secondBatchTimestamp / 500 * 500;

//            validateReceivedMessages(
//                    new Serdes.String().Deserializer(),
//                    new Serdes.String().Deserializer(),
//                    Array.asList(
//                        new KeyValueTimestamp<>("A@" + firstBatchWindow, "A", firstBatchTimestamp),
//                        new KeyValueTimestamp<>("A@" + secondBatchWindow, "A:A", secondBatchTimestamp),
//                        new KeyValueTimestamp<>("B@" + firstBatchWindow, "B", firstBatchTimestamp),
//                        new KeyValueTimestamp<>("B@" + secondBatchWindow, "B:B", secondBatchTimestamp),
//                        new KeyValueTimestamp<>("C@" + firstBatchWindow, "C", firstBatchTimestamp),
//                        new KeyValueTimestamp<>("C@" + secondBatchWindow, "C:C", secondBatchTimestamp),
//                        new KeyValueTimestamp<>("D@" + firstBatchWindow, "D", firstBatchTimestamp),
//                        new KeyValueTimestamp<>("D@" + secondBatchWindow, "D:D", secondBatchTimestamp),
//                        new KeyValueTimestamp<>("E@" + firstBatchWindow, "E", firstBatchTimestamp),
//                        new KeyValueTimestamp<>("E@" + secondBatchWindow, "E:E", secondBatchTimestamp)
//                    )
//            );
//        }

//        [Fact]
//        public void ShouldGroupByKey()
//        {// throws Exception
//            long timestamp = mockTime.GetCurrentInstant().ToUnixTimeMilliseconds();;
//            ProduceMessages(timestamp);
//            ProduceMessages(timestamp);

//            stream.groupByKey(Grouped.with(Serdes.Int(), Serdes.String()))
//                .windowedBy(TimeWindow.of(FromMilliseconds(500L)))
//                .count(Materialized.As("count-windows"))
//                    .toStream((windowedKey, value) => windowedKey.Key + "@" + windowedKey.window().start())
//                    .To(outputTopic, Produced.With(Serdes.String(), Serdes.Long()));

//            StartStreams();

//            long window = timestamp / 500 * 500;

//            validateReceivedMessages(
//                    new Serdes.String().Deserializer(),
//                    new LongDeserializer(),
//                    Array.asList(
//                        new KeyValueTimestamp<>("1@" + window, 2L, timestamp),
//                        new KeyValueTimestamp<>("2@" + window, 2L, timestamp),
//                        new KeyValueTimestamp<>("3@" + window, 2L, timestamp),
//                        new KeyValueTimestamp<>("4@" + window, 2L, timestamp),
//                        new KeyValueTimestamp<>("5@" + window, 2L, timestamp)
//                    )
//            );
//        }


//        private void ProduceMessages(long timestamp)
//        {// throws Exception
//            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
//                streamOneInput,
//                Array.asList(
//                    KeyValuePair.Create(1, "A"),
//                    KeyValuePair.Create(2, "B"),
//                    KeyValuePair.Create(3, "C"),
//                    KeyValuePair.Create(4, "D"),
//                    KeyValuePair.Create(5, "E")),
//                TestUtils.producerConfig(
//                    CLUSTER.bootstrapServers(),
//                    IntegerSerializer,
//                    Serdes.String().Serializer,
//                    new StreamsConfig()),
//                timestamp);
//        }


//        private void CreateTopics()
//        {// throws InterruptedException
//            streamOneInput = "stream-one-" + testNo;
//            outputTopic = "output-" + testNo;
//            CLUSTER.createTopic(streamOneInput, 3, 1);
//            CLUSTER.createTopic(outputTopic);
//        }

//        private void StartStreams()
//        {
//            kafkaStreams = new KafkaStreams(builder.Build(), streamsConfiguration);
//            kafkaStreams.start();
//        }


//        private void ValidateReceivedMessages<K, V>(IDeserializer<K> keyDeserializer,
//                                                     IDeserializer<V> valueDeserializer,
//                                                     List<KeyValueTimestamp<K, V>> expectedRecords)
//        // throws InterruptedException
//        {
//            StreamsConfig consumerProperties = new StreamsConfig();
//            consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//            consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kgroupedstream-test-" + testNo);
//            consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.GetType().FullName);
//            consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.GetType().FullName);

//            IntegrationTestUtils.waitUntilFinalKeyValueTimestampRecordsReceived(
//                consumerProperties,
//                outputTopic,
//                expectedRecords);
//        }

//    }
//}
