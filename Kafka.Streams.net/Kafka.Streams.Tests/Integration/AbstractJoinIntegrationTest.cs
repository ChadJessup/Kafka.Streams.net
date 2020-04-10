//using Confluent.Kafka;
//using Xunit;
//using System;
//using System.Collections.Generic;
//using Kafka.Streams.KStream;
//using Kafka.Streams.Tests.Helpers;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.State;
//using Kafka.Streams.State.Queryable;
//using Kafka.Streams.State.ReadOnly;
//using Kafka.Streams.State.KeyValues;

//namespace Kafka.Streams.Tests.Integration
//{
//    /**
//     * Tests All available joins of Kafka Streams DSL.
//     */


//    public abstract class AbstractJoinIntegrationTest
//    {

//        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);


//        public TemporaryFolder testFolder = new TemporaryFolder(TestUtils.GetTempDirectory());

//        // @Parameterized.Parameters(Name = "caching enabled = {0}")
//        public static Collection<object[]> Data()
//        {
//            List<object[]> values = new ArrayList<>();
//            foreach (bool cacheEnabled in Array.asList(true, false))
//            {
//                values.Add(new object[] { cacheEnabled });
//            }
//            return values;
//        }

//        private static readonly string appID;

//        private const long COMMIT_INTERVAL = 100L;
//        private static StreamsConfig STREAMS_CONFIG = new StreamsConfig();
//        private const string INPUT_TOPIC_RIGHT = "inputTopicRight";
//        private const string INPUT_TOPIC_LEFT = "inputTopicLeft";
//        private const string OUTPUT_TOPIC = "outputTopic";
//        private const long ANY_UNIQUE_KEY = 0L;

//        private static StreamsConfig PRODUCER_CONFIG = new StreamsConfig();
//        private static StreamsConfig RESULT_CONSUMER_CONFIG = new StreamsConfig();

//        private IProducer<long, string> producer;
//        private KafkaStreams streams;

//        StreamsBuilder builder;
//        readonly int numRecordsExpected = 0;
//        AtomicBoolean finalResultReached = new AtomicBoolean(false);

//        private readonly List<Input<string>> input = Array.asList(
//                new Input<string>(INPUT_TOPIC_LEFT, null),
//                new Input<string>(INPUT_TOPIC_RIGHT, null),
//                new Input<string>(INPUT_TOPIC_LEFT, "A"),
//                new Input<string>(INPUT_TOPIC_RIGHT, "a"),
//                new Input<string>(INPUT_TOPIC_LEFT, "B"),
//                new Input<string>(INPUT_TOPIC_RIGHT, "b"),
//                new Input<string>(INPUT_TOPIC_LEFT, null),
//                new Input<string>(INPUT_TOPIC_RIGHT, null),
//                new Input<string>(INPUT_TOPIC_LEFT, "C"),
//                new Input<string>(INPUT_TOPIC_RIGHT, "c"),
//                new Input<string>(INPUT_TOPIC_RIGHT, null),
//                new Input<string>(INPUT_TOPIC_LEFT, null),
//                new Input<string>(INPUT_TOPIC_RIGHT, null),
//                new Input<string>(INPUT_TOPIC_RIGHT, "d"),
//                new Input<string>(INPUT_TOPIC_LEFT, "D")
//        );
//        readonly IValueJoiner<string, string, string> valueJoiner = (value1, value2) => value1 + "-" + value2;
//        readonly bool cacheEnabled;

//        AbstractJoinIntegrationTest(bool cacheEnabled)
//        {
//            this.cacheEnabled = cacheEnabled;
//        }


//        public static void SetupConfigsAndUtils()
//        {
//            PRODUCER_CONFIG.Put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//            PRODUCER_CONFIG.Put(ProducerConfig.ACKS_CONFIG, "All");
//            PRODUCER_CONFIG.Put(ProducerConfig.RETRIES_CONFIG, 0);
//            PRODUCER_CONFIG.Put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.Long().Serializer);
//            PRODUCER_CONFIG.Put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);

//            RESULT_CONSUMER_CONFIG.Put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//            RESULT_CONSUMER_CONFIG.Put(ConsumerConfig.GROUP_ID_CONFIG, appID + "-result-consumer");
//            RESULT_CONSUMER_CONFIG.Put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            RESULT_CONSUMER_CONFIG.Put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer);
//            RESULT_CONSUMER_CONFIG.Put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().Deserializer);

//            STREAMS_CONFIG.Put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            STREAMS_CONFIG.Put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//            STREAMS_CONFIG.Put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
//            STREAMS_CONFIG.Put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//            STREAMS_CONFIG.Put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL);
//        }

//        void PrepareEnvironment()
//        {// throws InterruptedException
//            CLUSTER.createTopics(INPUT_TOPIC_LEFT, INPUT_TOPIC_RIGHT, OUTPUT_TOPIC);

//            if (!cacheEnabled)
//            {
//                STREAMS_CONFIG.Put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//            }

//            STREAMS_CONFIG.Put(StreamsConfig.STATE_DIR_CONFIG, testFolder.getRoot().getPath());

//            producer = new KafkaProducer<>(PRODUCER_CONFIG);
//        }


//        public void Cleanup()
//        {// throws InterruptedException
//            producer.Close(TimeSpan.FromMilliseconds(0));
//            CLUSTER.deleteAllTopicsAndWait(120000);
//        }

//        private void CheckResult(string outputTopic, List<KeyValueTimestamp<long, string>> expectedResult)
//        {// throws InterruptedException
//            IntegrationTestUtils.verifyKeyValueTimestamps(RESULT_CONSUMER_CONFIG, outputTopic, expectedResult);
//        }

//        private void CheckResult(string outputTopic, KeyValueTimestamp<long, string> expectedFinalResult, int expectedTotalNumRecords)
//        {// throws InterruptedException
//            List<KeyValueTimestamp<long, string>> result =
//                IntegrationTestUtils.waitUntilMinKeyValueWithTimestampRecordsReceived(RESULT_CONSUMER_CONFIG, outputTopic, expectedTotalNumRecords, 30 * 1000L);
//            Assert.Equal(result.Get(result.Count - 1), expectedFinalResult);
//        }

//        /*
//         * Runs the actual test. Checks the result after each input record to ensure fixed processing order.
//         * If an input tuple does not trigger any result, "expectedResult" should contain a "null" entry
//         */
//        void RunTest(List<List<KeyValueTimestamp<long, string>>> expectedResult)
//        {// throws Exception
//            RunTest(expectedResult, null);
//        }


//        /*
//         * Runs the actual test. Checks the result after each input record to ensure fixed processing order.
//         * If an input tuple does not trigger any result, "expectedResult" should contain a "null" entry
//         */
//        void RunTest(List<List<KeyValueTimestamp<long, string>>> expectedResult, string storeName)
//        {// throws Exception
//            Assert.True(expectedResult.Count == input.Count);

//            IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG);
//            streams = new KafkaStreams(builder.Build(), STREAMS_CONFIG);

//            KeyValueTimestamp<long, string> expectedFinalResult = null;

//            try
//            {
//                streams.start();

//                long firstTimestamp = System.currentTimeMillis();
//                long ts = firstTimestamp;

//                Iterator<List<KeyValueTimestamp<long, string>>> resultIterator = expectedResult.iterator();
//                foreach (Input<string> singleInput in input)
//                {
//                    producer.send(new ProducerRecord<>(singleInput.topic, null, ++ts, singleInput.record.key, singleInput.record.value)).Get();

//                    List<KeyValueTimestamp<long, string>> expected = resultIterator.MoveNext();

//                    if (expected != null)
//                    {
//                        List<KeyValueTimestamp<long, string>> updatedExpected = new LinkedList<>();
//                        foreach (KeyValueTimestamp<long, string> record in expected)
//                        {
//                            updatedExpected.Add(new KeyValueTimestamp<>(record.Key, record.Value, firstTimestamp + record.Timestamp));
//                        }

//                        CheckResult(OUTPUT_TOPIC, updatedExpected);
//                        expectedFinalResult = updatedExpected.Get(expected.Count - 1);
//                    }
//                }

//                if (storeName != null)
//                {
//                    CheckQueryableStore(storeName, expectedFinalResult);
//                }
//            }
//            finally
//            {
//                streams.Close();
//            }
//        }

//        /*
//         * Runs the actual test. Checks the result only after expected number of records have been consumed.
//         */
//        void RunTest(KeyValueTimestamp<long, string> expectedFinalResult)
//        {// throws Exception
//            RunTest(expectedFinalResult, null);
//        }

//        /*
//         * Runs the actual test. Checks the result only after expected number of records have been consumed.
//         */
//        void RunTest(KeyValueTimestamp<long, string> expectedFinalResult, string storeName)
//        {// throws Exception
//            IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG);
//            streams = new KafkaStreams(builder.Build(), STREAMS_CONFIG);

//            try
//            {
//                streams.start();

//                long firstTimestamp = System.currentTimeMillis();
//                long ts = firstTimestamp;

//                foreach (Input<string> singleInput in input)
//                {
//                    producer.send(new ProducerRecord<>(singleInput.topic, null, ++ts, singleInput.record.key, singleInput.record.value)).Get();
//                }

//                TestUtils.WaitForCondition(() => finalResultReached.Get(), "Never received expected result.");

//                KeyValueTimestamp<long, string> updatedExpectedFinalResult =
//                    new KeyValueTimestamp<>(
//                        expectedFinalResult.Key,
//                        expectedFinalResult.Value,
//                        firstTimestamp + expectedFinalResult.Timestamp);
//                CheckResult(OUTPUT_TOPIC, updatedExpectedFinalResult, numRecordsExpected);

//                if (storeName != null)
//                {
//                    CheckQueryableStore(storeName, updatedExpectedFinalResult);
//                }
//            }
//            finally
//            {
//                streams.Close();
//            }
//        }

//        /*
//         * Checks the embedded queryable state store snapshot
//         */
//        private void CheckQueryableStore(string queryableName, KeyValueTimestamp<long, string> expectedFinalResult)
//        {
//            IReadOnlyKeyValueStore<long, ValueAndTimestamp<string>> store = streams.store(queryableName, QueryableStoreTypes.TimestampedKeyValueStore());

//            IKeyValueIterator<long, ValueAndTimestamp<string>> All = store.All();
//            KeyValuePair<long, ValueAndTimestamp<string>> onlyEntry = All.MoveNext();

//            try
//            {
//                Assert.Equal(onlyEntry.key, expectedFinalResult.Key);
//                Assert.Equal(onlyEntry.value.Value, (expectedFinalResult.Value));
//                Assert.Equal(onlyEntry.value.Timestamp, (expectedFinalResult.Timestamp));
//                Assert.Equal(All.HasNext(), (false));
//            }
//            finally
//            {
//                All.Close();
//            }
//        }

//        private class Input<V>
//        {
//            readonly string topic;
//            readonly KeyValuePair<long, V> record;

//            public Input(string topic, V value)
//            {
//                this.topic = topic;
//                record = KeyValuePair.Create(ANY_UNIQUE_KEY, value);
//            }
//        }
//    }
//}
