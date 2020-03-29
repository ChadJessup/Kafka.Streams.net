using Confluent.Kafka;
using Xunit;
using System;
using System.Collections.Generic;
using Kafka.Streams.KStream;

namespace Kafka.Streams.Tests.Integration
{
    /**
     * Tests all available joins of Kafka Streams DSL.
     */


    public abstract class AbstractJoinIntegrationTest
    {

        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);


        public TemporaryFolder testFolder = new TemporaryFolder(TestUtils.tempDirectory());

        // @Parameterized.Parameters(name = "caching enabled = {0}")
        public static Collection<object[]> Data()
        {
            List<object[]> values = new ArrayList<>();
            foreach (bool cacheEnabled in Array.asList(true, false))
            {
                values.add(new object[] { cacheEnabled });
            }
            return values;
        }

        static string appID;

        private static long COMMIT_INTERVAL = 100L;
        static Properties STREAMS_CONFIG = new Properties();
        static string INPUT_TOPIC_RIGHT = "inputTopicRight";
        static string INPUT_TOPIC_LEFT = "inputTopicLeft";
        static string OUTPUT_TOPIC = "outputTopic";
        static long ANY_UNIQUE_KEY = 0L;

        private static Properties PRODUCER_CONFIG = new Properties();
        private static Properties RESULT_CONSUMER_CONFIG = new Properties();

        private KafkaProducer<long, string> producer;
        private KafkaStreams streams;

        StreamsBuilder builder;
        int numRecordsExpected = 0;
        AtomicBoolean finalResultReached = new AtomicBoolean(false);

        private List<Input<string>> input = Array.asList(
                new Input<>(INPUT_TOPIC_LEFT, null),
                new Input<>(INPUT_TOPIC_RIGHT, null),
                new Input<>(INPUT_TOPIC_LEFT, "A"),
                new Input<>(INPUT_TOPIC_RIGHT, "a"),
                new Input<>(INPUT_TOPIC_LEFT, "B"),
                new Input<>(INPUT_TOPIC_RIGHT, "b"),
                new Input<>(INPUT_TOPIC_LEFT, null),
                new Input<>(INPUT_TOPIC_RIGHT, null),
                new Input<>(INPUT_TOPIC_LEFT, "C"),
                new Input<>(INPUT_TOPIC_RIGHT, "c"),
                new Input<>(INPUT_TOPIC_RIGHT, null),
                new Input<>(INPUT_TOPIC_LEFT, null),
                new Input<>(INPUT_TOPIC_RIGHT, null),
                new Input<>(INPUT_TOPIC_RIGHT, "d"),
                new Input<>(INPUT_TOPIC_LEFT, "D")
        );

        IValueJoiner<string, string, string> valueJoiner = (value1, value2) => value1 + "-" + value2;

        bool cacheEnabled;

        AbstractJoinIntegrationTest(bool cacheEnabled)
        {
            this.cacheEnabled = cacheEnabled;
        }


        public static void SetupConfigsAndUtils()
        {
            PRODUCER_CONFIG.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
            PRODUCER_CONFIG.put(ProducerConfig.ACKS_CONFIG, "all");
            PRODUCER_CONFIG.put(ProducerConfig.RETRIES_CONFIG, 0);
            PRODUCER_CONFIG.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer);
            PRODUCER_CONFIG.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer);

            RESULT_CONSUMER_CONFIG.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
            RESULT_CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, appID + "-result-consumer");
            RESULT_CONSUMER_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            RESULT_CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer);
            RESULT_CONSUMER_CONFIG.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer);

            STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
            STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
            STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL);
        }

        void PrepareEnvironment()
        {// throws InterruptedException
            CLUSTER.createTopics(INPUT_TOPIC_LEFT, INPUT_TOPIC_RIGHT, OUTPUT_TOPIC);

            if (!cacheEnabled)
            {
                STREAMS_CONFIG.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
            }

            STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, testFolder.getRoot().getPath());

            producer = new KafkaProducer<>(PRODUCER_CONFIG);
        }


        public void Cleanup()
        {// throws InterruptedException
            producer.close(Duration.ofMillis(0));
            CLUSTER.deleteAllTopicsAndWait(120000);
        }

        private void CheckResult(string outputTopic, List<KeyValueTimestamp<long, string>> expectedResult)
        {// throws InterruptedException
            IntegrationTestUtils.verifyKeyValueTimestamps(RESULT_CONSUMER_CONFIG, outputTopic, expectedResult);
        }

        private void CheckResult(string outputTopic, KeyValueTimestamp<long, string> expectedFinalResult, int expectedTotalNumRecords)
        {// throws InterruptedException
            List<KeyValueTimestamp<long, string>> result =
                IntegrationTestUtils.waitUntilMinKeyValueWithTimestampRecordsReceived(RESULT_CONSUMER_CONFIG, outputTopic, expectedTotalNumRecords, 30 * 1000L);
            Assert.Equal(result.get(result.Count - 1), expectedFinalResult);
        }

        /*
         * Runs the actual test. Checks the result after each input record to ensure fixed processing order.
         * If an input tuple does not trigger any result, "expectedResult" should contain a "null" entry
         */
        void RunTest(List<List<KeyValueTimestamp<long, string>>> expectedResult)
        {// throws Exception
            RunTest(expectedResult, null);
        }


        /*
         * Runs the actual test. Checks the result after each input record to ensure fixed processing order.
         * If an input tuple does not trigger any result, "expectedResult" should contain a "null" entry
         */
        void RunTest(List<List<KeyValueTimestamp<long, string>>> expectedResult, string storeName)
        {// throws Exception
            assert expectedResult.Count == input.Count;

            IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG);
            streams = new KafkaStreams(builder.build(), STREAMS_CONFIG);

            KeyValueTimestamp<long, string> expectedFinalResult = null;

            try
            {
                streams.start();

                long firstTimestamp = System.currentTimeMillis();
                long ts = firstTimestamp;

                Iterator<List<KeyValueTimestamp<long, string>>> resultIterator = expectedResult.iterator();
                foreach (Input<string> singleInput in input)
                {
                    producer.send(new ProducerRecord<>(singleInput.topic, null, ++ts, singleInput.record.key, singleInput.record.value)).get();

                    List<KeyValueTimestamp<long, string>> expected = resultIterator.next();

                    if (expected != null)
                    {
                        List<KeyValueTimestamp<long, string>> updatedExpected = new LinkedList<>();
                        foreach (KeyValueTimestamp<long, string> record in expected)
                        {
                            updatedExpected.add(new KeyValueTimestamp<>(record.Key, record.Value, firstTimestamp + record.Timestamp));
                        }

                        CheckResult(OUTPUT_TOPIC, updatedExpected);
                        expectedFinalResult = updatedExpected.get(expected.Count - 1);
                    }
                }

                if (storeName != null)
                {
                    CheckQueryableStore(storeName, expectedFinalResult);
                }
            }
            finally
            {
                streams.close();
            }
        }

        /*
         * Runs the actual test. Checks the result only after expected number of records have been consumed.
         */
        void RunTest(KeyValueTimestamp<long, string> expectedFinalResult)
        {// throws Exception
            RunTest(expectedFinalResult, null);
        }

        /*
         * Runs the actual test. Checks the result only after expected number of records have been consumed.
         */
        void RunTest(KeyValueTimestamp<long, string> expectedFinalResult, string storeName)
        {// throws Exception
            IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG);
            streams = new KafkaStreams(builder.build(), STREAMS_CONFIG);

            try
            {
                streams.start();

                long firstTimestamp = System.currentTimeMillis();
                long ts = firstTimestamp;

                foreach (Input<string> singleInput in input)
                {
                    producer.send(new ProducerRecord<>(singleInput.topic, null, ++ts, singleInput.record.key, singleInput.record.value)).get();
                }

                TestUtils.waitForCondition(() => finalResultReached.get(), "Never received expected result.");

                KeyValueTimestamp<long, string> updatedExpectedFinalResult =
                    new KeyValueTimestamp<>(
                        expectedFinalResult.Key,
                        expectedFinalResult.Value,
                        firstTimestamp + expectedFinalResult.Timestamp);
                CheckResult(OUTPUT_TOPIC, updatedExpectedFinalResult, numRecordsExpected);

                if (storeName != null)
                {
                    CheckQueryableStore(storeName, updatedExpectedFinalResult);
                }
            }
            finally
            {
                streams.close();
            }
        }

        /*
         * Checks the embedded queryable state store snapshot
         */
        private void CheckQueryableStore(string queryableName, KeyValueTimestamp<long, string> expectedFinalResult)
        {
            ReadOnlyKeyValueStore<long, ValueAndTimestamp<string>> store = streams.store(queryableName, QueryableStoreTypes.timestampedKeyValueStore());

            KeyValueIterator<long, ValueAndTimestamp<string>> all = store.all();
            KeyValuePair<long, ValueAndTimestamp<string>> onlyEntry = all.next();

            try
            {
                Assert.Equal(onlyEntry.key, expectedFinalResult.Key);
                Assert.Equal(onlyEntry.value.Value, (expectedFinalResult.Value));
                Assert.Equal(onlyEntry.value.Timestamp, (expectedFinalResult.Timestamp));
                Assert.Equal(all.hasNext(), (false));
            }
            finally
            {
                all.close();
            }
        }

        private class Input<V>
        {
            string topic;
            KeyValuePair<long, V> record;

            Input(string topic, V value)
            {
                this.topic = topic;
                record = KeyValuePair.Create(ANY_UNIQUE_KEY, value);
            }
        }
    }
}
