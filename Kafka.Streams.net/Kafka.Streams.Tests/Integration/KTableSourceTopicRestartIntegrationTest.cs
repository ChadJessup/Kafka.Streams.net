///*






// *

// *





// */






































//using System.Collections.Generic;

//namespace Kafka.Streams.Tests.Integration
//{
//    public class KTableSourceTopicRestartIntegrationTest
//    {
//        private const int NUM_BROKERS = 3;
//        private const string SOURCE_TOPIC = "source-topic";


//        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
//        private Time time = CLUSTER.time;
//        private KafkaStreams streamsOne;
//        private StreamsBuilder streamsBuilder = new StreamsBuilder();
//        private Dictionary<string, string> readKeyValues = new ConcurrentHashMap<>();

//        private static StreamsConfig PRODUCER_CONFIG = new StreamsConfig();
//        private static StreamsConfig STREAMS_CONFIG = new StreamsConfig();
//        private Dictionary<string, string> expectedInitialResultsMap;
//        private Dictionary<string, string> expectedResultsWithDataWrittenDuringRestoreMap;


//        public static void SetUpBeforeAllTests()
//        {// throws Exception
//            CLUSTER.createTopic(SOURCE_TOPIC);

//            STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-restore-from-source");
//            STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//            STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().GetType().FullName);
//            STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().GetType().FullName);
//            STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath());
//            STREAMS_CONFIG.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//            STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5);
//            STREAMS_CONFIG.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor);

//            PRODUCER_CONFIG.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//            PRODUCER_CONFIG.put(ProducerConfig.ACKS_CONFIG, "all");
//            PRODUCER_CONFIG.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);
//            PRODUCER_CONFIG.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);

//        }


//        public void Before()
//        {
//            KTable<string, string> kTable = streamsBuilder.table(SOURCE_TOPIC, Materialized.As("store"));
//            kTable.toStream().ForEach();

//            expectedInitialResultsMap = createExpectedResultsMap("a", "b", "c");
//            expectedResultsWithDataWrittenDuringRestoreMap = createExpectedResultsMap("a", "b", "c", "d", "f", "g", "h");
//        }


//        public void After()
//        {// throws Exception
//            IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG);
//        }

//        [Fact]
//        public void ShouldRestoreAndProgressWhenTopicWrittenToDuringRestorationWithEosDisabled()
//        {// throws Exception
//            try
//            {
//                streamsOne = new KafkaStreams(streamsBuilder.Build(), STREAMS_CONFIG);
//                streamsOne.start();

//                produceKeyValues("a", "b", "c");

//                AssertNumberValuesRead(readKeyValues, expectedInitialResultsMap, "Table did not read all values");

//                streamsOne.close();
//                streamsOne = new KafkaStreams(streamsBuilder.Build(), STREAMS_CONFIG);
//                // the state restore listener will append one record to the log
//                streamsOne.setGlobalStateRestoreListener(new UpdatingSourceTopicOnRestoreStartStateRestoreListener());
//                streamsOne.start();

//                produceKeyValues("f", "g", "h");

//                AssertNumberValuesRead(
//                    readKeyValues,
//                    expectedResultsWithDataWrittenDuringRestoreMap,
//                    "Table did not get all values after restart");
//            }
//            finally
//            {
//                streamsOne.close(Duration.ofSeconds(5));
//            }
//        }

//        [Fact]
//        public void ShouldRestoreAndProgressWhenTopicWrittenToDuringRestorationWithEosEnabled()
//        {// throws Exception
//            try
//            {
//                STREAMS_CONFIG.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
//                streamsOne = new KafkaStreams(streamsBuilder.Build(), STREAMS_CONFIG);
//                streamsOne.start();

//                produceKeyValues("a", "b", "c");

//                AssertNumberValuesRead(readKeyValues, expectedInitialResultsMap, "Table did not read all values");

//                streamsOne.close();
//                streamsOne = new KafkaStreams(streamsBuilder.Build(), STREAMS_CONFIG);
//                // the state restore listener will append one record to the log
//                streamsOne.setGlobalStateRestoreListener(new UpdatingSourceTopicOnRestoreStartStateRestoreListener());
//                streamsOne.start();

//                produceKeyValues("f", "g", "h");

//                AssertNumberValuesRead(
//                    readKeyValues,
//                    expectedResultsWithDataWrittenDuringRestoreMap,
//                    "Table did not get all values after restart");
//            }
//            finally
//            {
//                streamsOne.close(Duration.ofSeconds(5));
//            }
//        }

//        [Fact]
//        public void ShouldRestoreAndProgressWhenTopicNotWrittenToDuringRestoration()
//        {// throws Exception
//            try
//            {
//                streamsOne = new KafkaStreams(streamsBuilder.Build(), STREAMS_CONFIG);
//                streamsOne.start();

//                produceKeyValues("a", "b", "c");

//                AssertNumberValuesRead(readKeyValues, expectedInitialResultsMap, "Table did not read all values");

//                streamsOne.close();
//                streamsOne = new KafkaStreams(streamsBuilder.Build(), STREAMS_CONFIG);
//                streamsOne.start();

//                produceKeyValues("f", "g", "h");

//                Dictionary<string, string> expectedValues = createExpectedResultsMap("a", "b", "c", "f", "g", "h");

//                AssertNumberValuesRead(readKeyValues, expectedValues, "Table did not get all values after restart");
//            }
//            finally
//            {
//                streamsOne.close(Duration.ofSeconds(5));
//            }
//        }

//        private void AssertNumberValuesRead(Dictionary<string, string> valueMap,
//                                            Dictionary<string, string> expectedMap,
//                                            string errorMessage)
//        {// throws InterruptedException
//            TestUtils.WaitForCondition(
//                () => valueMap.equals(expectedMap),
//                30 * 1000L,
//                errorMessage);
//        }

//        private void ProduceKeyValues(string... keys)
//        {// throws ExecutionException, InterruptedException
//            List<KeyValuePair<string, string>> keyValueList = new ArrayList<>();

//            foreach (string key in keys)
//            {
//                keyValueList.Add(KeyValuePair.Create(key, key + "1"));
//            }

//            IntegrationTestUtils.produceKeyValuesSynchronously(SOURCE_TOPIC,
//                                                               keyValueList,
//                                                               PRODUCER_CONFIG,
//                                                               time);
//        }

//        private Dictionary<string, string> CreateExpectedResultsMap(string... keys)
//        {
//            Dictionary<string, string> expectedMap = new HashMap<>();
//            foreach (string key in keys)
//            {
//                expectedMap.put(key, key + "1");
//            }
//            return expectedMap;
//        }

//        private class UpdatingSourceTopicOnRestoreStartStateRestoreListener : StateRestoreListener
//        {


//            public void OnRestoreStart(TopicPartition topicPartition,
//                                       string storeName,
//                                       long startingOffset,
//                                       long endingOffset)
//            {
//                try
//                {
//                    produceKeyValues("d");
//                }
//                catch (ExecutionException | InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//                }


//                public void onBatchRestored(TopicPartition topicPartition,
//                                            string storeName,
//                                            long batchEndOffset,
//                                            long numRestored)
//                {
//                }


//                public void onRestoreEnd(TopicPartition topicPartition,
//                                         string storeName,
//                                         long totalRestored)
//                {
//                }
//            }

//        }
//}