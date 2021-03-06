//using Confluent.Kafka;
//using Kafka.Common;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.State.Interfaces;
//using Kafka.Streams.Tests.Helpers;
//using Kafka.Streams.Threads.KafkaStreams;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Integration
//{
//    public class KTableSourceTopicRestartIntegrationTest
//    {
//        private const int NUM_BROKERS = 3;
//        private const string SOURCE_TOPIC = "source-topic";


//        public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
//        private IClock time = CLUSTER.time;
//        private IKafkaStreamsThread streamsOne;
//        private StreamsBuilder streamsBuilder = new StreamsBuilder();
//        private Dictionary<string, string> readKeyValues = new Dictionary<string, string>();

//        private static StreamsConfig PRODUCER_CONFIG = new StreamsConfig();
//        private static StreamsConfig STREAMS_CONFIG = new StreamsConfig();
//        private Dictionary<string, string> expectedInitialResultsMap;
//        private Dictionary<string, string> expectedResultsWithDataWrittenDuringRestoreMap;


//        public static void SetUpBeforeAllTests()
//        {// throws Exception
//            CLUSTER.CreateTopic(SOURCE_TOPIC);

//            STREAMS_CONFIG.Set(StreamsConfig.ApplicationIdConfig, "ktable-restore-from-source");
//            STREAMS_CONFIG.Set(StreamsConfig.BootstrapServersConfig, CLUSTER.bootstrapServers());
//            STREAMS_CONFIG.Set(StreamsConfig.DefaultKeySerdeClassConfig, Serdes.String().GetType().FullName);
//            STREAMS_CONFIG.Set(StreamsConfig.DefaultValueSerdeClassConfig, Serdes.String().GetType().FullName);
//            STREAMS_CONFIG.Set(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath());
//            STREAMS_CONFIG.Set(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//            STREAMS_CONFIG.Set(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5);
//            STREAMS_CONFIG.Set(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor);

//            PRODUCER_CONFIG.Set(ProducerConfig.BootstrapServersConfig, CLUSTER.bootstrapServers());
//            PRODUCER_CONFIG.Set(ProducerConfig.ACKS_CONFIG, "All");
//            PRODUCER_CONFIG.Set(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);
//            PRODUCER_CONFIG.Set(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().Serializer);
//        }


//        public void Before()
//        {
//            IKTable<string, string> kTable = streamsBuilder.table(SOURCE_TOPIC, Materialized.As("store"));
//            kTable.ToStream().ForEach();

//            expectedInitialResultsMap = createExpectedResultsMap("a", "b", "c");
//            expectedResultsWithDataWrittenDuringRestoreMap = createExpectedResultsMap("a", "b", "c", "d", "f", "g", "h");
//        }

//        public void After()
//        {// throws Exception
//            IntegrationTestUtils.PurgeLocalStreamsState(STREAMS_CONFIG);
//        }

//        [Fact]
//        public void ShouldRestoreAndProgressWhenTopicWrittenToDuringRestorationWithEosDisabled()
//        {// throws Exception
//            try
//            {
//                streamsOne = new KafkaStreamsThread(streamsBuilder.Build(), STREAMS_CONFIG);
//                streamsOne.Start();

//                ProduceKeyValues("a", "b", "c");

//                AssertNumberValuesRead(readKeyValues, expectedInitialResultsMap, "Table did not read All values");

//                streamsOne.Close();
//                streamsOne = new KafkaStreamsThread(streamsBuilder.Build(), STREAMS_CONFIG);
//                // the state restore listener will append one record to the log
//                streamsOne.SetGlobalStateRestoreListener(new UpdatingSourceTopicOnRestoreStartStateRestoreListener());
//                streamsOne.Start();

//                ProduceKeyValues("f", "g", "h");

//                AssertNumberValuesRead(
//                    readKeyValues,
//                    expectedResultsWithDataWrittenDuringRestoreMap,
//                    "Table did not get All values after restart");
//            }
//            finally
//            {
//                streamsOne.Close(TimeSpan.FromSeconds(5));
//            }
//        }

//        [Fact]
//        public void ShouldRestoreAndProgressWhenTopicWrittenToDuringRestorationWithEosEnabled()
//        {// throws Exception
//            try
//            {
//                STREAMS_CONFIG.Put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.StreamsConfig.ExactlyOnceConfig);
//                streamsOne = new KafkaStreamsThread(streamsBuilder.Build(), STREAMS_CONFIG);
//                streamsOne.Start();

//                ProduceKeyValues("a", "b", "c");

//                AssertNumberValuesRead(readKeyValues, expectedInitialResultsMap, "Table did not read All values");

//                streamsOne.Close();
//                streamsOne = new KafkaStreamsThread(streamsBuilder.Build(), STREAMS_CONFIG);
//                // the state restore listener will append one record to the log
//                streamsOne.SetGlobalStateRestoreListener(new UpdatingSourceTopicOnRestoreStartStateRestoreListener());
//                streamsOne.Start();

//                ProduceKeyValues("f", "g", "h");

//                AssertNumberValuesRead(
//                    readKeyValues,
//                    expectedResultsWithDataWrittenDuringRestoreMap,
//                    "Table did not get All values after restart");
//            }
//            finally
//            {
//                streamsOne.Close(TimeSpan.FromSeconds(5));
//            }
//        }

//        [Fact]
//        public void ShouldRestoreAndProgressWhenTopicNotWrittenToDuringRestoration()
//        {// throws Exception
//            try
//            {
//                streamsOne = new KafkaStreamsThread(streamsBuilder.Build(), STREAMS_CONFIG);
//                streamsOne.Start();

//                ProduceKeyValues("a", "b", "c");

//                AssertNumberValuesRead(readKeyValues, expectedInitialResultsMap, "Table did not read All values");

//                streamsOne.Close();
//                streamsOne = new KafkaStreamsThread(streamsBuilder.Build(), STREAMS_CONFIG);
//                streamsOne.Start();

//                ProduceKeyValues("f", "g", "h");

//                Dictionary<string, string> expectedValues = createExpectedResultsMap("a", "b", "c", "f", "g", "h");

//                AssertNumberValuesRead(readKeyValues, expectedValues, "Table did not get All values after restart");
//            }
//            finally
//            {
//                streamsOne.Close(TimeSpan.FromSeconds(5));
//            }
//        }

//        private void AssertNumberValuesRead(
//            Dictionary<string, string> valueMap,
//            Dictionary<string, string> expectedMap,
//            string errorMessage)
//        {// throws InterruptedException
//            TestUtils.WaitForCondition(
//                () => valueMap.Equals(expectedMap),
//                30 * 1000L,
//                errorMessage);
//        }

//        private void ProduceKeyValues(params string[] keys)
//        {// throws ExecutionException, InterruptedException
//            List<KeyValuePair<string, string>> keyValueList = new List<KeyValuePair<string, string>>();

//            foreach (string key in keys)
//            {
//                keyValueList.Add(KeyValuePair.Create(key, key + "1"));
//            }

//            IntegrationTestUtils.ProduceKeyValuesSynchronously(
//                SOURCE_TOPIC,
//                keyValueList,
//                PRODUCER_CONFIG,
//                time);
//        }

//        private Dictionary<string, string> CreateExpectedResultsMap(params string[] keys)
//        {
//            var expectedMap = new Dictionary<string, string>();
//            foreach (string key in keys)
//            {
//                expectedMap.Put(key, key + "1");
//            }

//            return expectedMap;
//        }

//        private class UpdatingSourceTopicOnRestoreStartStateRestoreListener : IStateRestoreListener
//        {
//            public void OnRestoreStart(
//                TopicPartition topicPartition,
//                string storeName,
//                long startingOffset,
//                long endingOffset)
//            {
//                try
//                {
//                    ProduceKeyValues("d");
//                }
//                catch (Exception e) //(ExecutionException || InterruptedException e) {
//                {
//                    throw new RuntimeException("", e);
//                }
//            }

//            public void OnBatchRestored(
//                TopicPartition topicPartition,
//                string storeName,
//                long batchEndOffset,
//                long numRestored)
//            {
//            }

//            public void OnRestoreEnd(
//                TopicPartition topicPartition,
//                string storeName,
//                long totalRestored)
//            {
//            }
//        }
//    }
//}
