using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Topologies;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace Kafka.Streams.Tests.Integration
{
    public class RepartitionOptimizingIntegrationTest
    {

        private const int NUM_BROKERS = 1;
        private const string INPUT_TOPIC = "input";
        private const string COUNT_TOPIC = "outputTopic_0";
        private const string AGGREGATION_TOPIC = "outputTopic_1";
        private const string REDUCE_TOPIC = "outputTopic_2";
        private const string JOINED_TOPIC = "joinedOutputTopic";

        private const int ONE_REPARTITION_TOPIC = 1;
        private const int FOUR_REPARTITION_TOPICS = 4;

        private Regex repartitionTopicPattern = new Regex("Sink: .*-repartition", RegexOptions.Compiled);

        private StreamsConfig streamsConfiguration;

        //public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
        private MockTime mockTime = null;// CLUSTER.time;

        public void SetUp()
        {// throws Exception
            StreamsConfig props = new StreamsConfig();
            props.Set(StreamsConfig.CacheMaxBytesBuffering, (1024 * 10).ToString());
            props.Set(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000.ToString());

            streamsConfiguration = StreamsTestUtils.getStreamsConfig(
                "maybe-optimized-test-app",
                //CLUSTER.bootstrapServers(),
                Serdes.String().GetType().FullName,
                Serdes.String().GetType().FullName,
                props);

            //CLUSTER.createTopics(INPUT_TOPIC,
            //                     COUNT_TOPIC,
            //                     AGGREGATION_TOPIC,
            //                     REDUCE_TOPIC,
            //                     JOINED_TOPIC);

            IntegrationTestUtils.PurgeLocalStreamsState(streamsConfiguration);
        }


        public void TearDown()
        {// throws Exception
            CLUSTER.deleteAllTopicsAndWait(30_000L);
        }

        [Fact]
        public void ShouldSendCorrectRecords_OPTIMIZED()
        {// throws Exception
            runIntegrationTest(StreamsConfig.OPTIMIZE,
                               ONE_REPARTITION_TOPIC);
        }

        [Fact]
        public void ShouldSendCorrectResults_NO_OPTIMIZATION()
        {// throws Exception
            runIntegrationTest(StreamsConfig.NoOptimization, FOUR_REPARTITION_TOPICS);
        }


        private void RunIntegrationTest(string optimizationConfig,
                                        int expectedNumberRepartitionTopics)
        {// throws Exception

            Initializer<int> initializer = new Initializer<int>(() => 0);
            var aggregator = new Aggregator<string, string, int>((k, v, agg) => agg + v.Length);

            IReducer<string> reducer = new Reducer<string>((v1, v2) => v1 + ":" + v2);

            List<string> processorValueCollector = new List<string>();

            StreamsBuilder builder = new StreamsBuilder();

            IKStream<K, V> sourceStream = builder.Stream(INPUT_TOPIC, Consumed.With(Serdes.String(), Serdes.String()));

            IKStream<K, V> KeyValuePair.Create(k.ToUpper(CultureInfo.CurrentCulture), v));

            mappedStream.Filter((k, v) => k.Equals("B")).MapValues<string>(v => v.ToUpper(CultureInfo.CurrentCulture))
                .Process(() => new SimpleProcessor(processorValueCollector));

            IKStream<K, V> countStream = mappedStream.GroupByKey().Count(Materialized.With(Serdes.String(), Serdes.Long())).ToStream();

            countStream.To(COUNT_TOPIC, Produced.With(Serdes.String(), Serdes.Long()));

            mappedStream.GroupByKey().Aggregate(initializer,
                                                aggregator,
                                                Materialized.With(Serdes.String(), Serdes.Int()))
                .ToStream().To(AGGREGATION_TOPIC, Produced.With(Serdes.String(), Serdes.Int()));

            // adding operators for case where the repartition node is further downstream
            mappedStream.Filter((k, v) => true).Peek((k, v) => System.Console.Out.WriteLine(k + ":" + v)).GroupByKey()
                .Reduce(reducer, Materialized.With(Serdes.String(), Serdes.String()))
                .ToStream().To(REDUCE_TOPIC, Produced.With(Serdes.String(), Serdes.String()));

            mappedStream.Filter((k, v) => k.Equals("A"))
                .Join(countStream, (v1, v2) => v1 + ":" + v2.ToString(),
                      JoinWindows.Of(TimeSpan.FromMilliseconds(5000)),
                      Joined.With(Serdes.String(), Serdes.String(), Serdes.Long()))
                .To(JOINED_TOPIC);

            streamsConfiguration.Set(StreamsConfig.TOPOLOGY_OPTIMIZATION, optimizationConfig);

            StreamsConfig producerConfig = TestUtils.ProducerConfig(CLUSTER.bootstrapServers(), Serdes.String().Serializer, Serdes.String().Serializer);

            IntegrationTestUtils.ProduceKeyValuesSynchronously(INPUT_TOPIC, GetKeyValues(), producerConfig, mockTime);

            StreamsConfig consumerConfig1 = TestUtils.ConsumerConfig(CLUSTER.bootstrapServers(), Serdes.String().Deserializer, LongDeserializer);
            StreamsConfig consumerConfig2 = TestUtils.ConsumerConfig(CLUSTER.bootstrapServers(), Serdes.String().Deserializer, IntegerDeserializer);
            StreamsConfig consumerConfig3 = TestUtils.ConsumerConfig(CLUSTER.bootstrapServers(), Serdes.String().Deserializer, Serdes.String());

            Topology topology = builder.Build(streamsConfiguration);
            string topologyString = topology.Describe().ToString();

            if (optimizationConfig.Equals(StreamsConfig.OPTIMIZE))
            {
                Assert.Equal(EXPECTED_OPTIMIZED_TOPOLOGY, topologyString);
            }
            else
            {
                Assert.Equal(EXPECTED_UNOPTIMIZED_TOPOLOGY, topologyString);
            }

            /*
               confirming number of expected repartition topics here
             */
            Assert.Equal(expectedNumberRepartitionTopics, GetCountOfRepartitionTopicsFound(topologyString));

            KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
            streams.start();

            var expectedCountKeyValues = new List<KeyValuePair<string, long>> { KeyValuePair.Create("A", 3L), KeyValuePair.Create("B", 3L), KeyValuePair.Create("C", 3L) };
            IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig1, COUNT_TOPIC, expectedCountKeyValues);

            var expectedAggKeyValues = new List<KeyValuePair<string, int>> { KeyValuePair.Create("A", 9), KeyValuePair.Create("B", 9), KeyValuePair.Create("C", 9) };
            IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig2, AGGREGATION_TOPIC, expectedAggKeyValues);

            var expectedReduceKeyValues = new List<KeyValuePair<string, string>> { KeyValuePair.Create("A", "foo:bar:baz"), KeyValuePair.Create("B", "foo:bar:baz"), KeyValuePair.Create("C", "foo:bar:baz") };
            IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig3, REDUCE_TOPIC, expectedReduceKeyValues);

            List<KeyValuePair<string, string>> expectedJoinKeyValues = new List<KeyValuePair<string, string>> { KeyValuePair.Create("A", "foo:3"), KeyValuePair.Create("A", "bar:3"), KeyValuePair.Create("A", "baz:3") };
            IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig3, JOINED_TOPIC, expectedJoinKeyValues);


            List<string> expectedCollectedProcessorValues = new List<string> { "FOO", "BAR", "BAZ" };

            Assert.Equal(3, processorValueCollector.Count);
            Assert.Equal(processorValueCollector, expectedCollectedProcessorValues);

            streams.Close(FromSeconds(5));
        }

        private int GetCountOfRepartitionTopicsFound(string topologyString)
        {
            var matches = repartitionTopicPattern.Matches(topologyString);
            List<string> repartitionTopicsFound = new List<string>();
            foreach (var match in matches.AsEnumerable())
            {
                repartitionTopicsFound.Add(match.Value);
            }

            return repartitionTopicsFound.Count;
        }


        private List<KeyValuePair<string, string>> GetKeyValues()
        {
            List<KeyValuePair<string, string>> keyValueList = new List<KeyValuePair<string, string>>();
            string[] keys = new string[] { "a", "b", "c" };
            string[] values = new string[] { "foo", "bar", "baz" };
            foreach (string key in keys)
            {
                foreach (string value in values)
                {
                    keyValueList.Add(KeyValuePair.Create(key, value));
                }
            }
            return keyValueList;
        }

        private class SimpleProcessor : AbstractProcessor<string, string>
        {
            List<string> valueList;

            public SimpleProcessor(List<string> valueList)
            {
                this.valueList = valueList;
            }

            public override void Process(string key, string value)
            {
                valueList.Add(value);
            }
        }

        private const string EXPECTED_OPTIMIZED_TOPOLOGY = "Topologies:\n"
                                                                  + "   Sub-topology: 0\n"
                                                                  + "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n"
                                                                  + "      -=> KSTREAM-MAP-0000000001\n"
                                                                  + "    Processor: KSTREAM-MAP-0000000001 (stores: [])\n"
                                                                  + "      -=> KSTREAM-FILTER-0000000002, KSTREAM-FILTER-0000000040\n"
                                                                  + "      <-- KSTREAM-SOURCE-0000000000\n"
                                                                  + "    Processor: KSTREAM-FILTER-0000000002 (stores: [])\n"
                                                                  + "      -=> KSTREAM-MAPVALUES-0000000003\n"
                                                                  + "      <-- KSTREAM-MAP-0000000001\n"
                                                                  + "    Processor: KSTREAM-FILTER-0000000040 (stores: [])\n"
                                                                  + "      -=> KSTREAM-SINK-0000000039\n"
                                                                  + "      <-- KSTREAM-MAP-0000000001\n"
                                                                  + "    Processor: KSTREAM-MAPVALUES-0000000003 (stores: [])\n"
                                                                  + "      -=> KSTREAM-PROCESSOR-0000000004\n"
                                                                  + "      <-- KSTREAM-FILTER-0000000002\n"
                                                                  + "    Processor: KSTREAM-PROCESSOR-0000000004 (stores: [])\n"
                                                                  + "      -=> none\n"
                                                                  + "      <-- KSTREAM-MAPVALUES-0000000003\n"
                                                                  + "    Sink: KSTREAM-SINK-0000000039 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000006-repartition)\n"
                                                                  + "      <-- KSTREAM-FILTER-0000000040\n"
                                                                  + "\n"
                                                                  + "  Sub-topology: 1\n"
                                                                  + "    Source: KSTREAM-SOURCE-0000000041 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000006-repartition])\n"
                                                                  + "      -=> KSTREAM-FILTER-0000000020, KSTREAM-AGGREGATE-0000000007, KSTREAM-AGGREGATE-0000000014, KSTREAM-FILTER-0000000029\n"
                                                                  + "    Processor: KSTREAM-AGGREGATE-0000000007 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000006])\n"
                                                                  + "      -=> KTABLE-TOSTREAM-0000000011\n"
                                                                  + "      <-- KSTREAM-SOURCE-0000000041\n"
                                                                  + "    Processor: KTABLE-TOSTREAM-0000000011 (stores: [])\n"
                                                                  + "      -=> KSTREAM-SINK-0000000012, KSTREAM-WINDOWED-0000000034\n"
                                                                  + "      <-- KSTREAM-AGGREGATE-0000000007\n"
                                                                  + "    Processor: KSTREAM-FILTER-0000000020 (stores: [])\n"
                                                                  + "      -=> KSTREAM-PEEK-0000000021\n"
                                                                  + "      <-- KSTREAM-SOURCE-0000000041\n"
                                                                  + "    Processor: KSTREAM-FILTER-0000000029 (stores: [])\n"
                                                                  + "      -=> KSTREAM-WINDOWED-0000000033\n"
                                                                  + "      <-- KSTREAM-SOURCE-0000000041\n"
                                                                  + "    Processor: KSTREAM-PEEK-0000000021 (stores: [])\n"
                                                                  + "      -=> KSTREAM-REDUCE-0000000023\n"
                                                                  + "      <-- KSTREAM-FILTER-0000000020\n"
                                                                  + "    Processor: KSTREAM-WINDOWED-0000000033 (stores: [KSTREAM-JOINTHIS-0000000035-store])\n"
                                                                  + "      -=> KSTREAM-JOINTHIS-0000000035\n"
                                                                  + "      <-- KSTREAM-FILTER-0000000029\n"
                                                                  + "    Processor: KSTREAM-WINDOWED-0000000034 (stores: [KSTREAM-JOINOTHER-0000000036-store])\n"
                                                                  + "      -=> KSTREAM-JOINOTHER-0000000036\n"
                                                                  + "      <-- KTABLE-TOSTREAM-0000000011\n"
                                                                  + "    Processor: KSTREAM-AGGREGATE-0000000014 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000013])\n"
                                                                  + "      -=> KTABLE-TOSTREAM-0000000018\n"
                                                                  + "      <-- KSTREAM-SOURCE-0000000041\n"
                                                                  + "    Processor: KSTREAM-JOINOTHER-0000000036 (stores: [KSTREAM-JOINTHIS-0000000035-store])\n"
                                                                  + "      -=> KSTREAM-MERGE-0000000037\n"
                                                                  + "      <-- KSTREAM-WINDOWED-0000000034\n"
                                                                  + "    Processor: KSTREAM-JOINTHIS-0000000035 (stores: [KSTREAM-JOINOTHER-0000000036-store])\n"
                                                                  + "      -=> KSTREAM-MERGE-0000000037\n"
                                                                  + "      <-- KSTREAM-WINDOWED-0000000033\n"
                                                                  + "    Processor: KSTREAM-REDUCE-0000000023 (stores: [KSTREAM-REDUCE-STATE-STORE-0000000022])\n"
                                                                  + "      -=> KTABLE-TOSTREAM-0000000027\n"
                                                                  + "      <-- KSTREAM-PEEK-0000000021\n"
                                                                  + "    Processor: KSTREAM-MERGE-0000000037 (stores: [])\n"
                                                                  + "      -=> KSTREAM-SINK-0000000038\n"
                                                                  + "      <-- KSTREAM-JOINTHIS-0000000035, KSTREAM-JOINOTHER-0000000036\n"
                                                                  + "    Processor: KTABLE-TOSTREAM-0000000018 (stores: [])\n"
                                                                  + "      -=> KSTREAM-SINK-0000000019\n"
                                                                  + "      <-- KSTREAM-AGGREGATE-0000000014\n"
                                                                  + "    Processor: KTABLE-TOSTREAM-0000000027 (stores: [])\n"
                                                                  + "      -=> KSTREAM-SINK-0000000028\n"
                                                                  + "      <-- KSTREAM-REDUCE-0000000023\n"
                                                                  + "    Sink: KSTREAM-SINK-0000000012 (topic: outputTopic_0)\n"
                                                                  + "      <-- KTABLE-TOSTREAM-0000000011\n"
                                                                  + "    Sink: KSTREAM-SINK-0000000019 (topic: outputTopic_1)\n"
                                                                  + "      <-- KTABLE-TOSTREAM-0000000018\n"
                                                                  + "    Sink: KSTREAM-SINK-0000000028 (topic: outputTopic_2)\n"
                                                                  + "      <-- KTABLE-TOSTREAM-0000000027\n"
                                                                  + "    Sink: KSTREAM-SINK-0000000038 (topic: joinedOutputTopic)\n"
                                                                  + "      <-- KSTREAM-MERGE-0000000037\n\n";


        private const string EXPECTED_UNOPTIMIZED_TOPOLOGY = "Topologies:\n"
                                                                    + "   Sub-topology: 0\n"
                                                                    + "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n"
                                                                    + "      -=> KSTREAM-MAP-0000000001\n"
                                                                    + "    Processor: KSTREAM-MAP-0000000001 (stores: [])\n"
                                                                    + "      -=> KSTREAM-FILTER-0000000020, KSTREAM-FILTER-0000000002, KSTREAM-FILTER-0000000009, KSTREAM-FILTER-0000000016, KSTREAM-FILTER-0000000029\n"
                                                                    + "      <-- KSTREAM-SOURCE-0000000000\n"
                                                                    + "    Processor: KSTREAM-FILTER-0000000020 (stores: [])\n"
                                                                    + "      -=> KSTREAM-PEEK-0000000021\n"
                                                                    + "      <-- KSTREAM-MAP-0000000001\n"
                                                                    + "    Processor: KSTREAM-FILTER-0000000002 (stores: [])\n"
                                                                    + "      -=> KSTREAM-MAPVALUES-0000000003\n"
                                                                    + "      <-- KSTREAM-MAP-0000000001\n"
                                                                    + "    Processor: KSTREAM-FILTER-0000000029 (stores: [])\n"
                                                                    + "      -=> KSTREAM-FILTER-0000000031\n"
                                                                    + "      <-- KSTREAM-MAP-0000000001\n"
                                                                    + "    Processor: KSTREAM-PEEK-0000000021 (stores: [])\n"
                                                                    + "      -=> KSTREAM-FILTER-0000000025\n"
                                                                    + "      <-- KSTREAM-FILTER-0000000020\n"
                                                                    + "    Processor: KSTREAM-FILTER-0000000009 (stores: [])\n"
                                                                    + "      -=> KSTREAM-SINK-0000000008\n"
                                                                    + "      <-- KSTREAM-MAP-0000000001\n"
                                                                    + "    Processor: KSTREAM-FILTER-0000000016 (stores: [])\n"
                                                                    + "      -=> KSTREAM-SINK-0000000015\n"
                                                                    + "      <-- KSTREAM-MAP-0000000001\n"
                                                                    + "    Processor: KSTREAM-FILTER-0000000025 (stores: [])\n"
                                                                    + "      -=> KSTREAM-SINK-0000000024\n"
                                                                    + "      <-- KSTREAM-PEEK-0000000021\n"
                                                                    + "    Processor: KSTREAM-FILTER-0000000031 (stores: [])\n"
                                                                    + "      -=> KSTREAM-SINK-0000000030\n"
                                                                    + "      <-- KSTREAM-FILTER-0000000029\n"
                                                                    + "    Processor: KSTREAM-MAPVALUES-0000000003 (stores: [])\n"
                                                                    + "      -=> KSTREAM-PROCESSOR-0000000004\n"
                                                                    + "      <-- KSTREAM-FILTER-0000000002\n"
                                                                    + "    Processor: KSTREAM-PROCESSOR-0000000004 (stores: [])\n"
                                                                    + "      -=> none\n"
                                                                    + "      <-- KSTREAM-MAPVALUES-0000000003\n"
                                                                    + "    Sink: KSTREAM-SINK-0000000008 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000006-repartition)\n"
                                                                    + "      <-- KSTREAM-FILTER-0000000009\n"
                                                                    + "    Sink: KSTREAM-SINK-0000000015 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000013-repartition)\n"
                                                                    + "      <-- KSTREAM-FILTER-0000000016\n"
                                                                    + "    Sink: KSTREAM-SINK-0000000024 (topic: KSTREAM-REDUCE-STATE-STORE-0000000022-repartition)\n"
                                                                    + "      <-- KSTREAM-FILTER-0000000025\n"
                                                                    + "    Sink: KSTREAM-SINK-0000000030 (topic: KSTREAM-FILTER-0000000029-repartition)\n"
                                                                    + "      <-- KSTREAM-FILTER-0000000031\n"
                                                                    + "\n"
                                                                    + "  Sub-topology: 1\n"
                                                                    + "    Source: KSTREAM-SOURCE-0000000010 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000006-repartition])\n"
                                                                    + "      -=> KSTREAM-AGGREGATE-0000000007\n"
                                                                    + "    Processor: KSTREAM-AGGREGATE-0000000007 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000006])\n"
                                                                    + "      -=> KTABLE-TOSTREAM-0000000011\n"
                                                                    + "      <-- KSTREAM-SOURCE-0000000010\n"
                                                                    + "    Processor: KTABLE-TOSTREAM-0000000011 (stores: [])\n"
                                                                    + "      -=> KSTREAM-SINK-0000000012, KSTREAM-WINDOWED-0000000034\n"
                                                                    + "      <-- KSTREAM-AGGREGATE-0000000007\n"
                                                                    + "    Source: KSTREAM-SOURCE-0000000032 (topics: [KSTREAM-FILTER-0000000029-repartition])\n"
                                                                    + "      -=> KSTREAM-WINDOWED-0000000033\n"
                                                                    + "    Processor: KSTREAM-WINDOWED-0000000033 (stores: [KSTREAM-JOINTHIS-0000000035-store])\n"
                                                                    + "      -=> KSTREAM-JOINTHIS-0000000035\n"
                                                                    + "      <-- KSTREAM-SOURCE-0000000032\n"
                                                                    + "    Processor: KSTREAM-WINDOWED-0000000034 (stores: [KSTREAM-JOINOTHER-0000000036-store])\n"
                                                                    + "      -=> KSTREAM-JOINOTHER-0000000036\n"
                                                                    + "      <-- KTABLE-TOSTREAM-0000000011\n"
                                                                    + "    Processor: KSTREAM-JOINOTHER-0000000036 (stores: [KSTREAM-JOINTHIS-0000000035-store])\n"
                                                                    + "      -=> KSTREAM-MERGE-0000000037\n"
                                                                    + "      <-- KSTREAM-WINDOWED-0000000034\n"
                                                                    + "    Processor: KSTREAM-JOINTHIS-0000000035 (stores: [KSTREAM-JOINOTHER-0000000036-store])\n"
                                                                    + "      -=> KSTREAM-MERGE-0000000037\n"
                                                                    + "      <-- KSTREAM-WINDOWED-0000000033\n"
                                                                    + "    Processor: KSTREAM-MERGE-0000000037 (stores: [])\n"
                                                                    + "      -=> KSTREAM-SINK-0000000038\n"
                                                                    + "      <-- KSTREAM-JOINTHIS-0000000035, KSTREAM-JOINOTHER-0000000036\n"
                                                                    + "    Sink: KSTREAM-SINK-0000000012 (topic: outputTopic_0)\n"
                                                                    + "      <-- KTABLE-TOSTREAM-0000000011\n"
                                                                    + "    Sink: KSTREAM-SINK-0000000038 (topic: joinedOutputTopic)\n"
                                                                    + "      <-- KSTREAM-MERGE-0000000037\n"
                                                                    + "\n"
                                                                    + "  Sub-topology: 2\n"
                                                                    + "    Source: KSTREAM-SOURCE-0000000017 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000013-repartition])\n"
                                                                    + "      -=> KSTREAM-AGGREGATE-0000000014\n"
                                                                    + "    Processor: KSTREAM-AGGREGATE-0000000014 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000013])\n"
                                                                    + "      -=> KTABLE-TOSTREAM-0000000018\n"
                                                                    + "      <-- KSTREAM-SOURCE-0000000017\n"
                                                                    + "    Processor: KTABLE-TOSTREAM-0000000018 (stores: [])\n"
                                                                    + "      -=> KSTREAM-SINK-0000000019\n"
                                                                    + "      <-- KSTREAM-AGGREGATE-0000000014\n"
                                                                    + "    Sink: KSTREAM-SINK-0000000019 (topic: outputTopic_1)\n"
                                                                    + "      <-- KTABLE-TOSTREAM-0000000018\n"
                                                                    + "\n"
                                                                    + "  Sub-topology: 3\n"
                                                                    + "    Source: KSTREAM-SOURCE-0000000026 (topics: [KSTREAM-REDUCE-STATE-STORE-0000000022-repartition])\n"
                                                                    + "      -=> KSTREAM-REDUCE-0000000023\n"
                                                                    + "    Processor: KSTREAM-REDUCE-0000000023 (stores: [KSTREAM-REDUCE-STATE-STORE-0000000022])\n"
                                                                    + "      -=> KTABLE-TOSTREAM-0000000027\n"
                                                                    + "      <-- KSTREAM-SOURCE-0000000026\n"
                                                                    + "    Processor: KTABLE-TOSTREAM-0000000027 (stores: [])\n"
                                                                    + "      -=> KSTREAM-SINK-0000000028\n"
                                                                    + "      <-- KSTREAM-REDUCE-0000000023\n"
                                                                    + "    Sink: KSTREAM-SINK-0000000028 (topic: outputTopic_2)\n"
                                                                    + "      <-- KTABLE-TOSTREAM-0000000027\n\n";

    }
}
