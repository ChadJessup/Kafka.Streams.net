namespace Kafka.Streams.Tests.Kstream
{
}
//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using NodaTime;

//namespace .kafka.streams.kstream
//{
























//    public class RepartitionTopicNamingTest
//    {

//        private IKeyValueMapper<string, string, string> kvMapper = (k, v) => k + v;
//        private static string INPUT_TOPIC = "input";
//        private static string COUNT_TOPIC = "outputTopic_0";
//        private static string AGGREGATION_TOPIC = "outputTopic_1";
//        private static string REDUCE_TOPIC = "outputTopic_2";
//        private static string JOINED_TOPIC = "outputTopicForJoin";

//        private string firstRepartitionTopicName = "count-stream";
//        private string secondRepartitionTopicName = "aggregate-stream";
//        private string thirdRepartitionTopicName = "reduced-stream";
//        private string fourthRepartitionTopicName = "joined-stream";
//        private Regex repartitionTopicPattern = new Regex("Sink: .*-repartition", RegexOptions.Compiled);


//        [Fact]
//        public void shouldReuseFirstRepartitionTopicNameWhenOptimizing()
//        {

//            string optimizedTopology = buildTopology(StreamsConfigPropertyNames.OPTIMIZE).describe().ToString();
//            string unOptimizedTopology = buildTopology(StreamsConfigPropertyNames.NO_OPTIMIZATION).describe().ToString();

//            Assert.Equal(optimizedTopology, (EXPECTED_OPTIMIZED_TOPOLOGY));
//            // only one repartition topic
//            Assert.Equal(1, (getCountOfRepartitionTopicsFound(optimizedTopology, repartitionTopicPattern)));
//            // the first named repartition topic
//            Assert.True(optimizedTopology.Contains(firstRepartitionTopicName + "-repartition"));


//            Assert.Equal(unOptimizedTopology, (EXPECTED_UNOPTIMIZED_TOPOLOGY));
//            // now 4 repartition topic
//            Assert.Equal(4, (getCountOfRepartitionTopicsFound(unOptimizedTopology, repartitionTopicPattern)));
//            // all 4 named repartition topics present
//            Assert.True(unOptimizedTopology.Contains(firstRepartitionTopicName + "-repartition"));
//            Assert.True(unOptimizedTopology.Contains(secondRepartitionTopicName + "-repartition"));
//            Assert.True(unOptimizedTopology.Contains(thirdRepartitionTopicName + "-repartition"));
//            Assert.True(unOptimizedTopology.Contains(fourthRepartitionTopicName + "-left-repartition"));

//        }

//        // can't use same repartition topic name
//        [Fact]
//        public void shouldFailWithSameRepartitionTopicName()
//        {
//            try
//            {
//                var builder = new StreamsBuilder();
//                builder.Stream<string, string>("topic").selectKey((k, v) => k)
//                                                .groupByKey(Grouped.As("grouping"))
//                                                .count().toStream();

//                builder.Stream<string, string>("topicII").selectKey((k, v) => k)
//                                                  .groupByKey(Grouped.As("grouping"))
//                                                  .count().toStream();
//                builder.Build();
//                Assert.False(true, "Should not build re-using repartition topic name");
//            }
//            catch (TopologyException te)
//            {
//                // ok
//            }
//        }

//        [Fact]
//        public void shouldNotFailWithSameRepartitionTopicNameUsingSameKGroupedStream()
//        {
//            var builder = new StreamsBuilder();
//            KGroupedStream<string, string> kGroupedStream = builder.Stream<string, string>("topic")
//                                                                         .selectKey((k, v) => k)
//                                                                         .groupByKey(Grouped.As("grouping"));

//            kGroupedStream.windowedBy(TimeWindows.of(Duration.FromMilliseconds(10L))).count().toStream().to("output-one");
//            kGroupedStream.windowedBy(TimeWindows.of(Duration.FromMilliseconds(30L))).count().toStream().to("output-two");

//            string topologyString = builder.Build().describe().ToString();
//            Assert.Equal(1, (getCountOfRepartitionTopicsFound(topologyString, repartitionTopicPattern)));
//            Assert.True(topologyString.Contains("grouping-repartition"));
//        }

//        [Fact]
//        public void shouldNotFailWithSameRepartitionTopicNameUsingSameTimeWindowStream()
//        {
//            var builder = new StreamsBuilder();
//            KGroupedStream<string, string> kGroupedStream = builder.Stream<string, string>("topic")
//                                                                         .selectKey((k, v) => k)
//                                                                         .groupByKey(Grouped.As("grouping"));

//            TimeWindowedKStream<string, string> timeWindowedKStream = kGroupedStream.windowedBy(TimeWindows.of(Duration.FromMilliseconds(10L)));

//            timeWindowedKStream.count().toStream().to("output-one");
//            timeWindowedKStream.reduce((v, v2) => v + v2).toStream().to("output-two");
//            kGroupedStream.windowedBy(TimeWindows.of(Duration.FromMilliseconds(30L))).count().toStream().to("output-two");

//            string topologyString = builder.Build().describe().ToString();
//            Assert.Equal(1, (getCountOfRepartitionTopicsFound(topologyString, repartitionTopicPattern)));
//            Assert.True(topologyString.Contains("grouping-repartition"));
//        }

//        [Fact]
//        public void shouldNotFailWithSameRepartitionTopicNameUsingSameSessionWindowStream()
//        {
//            var builder = new StreamsBuilder();
//            KGroupedStream<string, string> kGroupedStream = builder.Stream<string, string>("topic")
//                                                                         .selectKey((k, v) => k)
//                                                                         .groupByKey(Grouped.As("grouping"));

//            SessionWindowedKStream<string, string> sessionWindowedKStream = kGroupedStream.windowedBy(SessionWindows.with(Duration.FromMilliseconds(10L)));

//            sessionWindowedKStream.count().toStream().to("output-one");
//            sessionWindowedKStream.reduce((v, v2) => v + v2).toStream().to("output-two");
//            kGroupedStream.windowedBy(TimeWindows.of(Duration.FromMilliseconds(30L))).count().toStream().to("output-two");

//            string topologyString = builder.Build().describe().ToString();
//            Assert.Equal(1, (getCountOfRepartitionTopicsFound(topologyString, repartitionTopicPattern)));
//            Assert.True(topologyString.Contains("grouping-repartition"));
//        }

//        [Fact]
//        public void shouldNotFailWithSameRepartitionTopicNameUsingSameKGroupedTable()
//        {
//            var builder = new StreamsBuilder();
//            KGroupedTable<string, string> kGroupedTable = builder.< string, string> table("topic")
//                                                                        .groupBy(KeyValuePair::pair, Grouped.As("grouping"));
//            kGroupedTable.count().toStream().to("output-count");
//            kGroupedTable.reduce((v, v2) => v2, (v, v2) => v2).toStream().to("output-reduce");
//            string topologyString = builder.Build().describe().ToString();
//            Assert.Equal(1, (getCountOfRepartitionTopicsFound(topologyString, repartitionTopicPattern)));
//            Assert.True(topologyString.Contains("grouping-repartition"));
//        }

//        [Fact]
//        public void shouldNotReuseRepartitionNodeWithUnamedRepartitionTopics()
//        {
//            var builder = new StreamsBuilder();
//            KGroupedStream<string, string> kGroupedStream = builder.Stream<string, string>("topic")
//                                                                         .selectKey((k, v) => k)
//                                                                         .groupByKey();
//            kGroupedStream.windowedBy(TimeWindows.of(Duration.FromMilliseconds(10L))).count().toStream().to("output-one");
//            kGroupedStream.windowedBy(TimeWindows.of(Duration.FromMilliseconds(30L))).count().toStream().to("output-two");
//            string topologyString = builder.Build().describe().ToString();
//            Assert.Equal(2, (getCountOfRepartitionTopicsFound(topologyString, repartitionTopicPattern)));
//        }

//        [Fact]
//        public void shouldNotReuseRepartitionNodeWithUnamedRepartitionTopicsKGroupedTable()
//        {
//            var builder = new StreamsBuilder();
//            KGroupedTable<string, string> kGroupedTable = builder.< string, string> table("topic").groupBy(KeyValuePair::pair);
//            kGroupedTable.count().toStream().to("output-count");
//            kGroupedTable.reduce((v, v2) => v2, (v, v2) => v2).toStream().to("output-reduce");
//            string topologyString = builder.Build().describe().ToString();
//            Assert.Equal(2, (getCountOfRepartitionTopicsFound(topologyString, repartitionTopicPattern)));
//        }

//        [Fact]
//        public void shouldNotFailWithSameRepartitionTopicNameUsingSameKGroupedStreamOptimizationsOn()
//        {
//            var builder = new StreamsBuilder();
//            KGroupedStream<string, string> kGroupedStream = builder.Stream<string, string>("topic")
//                                                                         .selectKey((k, v) => k)
//                                                                         .groupByKey(Grouped.As("grouping"));
//            kGroupedStream.windowedBy(TimeWindows.of(Duration.FromMilliseconds(10L))).count();
//            kGroupedStream.windowedBy(TimeWindows.of(Duration.FromMilliseconds(30L))).count();
//            var properties = new StreamsConfig();
//            properties.put(StreamsConfigPropertyNames.TOPOLOGY_OPTIMIZATION, StreamsConfigPropertyNames.OPTIMIZE);
//            Topology topology = builder.Build(properties);
//            Assert.Equal(getCountOfRepartitionTopicsFound(topology.describe().ToString(), repartitionTopicPattern), (1));
//        }


//        // can't use same repartition topic name in joins
//        [Fact]
//        public void shouldFailWithSameRepartitionTopicNameInJoin()
//        {
//            try
//            {
//                var builder = new StreamsBuilder();
//                IKStream<string, string> stream1 = builder.Stream<string, string>("topic").selectKey((k, v) => k);
//                IKStream<string, string> stream2 = builder.Stream<string, string>("topic2").selectKey((k, v) => k);
//                IKStream<string, string> stream3 = builder.Stream<string, string>("topic3").selectKey((k, v) => k);

//                IKStream<string, string> joined = stream1.join(stream2, (v1, v2) => v1 + v2,
//                                                                    JoinWindows.of(Duration.FromMilliseconds(30L)),
//                                                                    Joined.named("join-repartition"));

//                joined.join(stream3, (v1, v2) => v1 + v2, JoinWindows.of(Duration.FromMilliseconds(30L)),
//                                                          Joined.named("join-repartition"));
//                builder.Build();
//                Assert.False(true, "Should not build re-using repartition topic name");
//            }
//            catch (TopologyException te)
//            {
//                // ok
//            }
//        }

//        [Fact]
//        public void shouldPassWithSameRepartitionTopicNameUsingSameKGroupedStreamOptimized()
//        {
//            var builder = new StreamsBuilder();
//            var properties = new StreamsConfig();
//            properties.put(StreamsConfigPropertyNames.TOPOLOGY_OPTIMIZATION, StreamsConfigPropertyNames.OPTIMIZE);
//            KGroupedStream<string, string> kGroupedStream = builder.Stream<string, string>("topic")
//                                                                         .selectKey((k, v) => k)
//                                                                         .groupByKey(Grouped.As("grouping"));
//            kGroupedStream.windowedBy(TimeWindows.of(Duration.FromMilliseconds(10L))).count();
//            kGroupedStream.windowedBy(TimeWindows.of(Duration.FromMilliseconds(30L))).count();
//            builder.Build(properties);
//        }


//        [Fact]
//        public void shouldKeepRepartitionTopicNameForJoins()
//        {

//            var expectedLeftRepartitionTopic = "(topic: my-join-left-repartition)";
//            var expectedRightRepartitionTopic = "(topic: my-join-right-repartition)";


//            var joinTopologyFirst = buildStreamJoin(false);

//            Assert.True(joinTopologyFirst.Contains(expectedLeftRepartitionTopic));
//            Assert.True(joinTopologyFirst.Contains(expectedRightRepartitionTopic));

//            var joinTopologyUpdated = buildStreamJoin(true);

//            Assert.True(joinTopologyUpdated.Contains(expectedLeftRepartitionTopic));
//            Assert.True(joinTopologyUpdated.Contains(expectedRightRepartitionTopic));
//        }

//        [Fact]
//        public void shouldKeepRepartitionTopicNameForGroupByKeyTimeWindows()
//        {

//            var expectedTimeWindowRepartitionTopic = "(topic: time-window-grouping-repartition)";

//            var timeWindowGroupingRepartitionTopology = buildStreamGroupByKeyTimeWindows(false, true);
//            Assert.True(timeWindowGroupingRepartitionTopology.Contains(expectedTimeWindowRepartitionTopic));

//            var timeWindowGroupingUpdatedTopology = buildStreamGroupByKeyTimeWindows(true, true);
//            Assert.True(timeWindowGroupingUpdatedTopology.Contains(expectedTimeWindowRepartitionTopic));
//        }

//        [Fact]
//        public void shouldKeepRepartitionTopicNameForGroupByTimeWindows()
//        {

//            var expectedTimeWindowRepartitionTopic = "(topic: time-window-grouping-repartition)";

//            var timeWindowGroupingRepartitionTopology = buildStreamGroupByKeyTimeWindows(false, false);
//            Assert.True(timeWindowGroupingRepartitionTopology.Contains(expectedTimeWindowRepartitionTopic));

//            var timeWindowGroupingUpdatedTopology = buildStreamGroupByKeyTimeWindows(true, false);
//            Assert.True(timeWindowGroupingUpdatedTopology.Contains(expectedTimeWindowRepartitionTopic));
//        }


//        [Fact]
//        public void shouldKeepRepartitionTopicNameForGroupByKeyNoWindows()
//        {

//            var expectedNoWindowRepartitionTopic = "(topic: kstream-grouping-repartition)";

//            var noWindowGroupingRepartitionTopology = buildStreamGroupByKeyNoWindows(false, true);
//            Assert.True(noWindowGroupingRepartitionTopology.Contains(expectedNoWindowRepartitionTopic));

//            var noWindowGroupingUpdatedTopology = buildStreamGroupByKeyNoWindows(true, true);
//            Assert.True(noWindowGroupingUpdatedTopology.Contains(expectedNoWindowRepartitionTopic));
//        }

//        [Fact]
//        public void shouldKeepRepartitionTopicNameForGroupByNoWindows()
//        {

//            var expectedNoWindowRepartitionTopic = "(topic: kstream-grouping-repartition)";

//            var noWindowGroupingRepartitionTopology = buildStreamGroupByKeyNoWindows(false, false);
//            Assert.True(noWindowGroupingRepartitionTopology.Contains(expectedNoWindowRepartitionTopic));

//            var noWindowGroupingUpdatedTopology = buildStreamGroupByKeyNoWindows(true, false);
//            Assert.True(noWindowGroupingUpdatedTopology.Contains(expectedNoWindowRepartitionTopic));
//        }


//        [Fact]
//        public void shouldKeepRepartitionTopicNameForGroupByKeySessionWindows()
//        {

//            var expectedSessionWindowRepartitionTopic = "(topic: session-window-grouping-repartition)";

//            var sessionWindowGroupingRepartitionTopology = buildStreamGroupByKeySessionWindows(false, true);
//            Assert.True(sessionWindowGroupingRepartitionTopology.Contains(expectedSessionWindowRepartitionTopic));

//            var sessionWindowGroupingUpdatedTopology = buildStreamGroupByKeySessionWindows(true, true);
//            Assert.True(sessionWindowGroupingUpdatedTopology.Contains(expectedSessionWindowRepartitionTopic));
//        }

//        [Fact]
//        public void shouldKeepRepartitionTopicNameForGroupBySessionWindows()
//        {

//            var expectedSessionWindowRepartitionTopic = "(topic: session-window-grouping-repartition)";

//            var sessionWindowGroupingRepartitionTopology = buildStreamGroupByKeySessionWindows(false, false);
//            Assert.True(sessionWindowGroupingRepartitionTopology.Contains(expectedSessionWindowRepartitionTopic));

//            var sessionWindowGroupingUpdatedTopology = buildStreamGroupByKeySessionWindows(true, false);
//            Assert.True(sessionWindowGroupingUpdatedTopology.Contains(expectedSessionWindowRepartitionTopic));
//        }

//        [Fact]
//        public void shouldKeepRepartitionNameForGroupByKTable()
//        {
//            var expectedKTableGroupByRepartitionTopic = "(topic: ktable-group-by-repartition)";

//            var ktableGroupByTopology = buildKTableGroupBy(false);
//            Assert.True(ktableGroupByTopology.Contains(expectedKTableGroupByRepartitionTopic));

//            var ktableUpdatedGroupByTopology = buildKTableGroupBy(true);
//            Assert.True(ktableUpdatedGroupByTopology.Contains(expectedKTableGroupByRepartitionTopic));
//        }


//        private string buildKTableGroupBy(bool otherOperations)
//        {
//            var ktableGroupByTopicName = "ktable-group-by";
//            var builder = new StreamsBuilder();

//            IKTable<string, string> ktable = builder.Table("topic");

//            if (otherOperations)
//            {
//                ktable.filter((k, v) => true).groupBy(KeyValuePair::pair, Grouped.As(ktableGroupByTopicName)).count();
//            }
//            else
//            {
//                ktable.groupBy(KeyValuePair::pair, Grouped.As(ktableGroupByTopicName)).count();
//            }

//            return builder.Build().describe().ToString();
//        }

//        private string buildStreamGroupByKeyTimeWindows(bool otherOperations, bool isGroupByKey)
//        {

//            var groupedTimeWindowRepartitionTopicName = "time-window-grouping";
//            var builder = new StreamsBuilder();

//            IKStream<string, string> selectKeyStream = builder.Stream<string, string>("topic").selectKey((k, v) => k + v);


//            if (isGroupByKey)
//            {
//                if (otherOperations)
//                {
//                    selectKeyStream.filter((k, v) => true).mapValues(v => v).groupByKey(Grouped.As(groupedTimeWindowRepartitionTopicName)).windowedBy(TimeWindows.of(Duration.FromMilliseconds(10L))).count();
//                }
//                else
//                {
//                    selectKeyStream.groupByKey(Grouped.As(groupedTimeWindowRepartitionTopicName)).windowedBy(TimeWindows.of(Duration.FromMilliseconds(10L))).count();
//                }
//            }
//            else
//            {
//                if (otherOperations)
//                {
//                    selectKeyStream.filter((k, v) => true).mapValues(v => v).groupBy(kvMapper, Grouped.As(groupedTimeWindowRepartitionTopicName)).count();
//                }
//                else
//                {
//                    selectKeyStream.groupBy(kvMapper, Grouped.As(groupedTimeWindowRepartitionTopicName)).count();
//                }
//            }

//            return builder.Build().describe().ToString();
//        }


//        private string buildStreamGroupByKeySessionWindows(bool otherOperations, bool isGroupByKey)
//        {

//            var builder = new StreamsBuilder();

//            IKStream<string, string> selectKeyStream = builder.Stream<string, string>("topic").selectKey((k, v) => k + v);

//            var groupedSessionWindowRepartitionTopicName = "session-window-grouping";
//            if (isGroupByKey)
//            {
//                if (otherOperations)
//                {
//                    selectKeyStream.filter((k, v) => true).mapValues(v => v).groupByKey(Grouped.As(groupedSessionWindowRepartitionTopicName)).windowedBy(SessionWindows.with(Duration.FromMilliseconds(10L))).count();
//                }
//                else
//                {
//                    selectKeyStream.groupByKey(Grouped.As(groupedSessionWindowRepartitionTopicName)).windowedBy(SessionWindows.with(Duration.FromMilliseconds(10L))).count();
//                }
//            }
//            else
//            {
//                if (otherOperations)
//                {
//                    selectKeyStream.filter((k, v) => true).mapValues(v => v).groupBy(kvMapper, Grouped.As(groupedSessionWindowRepartitionTopicName)).windowedBy(SessionWindows.with(Duration.FromMilliseconds(10L))).count();
//                }
//                else
//                {
//                    selectKeyStream.groupBy(kvMapper, Grouped.As(groupedSessionWindowRepartitionTopicName)).windowedBy(SessionWindows.with(Duration.FromMilliseconds(10L))).count();
//                }
//            }

//            return builder.Build().describe().ToString();
//        }


//        private string buildStreamGroupByKeyNoWindows(bool otherOperations, bool isGroupByKey)
//        {

//            var builder = new StreamsBuilder();

//            IKStream<string, string> selectKeyStream = builder.Stream<string, string>("topic").selectKey((k, v) => k + v);

//            var groupByAndCountRepartitionTopicName = "kstream-grouping";
//            if (isGroupByKey)
//            {
//                if (otherOperations)
//                {
//                    selectKeyStream.filter((k, v) => true).mapValues(v => v).groupByKey(Grouped.As(groupByAndCountRepartitionTopicName)).count();
//                }
//                else
//                {
//                    selectKeyStream.groupByKey(Grouped.As(groupByAndCountRepartitionTopicName)).count();
//                }
//            }
//            else
//            {
//                if (otherOperations)
//                {
//                    selectKeyStream.filter((k, v) => true).mapValues(v => v).groupBy(kvMapper, Grouped.As(groupByAndCountRepartitionTopicName)).count();
//                }
//                else
//                {
//                    selectKeyStream.groupBy(kvMapper, Grouped.As(groupByAndCountRepartitionTopicName)).count();
//                }
//            }

//            return builder.Build().describe().ToString();
//        }

//        private string buildStreamJoin(bool includeOtherOperations)
//        {
//            var builder = new StreamsBuilder();
//            IKStream<string, string> initialStreamOne = builder.Stream("topic-one");
//            IKStream<string, string> initialStreamTwo = builder.Stream("topic-two");

//            IKStream<string, string> updatedStreamOne;
//            IKStream<string, string> updatedStreamTwo;

//            if (includeOtherOperations)
//            {
//                // without naming the join, the repartition topic name would change due to operator changing before join performed
//                updatedStreamOne = initialStreamOne.selectKey((k, v) => k + v).filter((k, v) => true).peek((k, v) => System.Console.Out.WriteLine(k + v));
//                updatedStreamTwo = initialStreamTwo.selectKey((k, v) => k + v).filter((k, v) => true).peek((k, v) => System.Console.Out.WriteLine(k + v));
//            }
//            else
//            {
//                updatedStreamOne = initialStreamOne.selectKey((k, v) => k + v);
//                updatedStreamTwo = initialStreamTwo.selectKey((k, v) => k + v);
//            }

//            var joinRepartitionTopicName = "my-join";
//            updatedStreamOne.join(updatedStreamTwo, (v1, v2) => v1 + v2,
//                    JoinWindows.of(Duration.FromMilliseconds(1000L)), Joined.with(Serdes.String(), Serdes.String(), Serdes.String(), joinRepartitionTopicName));

//            return builder.Build().describe().ToString();
//        }


//        private int getCountOfRepartitionTopicsFound(string topologyString, Regex repartitionTopicPattern)
//        {
//            Matcher matcher = repartitionTopicPattern.matcher(topologyString);
//            List<string> repartitionTopicsFound = new List<>();
//            while (matcher.find())
//            {
//                repartitionTopicsFound.add(matcher.group());
//            }
//            return repartitionTopicsFound.Count;
//        }


//        private Topology buildTopology(string optimizationConfig)
//        {
//            Initializer<int> initializer = () => 0;
//            Aggregator<string, string, int> aggregator = (k, v, agg) => agg + v.Length();
//            Reducer<string> reducer = (v1, v2) => v1 + ":" + v2;
//            List<string> processorValueCollector = new List<>();

//            var builder = new StreamsBuilder();

//            IKStream<string, string> sourceStream = builder.Stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

//            IKStream<string, string> mappedStream = sourceStream.map((k, v) => KeyValuePair.Create(k.toUppercase(Locale.getDefault()), v));

//            mappedStream.filter((k, v) => k.equals("B")).mapValues(v => v.toUppercase(Locale.getDefault()))
//                    .process(() => new SimpleProcessor(processorValueCollector));

//            IKStream<string, long> countStream = mappedStream.groupByKey(Grouped.As(firstRepartitionTopicName)).count(Materialized.with(Serdes.String(), Serdes.Long())).toStream();

//            countStream.to(COUNT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

//            mappedStream.groupByKey(Grouped.As(secondRepartitionTopicName)).aggregate(initializer,
//                    aggregator,
//                    Materialized.with(Serdes.String(), Serdes.Int()))
//                    .toStream().to(AGGREGATION_TOPIC, Produced.with(Serdes.String(), Serdes.Int()));

//            // adding operators for case where the repartition node is further downstream
//            mappedStream.filter((k, v) => true).peek((k, v) => System.Console.Out.WriteLine(k + ":" + v)).groupByKey(Grouped.As(thirdRepartitionTopicName))
//                    .reduce(reducer, Materialized.with(Serdes.String(), Serdes.String()))
//                    .toStream().to(REDUCE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

//            mappedStream.filter((k, v) => k.equals("A"))
//                    .join(countStream, (v1, v2) => v1 + ":" + v2.ToString(),
//                            JoinWindows.of(Duration.FromMilliseconds(5000L)),
//                            Joined.with(Serdes.String(), Serdes.String(), Serdes.Long(), fourthRepartitionTopicName))
//                    .to(JOINED_TOPIC);

//            var properties = new StreamsConfig();

//            properties.put(StreamsConfigPropertyNames.TOPOLOGY_OPTIMIZATION, optimizationConfig);
//            return builder.Build(properties);
//        }


//        private static class SimpleProcessor : AbstractProcessor<string, string>
//        {

//            List<string> valueList;

//            SimpleProcessor(List<string> valueList)
//            {
//                this.valueList = valueList;
//            }


//            public void process(string key, string value)
//            {
//                valueList.add(value);
//            }
//        }


//        private static string EXPECTED_OPTIMIZED_TOPOLOGY = "Topologies:\n" +
//                "   Sub-topology: 0\n" +
//                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n" +
//                "      -=> KSTREAM-MAP-0000000001\n" +
//                "    Processor: KSTREAM-MAP-0000000001 (stores: [])\n" +
//                "      -=> KSTREAM-FILTER-0000000002, KSTREAM-FILTER-0000000040\n" +
//                "      <-- KSTREAM-SOURCE-0000000000\n" +
//                "    Processor: KSTREAM-FILTER-0000000002 (stores: [])\n" +
//                "      -=> KSTREAM-MAPVALUES-0000000003\n" +
//                "      <-- KSTREAM-MAP-0000000001\n" +
//                "    Processor: KSTREAM-FILTER-0000000040 (stores: [])\n" +
//                "      -=> KSTREAM-SINK-0000000039\n" +
//                "      <-- KSTREAM-MAP-0000000001\n" +
//                "    Processor: KSTREAM-MAPVALUES-0000000003 (stores: [])\n" +
//                "      -=> KSTREAM-PROCESSOR-0000000004\n" +
//                "      <-- KSTREAM-FILTER-0000000002\n" +
//                "    Processor: KSTREAM-PROCESSOR-0000000004 (stores: [])\n" +
//                "      -=> none\n" +
//                "      <-- KSTREAM-MAPVALUES-0000000003\n" +
//                "    Sink: KSTREAM-SINK-0000000039 (topic: count-stream-repartition)\n" +
//                "      <-- KSTREAM-FILTER-0000000040\n" +
//                "\n" +
//                "  Sub-topology: 1\n" +
//                "    Source: KSTREAM-SOURCE-0000000041 (topics: [count-stream-repartition])\n" +
//                "      -=> KSTREAM-FILTER-0000000020, KSTREAM-AGGREGATE-0000000007, KSTREAM-AGGREGATE-0000000014, KSTREAM-FILTER-0000000029\n" +
//                "    Processor: KSTREAM-AGGREGATE-0000000007 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000006])\n" +
//                "      -=> KTABLE-TOSTREAM-0000000011\n" +
//                "      <-- KSTREAM-SOURCE-0000000041\n" +
//                "    Processor: KTABLE-TOSTREAM-0000000011 (stores: [])\n" +
//                "      -=> joined-stream-other-windowed, KSTREAM-SINK-0000000012\n" +
//                "      <-- KSTREAM-AGGREGATE-0000000007\n" +
//                "    Processor: KSTREAM-FILTER-0000000020 (stores: [])\n" +
//                "      -=> KSTREAM-PEEK-0000000021\n" +
//                "      <-- KSTREAM-SOURCE-0000000041\n" +
//                "    Processor: KSTREAM-FILTER-0000000029 (stores: [])\n" +
//                "      -=> joined-stream-this-windowed\n" +
//                "      <-- KSTREAM-SOURCE-0000000041\n" +
//                "    Processor: KSTREAM-PEEK-0000000021 (stores: [])\n" +
//                "      -=> KSTREAM-REDUCE-0000000023\n" +
//                "      <-- KSTREAM-FILTER-0000000020\n" +
//                "    Processor: joined-stream-other-windowed (stores: [joined-stream-other-join-store])\n" +
//                "      -=> joined-stream-other-join\n" +
//                "      <-- KTABLE-TOSTREAM-0000000011\n" +
//                "    Processor: joined-stream-this-windowed (stores: [joined-stream-this-join-store])\n" +
//                "      -=> joined-stream-this-join\n" +
//                "      <-- KSTREAM-FILTER-0000000029\n" +
//                "    Processor: KSTREAM-AGGREGATE-0000000014 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000013])\n" +
//                "      -=> KTABLE-TOSTREAM-0000000018\n" +
//                "      <-- KSTREAM-SOURCE-0000000041\n" +
//                "    Processor: KSTREAM-REDUCE-0000000023 (stores: [KSTREAM-REDUCE-STATE-STORE-0000000022])\n" +
//                "      -=> KTABLE-TOSTREAM-0000000027\n" +
//                "      <-- KSTREAM-PEEK-0000000021\n" +
//                "    Processor: joined-stream-other-join (stores: [joined-stream-this-join-store])\n" +
//                "      -=> joined-stream-merge\n" +
//                "      <-- joined-stream-other-windowed\n" +
//                "    Processor: joined-stream-this-join (stores: [joined-stream-other-join-store])\n" +
//                "      -=> joined-stream-merge\n" +
//                "      <-- joined-stream-this-windowed\n" +
//                "    Processor: KTABLE-TOSTREAM-0000000018 (stores: [])\n" +
//                "      -=> KSTREAM-SINK-0000000019\n" +
//                "      <-- KSTREAM-AGGREGATE-0000000014\n" +
//                "    Processor: KTABLE-TOSTREAM-0000000027 (stores: [])\n" +
//                "      -=> KSTREAM-SINK-0000000028\n" +
//                "      <-- KSTREAM-REDUCE-0000000023\n" +
//                "    Processor: joined-stream-merge (stores: [])\n" +
//                "      -=> KSTREAM-SINK-0000000038\n" +
//                "      <-- joined-stream-this-join, joined-stream-other-join\n" +
//                "    Sink: KSTREAM-SINK-0000000012 (topic: outputTopic_0)\n" +
//                "      <-- KTABLE-TOSTREAM-0000000011\n" +
//                "    Sink: KSTREAM-SINK-0000000019 (topic: outputTopic_1)\n" +
//                "      <-- KTABLE-TOSTREAM-0000000018\n" +
//                "    Sink: KSTREAM-SINK-0000000028 (topic: outputTopic_2)\n" +
//                "      <-- KTABLE-TOSTREAM-0000000027\n" +
//                "    Sink: KSTREAM-SINK-0000000038 (topic: outputTopicForJoin)\n" +
//                "      <-- joined-stream-merge\n\n";


//        private static string EXPECTED_UNOPTIMIZED_TOPOLOGY = "Topologies:\n" +
//                "   Sub-topology: 0\n" +
//                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n" +
//                "      -=> KSTREAM-MAP-0000000001\n" +
//                "    Processor: KSTREAM-MAP-0000000001 (stores: [])\n" +
//                "      -=> KSTREAM-FILTER-0000000020, KSTREAM-FILTER-0000000002, KSTREAM-FILTER-0000000009, KSTREAM-FILTER-0000000016, KSTREAM-FILTER-0000000029\n" +
//                "      <-- KSTREAM-SOURCE-0000000000\n" +
//                "    Processor: KSTREAM-FILTER-0000000020 (stores: [])\n" +
//                "      -=> KSTREAM-PEEK-0000000021\n" +
//                "      <-- KSTREAM-MAP-0000000001\n" +
//                "    Processor: KSTREAM-FILTER-0000000002 (stores: [])\n" +
//                "      -=> KSTREAM-MAPVALUES-0000000003\n" +
//                "      <-- KSTREAM-MAP-0000000001\n" +
//                "    Processor: KSTREAM-FILTER-0000000029 (stores: [])\n" +
//                "      -=> KSTREAM-FILTER-0000000031\n" +
//                "      <-- KSTREAM-MAP-0000000001\n" +
//                "    Processor: KSTREAM-PEEK-0000000021 (stores: [])\n" +
//                "      -=> KSTREAM-FILTER-0000000025\n" +
//                "      <-- KSTREAM-FILTER-0000000020\n" +
//                "    Processor: KSTREAM-FILTER-0000000009 (stores: [])\n" +
//                "      -=> KSTREAM-SINK-0000000008\n" +
//                "      <-- KSTREAM-MAP-0000000001\n" +
//                "    Processor: KSTREAM-FILTER-0000000016 (stores: [])\n" +
//                "      -=> KSTREAM-SINK-0000000015\n" +
//                "      <-- KSTREAM-MAP-0000000001\n" +
//                "    Processor: KSTREAM-FILTER-0000000025 (stores: [])\n" +
//                "      -=> KSTREAM-SINK-0000000024\n" +
//                "      <-- KSTREAM-PEEK-0000000021\n" +
//                "    Processor: KSTREAM-FILTER-0000000031 (stores: [])\n" +
//                "      -=> KSTREAM-SINK-0000000030\n" +
//                "      <-- KSTREAM-FILTER-0000000029\n" +
//                "    Processor: KSTREAM-MAPVALUES-0000000003 (stores: [])\n" +
//                "      -=> KSTREAM-PROCESSOR-0000000004\n" +
//                "      <-- KSTREAM-FILTER-0000000002\n" +
//                "    Processor: KSTREAM-PROCESSOR-0000000004 (stores: [])\n" +
//                "      -=> none\n" +
//                "      <-- KSTREAM-MAPVALUES-0000000003\n" +
//                "    Sink: KSTREAM-SINK-0000000008 (topic: count-stream-repartition)\n" +
//                "      <-- KSTREAM-FILTER-0000000009\n" +
//                "    Sink: KSTREAM-SINK-0000000015 (topic: aggregate-stream-repartition)\n" +
//                "      <-- KSTREAM-FILTER-0000000016\n" +
//                "    Sink: KSTREAM-SINK-0000000024 (topic: reduced-stream-repartition)\n" +
//                "      <-- KSTREAM-FILTER-0000000025\n" +
//                "    Sink: KSTREAM-SINK-0000000030 (topic: joined-stream-left-repartition)\n" +
//                "      <-- KSTREAM-FILTER-0000000031\n" +
//                "\n" +
//                "  Sub-topology: 1\n" +
//                "    Source: KSTREAM-SOURCE-0000000010 (topics: [count-stream-repartition])\n" +
//                "      -=> KSTREAM-AGGREGATE-0000000007\n" +
//                "    Processor: KSTREAM-AGGREGATE-0000000007 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000006])\n" +
//                "      -=> KTABLE-TOSTREAM-0000000011\n" +
//                "      <-- KSTREAM-SOURCE-0000000010\n" +
//                "    Processor: KTABLE-TOSTREAM-0000000011 (stores: [])\n" +
//                "      -=> KSTREAM-SINK-0000000012, joined-stream-other-windowed\n" +
//                "      <-- KSTREAM-AGGREGATE-0000000007\n" +
//                "    Source: KSTREAM-SOURCE-0000000032 (topics: [joined-stream-left-repartition])\n" +
//                "      -=> joined-stream-this-windowed\n" +
//                "    Processor: joined-stream-other-windowed (stores: [joined-stream-other-join-store])\n" +
//                "      -=> joined-stream-other-join\n" +
//                "      <-- KTABLE-TOSTREAM-0000000011\n" +
//                "    Processor: joined-stream-this-windowed (stores: [joined-stream-this-join-store])\n" +
//                "      -=> joined-stream-this-join\n" +
//                "      <-- KSTREAM-SOURCE-0000000032\n" +
//                "    Processor: joined-stream-other-join (stores: [joined-stream-this-join-store])\n" +
//                "      -=> joined-stream-merge\n" +
//                "      <-- joined-stream-other-windowed\n" +
//                "    Processor: joined-stream-this-join (stores: [joined-stream-other-join-store])\n" +
//                "      -=> joined-stream-merge\n" +
//                "      <-- joined-stream-this-windowed\n" +
//                "    Processor: joined-stream-merge (stores: [])\n" +
//                "      -=> KSTREAM-SINK-0000000038\n" +
//                "      <-- joined-stream-this-join, joined-stream-other-join\n" +
//                "    Sink: KSTREAM-SINK-0000000012 (topic: outputTopic_0)\n" +
//                "      <-- KTABLE-TOSTREAM-0000000011\n" +
//                "    Sink: KSTREAM-SINK-0000000038 (topic: outputTopicForJoin)\n" +
//                "      <-- joined-stream-merge\n" +
//                "\n" +
//                "  Sub-topology: 2\n" +
//                "    Source: KSTREAM-SOURCE-0000000017 (topics: [aggregate-stream-repartition])\n" +
//                "      -=> KSTREAM-AGGREGATE-0000000014\n" +
//                "    Processor: KSTREAM-AGGREGATE-0000000014 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000013])\n" +
//                "      -=> KTABLE-TOSTREAM-0000000018\n" +
//                "      <-- KSTREAM-SOURCE-0000000017\n" +
//                "    Processor: KTABLE-TOSTREAM-0000000018 (stores: [])\n" +
//                "      -=> KSTREAM-SINK-0000000019\n" +
//                "      <-- KSTREAM-AGGREGATE-0000000014\n" +
//                "    Sink: KSTREAM-SINK-0000000019 (topic: outputTopic_1)\n" +
//                "      <-- KTABLE-TOSTREAM-0000000018\n" +
//                "\n" +
//                "  Sub-topology: 3\n" +
//                "    Source: KSTREAM-SOURCE-0000000026 (topics: [reduced-stream-repartition])\n" +
//                "      -=> KSTREAM-REDUCE-0000000023\n" +
//                "    Processor: KSTREAM-REDUCE-0000000023 (stores: [KSTREAM-REDUCE-STATE-STORE-0000000022])\n" +
//                "      -=> KTABLE-TOSTREAM-0000000027\n" +
//                "      <-- KSTREAM-SOURCE-0000000026\n" +
//                "    Processor: KTABLE-TOSTREAM-0000000027 (stores: [])\n" +
//                "      -=> KSTREAM-SINK-0000000028\n" +
//                "      <-- KSTREAM-REDUCE-0000000023\n" +
//                "    Sink: KSTREAM-SINK-0000000028 (topic: outputTopic_2)\n" +
//                "      <-- KTABLE-TOSTREAM-0000000027\n\n";


//    }
