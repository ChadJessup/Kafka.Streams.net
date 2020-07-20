using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.Topologies;
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Xunit;

namespace Kafka.Streams.Tests.Kstream
{
    public class RepartitionTopicNamingTest
    {

        private readonly KeyValueMapper<string, string, string> kvMapper = (k, v) => k + v;
        private static readonly string INPUT_TOPIC = "input";
        private static readonly string COUNT_TOPIC = "outputTopic_0";
        private static readonly string AGGREGATION_TOPIC = "outputTopic_1";
        private static readonly string REDUCE_TOPIC = "outputTopic_2";
        private static readonly string JOINED_TOPIC = "outputTopicForJoin";

        private readonly string firstRepartitionTopicName = "count-stream";
        private readonly string secondRepartitionTopicName = "aggregate-stream";
        private readonly string thirdRepartitionTopicName = "reduced-stream";
        private readonly string fourthRepartitionTopicName = "joined-stream";
        private readonly Regex repartitionTopicPattern = new Regex("Sink: .*-repartition", RegexOptions.Compiled);


        [Fact]
        public void shouldReuseFirstRepartitionTopicNameWhenOptimizing()
        {
            string optimizedTopology = buildTopology(StreamsConfig.OPTIMIZEConfig).Describe().ToString();
            string unOptimizedTopology = buildTopology(StreamsConfig.NoOptimizationConfig).Describe().ToString();

            Assert.Equal(optimizedTopology, EXPECTED_OPTIMIZED_TOPOLOGY);
            // only one repartition topic
            Assert.Equal(1, getCountOfRepartitionTopicsFound(optimizedTopology, repartitionTopicPattern));
            // the first named repartition topic
            Assert.Contains(firstRepartitionTopicName + "-repartition", optimizedTopology);


            Assert.Equal(unOptimizedTopology, EXPECTED_UNOPTIMIZED_TOPOLOGY);
            // now 4 repartition topic
            Assert.Equal(4, getCountOfRepartitionTopicsFound(unOptimizedTopology, repartitionTopicPattern));
            // All 4 named repartition topics present
            Assert.Contains(firstRepartitionTopicName + "-repartition", unOptimizedTopology);
            Assert.Contains(secondRepartitionTopicName + "-repartition", unOptimizedTopology);
            Assert.Contains(thirdRepartitionTopicName + "-repartition", unOptimizedTopology);
            Assert.Contains(fourthRepartitionTopicName + "-left-repartition", unOptimizedTopology);
        }

        // can't use same repartition topic Name
        [Fact]
        public void shouldFailWithSameRepartitionTopicName()
        {
            try
            {
                var builder = new StreamsBuilder();
                builder.Stream<string, string>("topic")
                    .SelectKey((k, v) => k)
                    .GroupByKey(Grouped.As<string, string>("grouping"))
                    .Count().ToStream();

                builder.Stream<string, string>("topicII")
                    .SelectKey((k, v) => k)
                    .GroupByKey(Grouped.As<string, string>("grouping"))
                    .Count().ToStream();

                builder.Build();
                Assert.False(true, "Should not build re-using repartition topic Name");
            }
            catch (TopologyException te)
            {
                // ok
            }
        }

        [Fact]
        public void ShouldNotFailWithSameRepartitionTopicNameUsingSameKGroupedStream()
        {
            var builder = new StreamsBuilder();
            IKGroupedStream<string, string> kGroupedStream = builder
                .Stream<string, string>("topic")
                .SelectKey((k, v) => k)
                .GroupByKey(Grouped.As<string, string>("grouping"));

            kGroupedStream.WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(10L))).Count().ToStream().To("output-one");
            kGroupedStream.WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(30L))).Count().ToStream().To("output-two");

            string topologyString = builder.Build().Describe().ToString();
            Assert.Equal(1, getCountOfRepartitionTopicsFound(topologyString, repartitionTopicPattern));
            Assert.Contains("grouping-repartition", topologyString);
        }

        [Fact]
        public void shouldNotFailWithSameRepartitionTopicNameUsingSameTimeWindowStream()
        {
            var builder = new StreamsBuilder();
            IKGroupedStream<string, string> kGroupedStream = builder
                .Stream<string, string>("topic")
                .SelectKey((k, v) => k)
                .GroupByKey(Grouped.As<string, string>("grouping"));

            var timeWindowedKStream = kGroupedStream.WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(10L)));

            timeWindowedKStream.Count().ToStream().To("output-one");
            timeWindowedKStream.Reduce((v, v2) => v + v2).ToStream().To("output-two");
            kGroupedStream.WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(30L))).Count().ToStream().To("output-two");

            string topologyString = builder.Build().Describe().ToString();
            Assert.Equal(1, getCountOfRepartitionTopicsFound(topologyString, repartitionTopicPattern));
            Assert.Contains("grouping-repartition", topologyString);
        }

        [Fact]
        public void ShouldNotFailWithSameRepartitionTopicNameUsingSameSessionWindowStream()
        {
            var builder = new StreamsBuilder();
            IKGroupedStream<string, string> kGroupedStream = builder
                .Stream<string, string>("topic")
                .SelectKey((k, v) => k)
                .GroupByKey(Grouped.As<string, string>("grouping"));

            var sessionWindowedKStream = kGroupedStream.WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(10L)));

            sessionWindowedKStream.Count().ToStream().To("output-one");
            sessionWindowedKStream.Reduce((v, v2) => v + v2).ToStream().To("output-two");
            kGroupedStream.WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(30L))).Count().ToStream().To("output-two");

            string topologyString = builder.Build().Describe().ToString();
            Assert.Equal(1, getCountOfRepartitionTopicsFound(topologyString, repartitionTopicPattern));
            Assert.Contains("grouping-repartition", topologyString);
        }

        [Fact]
        public void ShouldNotFailWithSameRepartitionTopicNameUsingSameKGroupedTable()
        {
            var builder = new StreamsBuilder();
            IKGroupedTable<string, string> kGroupedTable = builder.Table<string, string>("topic")
                .GroupBy(KeyValuePair.Create, Grouped.As<string, string>("grouping"));

            kGroupedTable.Count().ToStream().To("output-count");
            kGroupedTable.Reduce((v, v2) => v2, (v, v2) => v2).ToStream().To("output-reduce");

            string topologyString = builder.Build().Describe().ToString();
            Assert.Equal(1, getCountOfRepartitionTopicsFound(topologyString, repartitionTopicPattern));
            Assert.Contains("grouping-repartition", topologyString);
        }

        [Fact]
        public void shouldNotReuseRepartitionNodeWithUnamedRepartitionTopics()
        {
            var builder = new StreamsBuilder();
            var kGroupedStream = builder
                .Stream<string, string>("topic")
                .SelectKey((k, v) => k)
                .GroupByKey();

            kGroupedStream.WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(10L))).Count().ToStream().To("output-one");
            kGroupedStream.WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(30L))).Count().ToStream().To("output-two");
            string topologyString = builder.Build().Describe().ToString();
            Assert.Equal(2, getCountOfRepartitionTopicsFound(topologyString, repartitionTopicPattern));
        }

        [Fact]
        public void shouldNotReuseRepartitionNodeWithUnamedRepartitionTopicsKGroupedTable()
        {
            var builder = new StreamsBuilder();
            IKGroupedTable<string, string> kGroupedTable = builder.Table<string, string>("topic").GroupBy(KeyValuePair.Create);
            kGroupedTable.Count().ToStream().To("output-count");
            kGroupedTable.Reduce((v, v2) => v2, (v, v2) => v2).ToStream().To("output-reduce");
            string topologyString = builder.Build().Describe().ToString();
            Assert.Equal(2, getCountOfRepartitionTopicsFound(topologyString, repartitionTopicPattern));
        }

        [Fact]
        public void ShouldNotFailWithSameRepartitionTopicNameUsingSameKGroupedStreamOptimizationsOn()
        {
            var builder = new StreamsBuilder();
            var kGroupedStream = builder.Stream<string, string>("topic")
               .SelectKey((k, v) => k)
               .GroupByKey(Grouped.As<string, string>("grouping"));

            kGroupedStream.WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(10L))).Count();
            kGroupedStream.WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(30L))).Count();
            var properties = new StreamsConfig();
            properties.Put(StreamsConfig.TOPOLOGY_OPTIMIZATIONConfig, StreamsConfig.OPTIMIZEConfig);
            Topology topology = builder.Build(properties);
            Assert.Equal(1, getCountOfRepartitionTopicsFound(topology.Describe().ToString(), repartitionTopicPattern));
        }


        // can't use same repartition topic Name in joins
        [Fact]
        public void shouldFailWithSameRepartitionTopicNameInJoin()
        {
            try
            {
                var builder = new StreamsBuilder();

                IKStream<string, string> stream1 = builder.Stream<string, string>("topic").SelectKey((k, v) => k);
                IKStream<string, string> stream2 = builder.Stream<string, string>("topic2").SelectKey((k, v) => k);
                IKStream<string, string> stream3 = builder.Stream<string, string>("topic3").SelectKey((k, v) => k);
                IKStream<string, string> joined = stream1.Join(
                    stream2, (v1, v2) => v1 + v2,
                    JoinWindows.Of(TimeSpan.FromMilliseconds(30L)),
                    Joined.As<string, string, string>("join-repartition"));

                joined.Join(
                    stream3,
                    (v1, v2) => v1 + v2,
                    JoinWindows.Of(TimeSpan.FromMilliseconds(30L)),
                    Joined.As<string, string, string>("join-repartition"));

                builder.Build();
                Assert.True(false, "Should not build re-using repartition topic name");
            }
            catch (TopologyException te)
            {
                // ok
            }
        }

        [Fact]
        public void shouldPassWithSameRepartitionTopicNameUsingSameKGroupedStreamOptimized()
        {
            var builder = new StreamsBuilder();
            var properties = new StreamsConfig();
            //properties.TopologyOptimization= StreamsConfig.OPTIMIZEConfig;
            IKGroupedStream<string, string> kGroupedStream = builder.Stream<string, string>("topic")
                .SelectKey((k, v) => k)
                .GroupByKey(Grouped.As<string, string>("grouping"));

            kGroupedStream.WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(10L))).Count();
            kGroupedStream.WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(30L))).Count();
            builder.Build(properties);
        }


        [Fact]
        public void ShouldKeepRepartitionTopicNameForJoins()
        {
            var expectedLeftRepartitionTopic = "(topic: my-join-left-repartition)";
            var expectedRightRepartitionTopic = "(topic: my-join-right-repartition)";

            var joinTopologyFirst = buildStreamJoin(false);

            Assert.Contains(expectedLeftRepartitionTopic, joinTopologyFirst);
            Assert.Contains(expectedRightRepartitionTopic, joinTopologyFirst);

            var joinTopologyUpdated = buildStreamJoin(true);

            Assert.Contains(expectedLeftRepartitionTopic, joinTopologyUpdated);
            Assert.Contains(expectedRightRepartitionTopic, joinTopologyUpdated);
        }

        [Fact]
        public void shouldKeepRepartitionTopicNameForGroupByKeyTimeWindows()
        {

            var expectedTimeWindowRepartitionTopic = "(topic: time-window-grouping-repartition)";

            var timeWindowGroupingRepartitionTopology = BuildStreamGroupByKeyTimeWindows(false, true);
            Assert.Contains(expectedTimeWindowRepartitionTopic, timeWindowGroupingRepartitionTopology);

            var timeWindowGroupingUpdatedTopology = BuildStreamGroupByKeyTimeWindows(true, true);
            Assert.Contains(expectedTimeWindowRepartitionTopic, timeWindowGroupingUpdatedTopology);
        }

        [Fact]
        public void ShouldKeepRepartitionTopicNameForGroupByTimeWindows()
        {
            var expectedTimeWindowRepartitionTopic = "(topic: time-window-grouping-repartition)";

            var timeWindowGroupingRepartitionTopology = BuildStreamGroupByKeyTimeWindows(false, false);
            Assert.Contains(expectedTimeWindowRepartitionTopic, timeWindowGroupingRepartitionTopology);

            var timeWindowGroupingUpdatedTopology = BuildStreamGroupByKeyTimeWindows(true, false);
            Assert.Contains(expectedTimeWindowRepartitionTopic, timeWindowGroupingUpdatedTopology);
        }


        [Fact]
        public void shouldKeepRepartitionTopicNameForGroupByKeyNoWindows()
        {

            var expectedNoWindowRepartitionTopic = "(topic: kstream-grouping-repartition)";

            var noWindowGroupingRepartitionTopology = BuildStreamGroupByKeyNoWindows(false, true);
            Assert.Contains(expectedNoWindowRepartitionTopic, noWindowGroupingRepartitionTopology);

            var noWindowGroupingUpdatedTopology = BuildStreamGroupByKeyNoWindows(true, true);
            Assert.Contains(expectedNoWindowRepartitionTopic, noWindowGroupingUpdatedTopology);
        }

        [Fact]
        public void ShouldKeepRepartitionTopicNameForGroupByNoWindows()
        {
            var expectedNoWindowRepartitionTopic = "(topic: kstream-grouping-repartition)";

            var noWindowGroupingRepartitionTopology = BuildStreamGroupByKeyNoWindows(false, false);
            Assert.Contains(expectedNoWindowRepartitionTopic, noWindowGroupingRepartitionTopology);

            var noWindowGroupingUpdatedTopology = BuildStreamGroupByKeyNoWindows(true, false);
            Assert.Contains(expectedNoWindowRepartitionTopic, noWindowGroupingUpdatedTopology);
        }


        [Fact]
        public void ShouldKeepRepartitionTopicNameForGroupByKeySessionWindows()
        {
            var expectedSessionWindowRepartitionTopic = "(topic: session-window-grouping-repartition)";

            var sessionWindowGroupingRepartitionTopology = BuildStreamGroupByKeySessionWindows(false, true);
            Assert.Contains(expectedSessionWindowRepartitionTopic, sessionWindowGroupingRepartitionTopology);

            var sessionWindowGroupingUpdatedTopology = BuildStreamGroupByKeySessionWindows(true, true);
            Assert.Contains(expectedSessionWindowRepartitionTopic, sessionWindowGroupingUpdatedTopology);
        }

        [Fact]
        public void ShouldKeepRepartitionTopicNameForGroupBySessionWindows()
        {
            var expectedSessionWindowRepartitionTopic = "(topic: session-window-grouping-repartition)";

            var sessionWindowGroupingRepartitionTopology = BuildStreamGroupByKeySessionWindows(false, false);
            Assert.Contains(expectedSessionWindowRepartitionTopic, sessionWindowGroupingRepartitionTopology);

            var sessionWindowGroupingUpdatedTopology = BuildStreamGroupByKeySessionWindows(true, false);
            Assert.Contains(expectedSessionWindowRepartitionTopic, sessionWindowGroupingUpdatedTopology);
        }

        [Fact]
        public void ShouldKeepRepartitionNameForGroupByKTable()
        {
            var expectedKTableGroupByRepartitionTopic = "(topic: ktable-group-by-repartition)";

            var ktableGroupByTopology = BuildKTableGroupBy(false);
            Assert.Contains(expectedKTableGroupByRepartitionTopic, ktableGroupByTopology);

            var ktableUpdatedGroupByTopology = BuildKTableGroupBy(true);
            Assert.Contains(expectedKTableGroupByRepartitionTopic, ktableUpdatedGroupByTopology);
        }

        private string BuildKTableGroupBy(bool otherOperations)
        {
            var ktableGroupByTopicName = "ktable-group-by";
            var builder = new StreamsBuilder();

            IKTable<string, string> ktable = builder.Table<string, string>("topic");

            if (otherOperations)
            {
                ktable.Filter((k, v) => true).GroupBy(KeyValuePair.Create, Grouped.As<string, string>(ktableGroupByTopicName)).Count();
            }
            else
            {
                ktable.GroupBy(KeyValuePair.Create, Grouped.As<string, string>(ktableGroupByTopicName)).Count();
            }

            return builder.Build().Describe().ToString();
        }

        private string BuildStreamGroupByKeyTimeWindows(bool otherOperations, bool isGroupByKey)
        {
            var groupedTimeWindowRepartitionTopicName = "time-window-grouping";
            var builder = new StreamsBuilder();

            IKStream<string, string> selectKeyStream = builder.Stream<string, string>("topic").SelectKey((k, v) => k + v);

            if (isGroupByKey)
            {
                if (otherOperations)
                {
                    selectKeyStream.Filter((k, v) => true).MapValues(v => v).GroupByKey(Grouped.As<string, string>(groupedTimeWindowRepartitionTopicName)).WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(10L))).Count();
                }
                else
                {
                    selectKeyStream.GroupByKey(Grouped.As<string, string>(groupedTimeWindowRepartitionTopicName)).WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(10L))).Count();
                }
            }
            else
            {
                if (otherOperations)
                {
                    selectKeyStream.Filter((k, v) => true).MapValues(v => v).GroupBy(kvMapper, Grouped.As<string, string>(groupedTimeWindowRepartitionTopicName)).Count();
                }
                else
                {
                    selectKeyStream.GroupBy(kvMapper, Grouped.As<string, string>(groupedTimeWindowRepartitionTopicName)).Count();
                }
            }

            return builder.Build().Describe().ToString();
        }

        private string BuildStreamGroupByKeySessionWindows(bool otherOperations, bool isGroupByKey)
        {

            var builder = new StreamsBuilder();

            IKStream<string, string> selectKeyStream = builder.Stream<string, string>("topic").SelectKey((k, v) => k + v);

            var groupedSessionWindowRepartitionTopicName = "session-window-grouping";
            if (isGroupByKey)
            {
                if (otherOperations)
                {
                    selectKeyStream.Filter((k, v) => true).MapValues(v => v).GroupByKey(Grouped.As<string, string>(groupedSessionWindowRepartitionTopicName)).WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(10L))).Count();
                }
                else
                {
                    selectKeyStream.GroupByKey(Grouped.As<string, string>(groupedSessionWindowRepartitionTopicName)).WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(10L))).Count();
                }
            }
            else
            {
                if (otherOperations)
                {
                    selectKeyStream.Filter((k, v) => true).MapValues(v => v).GroupBy(kvMapper, Grouped.As<string, string>(groupedSessionWindowRepartitionTopicName)).WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(10L))).Count();
                }
                else
                {
                    selectKeyStream.GroupBy(kvMapper, Grouped.As<string, string>(groupedSessionWindowRepartitionTopicName)).WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(10L))).Count();
                }
            }

            return builder.Build().Describe().ToString();
        }

        private string BuildStreamGroupByKeyNoWindows(bool otherOperations, bool isGroupByKey)
        {
            var builder = new StreamsBuilder();

            IKStream<string, string> selectKeyStream = builder.Stream<string, string>("topic").SelectKey((k, v) => k + v);

            var groupByAndCountRepartitionTopicName = "kstream-grouping";
            if (isGroupByKey)
            {
                if (otherOperations)
                {
                    selectKeyStream.Filter((k, v) => true).MapValues(v => v).GroupByKey(Grouped.As<string, string>(groupByAndCountRepartitionTopicName)).Count();
                }
                else
                {
                    selectKeyStream.GroupByKey(Grouped.As<string, string>(groupByAndCountRepartitionTopicName)).Count();
                }
            }
            else
            {
                if (otherOperations)
                {
                    selectKeyStream.Filter((k, v) => true).MapValues(v => v).GroupBy(kvMapper, Grouped.As<string, string>(groupByAndCountRepartitionTopicName)).Count();
                }
                else
                {
                    selectKeyStream.GroupBy(kvMapper, Grouped.As<string, string>(groupByAndCountRepartitionTopicName)).Count();
                }
            }

            return builder.Build().Describe().ToString();
        }

        private string buildStreamJoin(bool includeOtherOperations)
        {
            var builder = new StreamsBuilder();
            IKStream<string, string> initialStreamOne = builder.Stream<string, string>("topic-one");
            IKStream<string, string> initialStreamTwo = builder.Stream<string, string>("topic-two");

            IKStream<string, string> updatedStreamOne;
            IKStream<string, string> updatedStreamTwo;

            if (includeOtherOperations)
            {
                // without naming the join, the repartition topic Name would change due to operator changing before join performed
                updatedStreamOne = initialStreamOne
                    .SelectKey((k, v) => k + v)
                    .Filter((k, v) => true)
                    .Peek((k, v) => Console.Out.WriteLine(k + v));

                updatedStreamTwo = initialStreamTwo.
                    SelectKey((k, v) => k + v).Filter((k, v) => true)
                    .Peek((k, v) => Console.Out.WriteLine(k + v));
            }
            else
            {
                updatedStreamOne = initialStreamOne.SelectKey((k, v) => k + v);
                updatedStreamTwo = initialStreamTwo.SelectKey((k, v) => k + v);
            }

            var joinRepartitionTopicName = "my-join";
            updatedStreamOne.Join(updatedStreamTwo, (v1, v2) => v1 + v2,
                    JoinWindows.Of(TimeSpan.FromMilliseconds(1000L)), Joined.With(Serdes.String(), Serdes.String(), Serdes.String(), joinRepartitionTopicName));

            return builder.Build().Describe().ToString();
        }


        private int getCountOfRepartitionTopicsFound(string topologyString, Regex repartitionTopicPattern)
        {
            var matcher = repartitionTopicPattern.Match(topologyString);
            List<string> repartitionTopicsFound = new List<string>();
            while (matcher.Success)
            {
                repartitionTopicsFound.Add(matcher.Value);
            }

            return repartitionTopicsFound.Count;
        }


        private Topology buildTopology(string optimizationConfig)
        {
            int initializer() => 0;
            int aggregator(string k, string v, int agg) => agg + v.Length;
            string reducer(string v1, string v2) => v1 + ":" + v2;
            List<string> processorValueCollector = new List<string>();

            StreamsBuilder builder = new StreamsBuilder();

            IKStream<string, string> sourceStream = builder.Stream(INPUT_TOPIC, Consumed.With(Serdes.String(), Serdes.String()));
            IKStream<string, string> mappedStream = sourceStream.Map((k, v) => KeyValuePair.Create(k.ToUpper(), v));

            mappedStream.Filter((k, v) => k.Equals("B"))
                .MapValues(v => v.ToUpper())
                .Process(() => new SimpleProcessor(processorValueCollector));

            IKStream<string, long> countStream = mappedStream
                .GroupByKey(Grouped.As<string, string>(firstRepartitionTopicName))
                .Count(Materialized.With<string, long, IKeyValueStore<Bytes, byte[]>>(Serdes.String(), Serdes.Long())).ToStream();

            countStream.To(COUNT_TOPIC, Produced.With(Serdes.String(), Serdes.Long()));

            mappedStream.GroupByKey(Grouped.As<string, string>(secondRepartitionTopicName))
                .Aggregate(initializer, aggregator,
                    Materialized.With(Serdes.String(), Serdes.Int()))
                    .ToStream()
                    .To(AGGREGATION_TOPIC, Produced.With(Serdes.String(), Serdes.Int()));

            // adding operators for case where the repartition node is further downstream
            mappedStream.Filter((k, v) => true).Peek((k, v) => Console.WriteLine(k + ":" + v))
                .GroupByKey(Grouped.As<string, string>(thirdRepartitionTopicName))
                    .Reduce(reducer, Materialized.With(Serdes.String(), Serdes.String()))
                    .ToStream()
                    .To(REDUCE_TOPIC, Produced.With(Serdes.String(), Serdes.String()));

            mappedStream.Filter((k, v) => k.Equals("A"))
                    .Join(countStream, (v1, v2) => v1 + ":" + v2.ToString(),
                            JoinWindows.Of(TimeSpan.FromMilliseconds(5000L)),
                            Joined.With(Serdes.String(), Serdes.String(), Serdes.Long(), fourthRepartitionTopicName))
                    .To(JOINED_TOPIC);

            var properties = new StreamsConfig
            {
                TopologyOptimization = TopologyOptimization.All,
            };

            return builder.Build(properties);
        }

        private class SimpleProcessor : AbstractProcessor<string, string>, IProcessorSupplier<string, string>
        {
            private readonly List<string> valueList;

            public SimpleProcessor(List<string> valueList)
            {
                this.valueList = valueList;
            }

            public IKeyValueProcessor<string, string> Get()
            {
                return this;
            }

            public override void Process(string key, string value)
            {
                valueList.Add(value);
            }

            IKeyValueProcessor IProcessorSupplier.Get()
            {
                return this;
            }
        }

        private static readonly string EXPECTED_OPTIMIZED_TOPOLOGY = "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n" +
                "      -=> KSTREAM-MAP-0000000001\n" +
                "    Processor: KSTREAM-MAP-0000000001 (stores: [])\n" +
                "      -=> KSTREAM-FILTER-0000000002, KSTREAM-FILTER-0000000040\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n" +
                "    Processor: KSTREAM-FILTER-0000000002 (stores: [])\n" +
                "      -=> KSTREAM-MAPVALUES-0000000003\n" +
                "      <-- KSTREAM-MAP-0000000001\n" +
                "    Processor: KSTREAM-FILTER-0000000040 (stores: [])\n" +
                "      -=> KSTREAM-SINK-0000000039\n" +
                "      <-- KSTREAM-MAP-0000000001\n" +
                "    Processor: KSTREAM-MAPVALUES-0000000003 (stores: [])\n" +
                "      -=> KSTREAM-PROCESSOR-0000000004\n" +
                "      <-- KSTREAM-FILTER-0000000002\n" +
                "    Processor: KSTREAM-PROCESSOR-0000000004 (stores: [])\n" +
                "      -=> none\n" +
                "      <-- KSTREAM-MAPVALUES-0000000003\n" +
                "    Sink: KSTREAM-SINK-0000000039 (topic: count-stream-repartition)\n" +
                "      <-- KSTREAM-FILTER-0000000040\n" +
                "\n" +
                "  Sub-topology: 1\n" +
                "    Source: KSTREAM-SOURCE-0000000041 (topics: [count-stream-repartition])\n" +
                "      -=> KSTREAM-FILTER-0000000020, KSTREAM-AGGREGATE-0000000007, KSTREAM-AGGREGATE-0000000014, KSTREAM-FILTER-0000000029\n" +
                "    Processor: KSTREAM-AGGREGATE-0000000007 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000006])\n" +
                "      -=> KTABLE-TOSTREAM-0000000011\n" +
                "      <-- KSTREAM-SOURCE-0000000041\n" +
                "    Processor: KTABLE-TOSTREAM-0000000011 (stores: [])\n" +
                "      -=> joined-stream-other-windowed, KSTREAM-SINK-0000000012\n" +
                "      <-- KSTREAM-AGGREGATE-0000000007\n" +
                "    Processor: KSTREAM-FILTER-0000000020 (stores: [])\n" +
                "      -=> KSTREAM-PEEK-0000000021\n" +
                "      <-- KSTREAM-SOURCE-0000000041\n" +
                "    Processor: KSTREAM-FILTER-0000000029 (stores: [])\n" +
                "      -=> joined-stream-this-windowed\n" +
                "      <-- KSTREAM-SOURCE-0000000041\n" +
                "    Processor: KSTREAM-PEEK-0000000021 (stores: [])\n" +
                "      -=> KSTREAM-REDUCE-0000000023\n" +
                "      <-- KSTREAM-FILTER-0000000020\n" +
                "    Processor: joined-stream-other-windowed (stores: [joined-stream-other-join-store])\n" +
                "      -=> joined-stream-other-join\n" +
                "      <-- KTABLE-TOSTREAM-0000000011\n" +
                "    Processor: joined-stream-this-windowed (stores: [joined-stream-this-join-store])\n" +
                "      -=> joined-stream-this-join\n" +
                "      <-- KSTREAM-FILTER-0000000029\n" +
                "    Processor: KSTREAM-AGGREGATE-0000000014 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000013])\n" +
                "      -=> KTABLE-TOSTREAM-0000000018\n" +
                "      <-- KSTREAM-SOURCE-0000000041\n" +
                "    Processor: KSTREAM-REDUCE-0000000023 (stores: [KSTREAM-REDUCE-STATE-STORE-0000000022])\n" +
                "      -=> KTABLE-TOSTREAM-0000000027\n" +
                "      <-- KSTREAM-PEEK-0000000021\n" +
                "    Processor: joined-stream-other-join (stores: [joined-stream-this-join-store])\n" +
                "      -=> joined-stream-merge\n" +
                "      <-- joined-stream-other-windowed\n" +
                "    Processor: joined-stream-this-join (stores: [joined-stream-other-join-store])\n" +
                "      -=> joined-stream-merge\n" +
                "      <-- joined-stream-this-windowed\n" +
                "    Processor: KTABLE-TOSTREAM-0000000018 (stores: [])\n" +
                "      -=> KSTREAM-SINK-0000000019\n" +
                "      <-- KSTREAM-AGGREGATE-0000000014\n" +
                "    Processor: KTABLE-TOSTREAM-0000000027 (stores: [])\n" +
                "      -=> KSTREAM-SINK-0000000028\n" +
                "      <-- KSTREAM-REDUCE-0000000023\n" +
                "    Processor: joined-stream-merge (stores: [])\n" +
                "      -=> KSTREAM-SINK-0000000038\n" +
                "      <-- joined-stream-this-join, joined-stream-other-join\n" +
                "    Sink: KSTREAM-SINK-0000000012 (topic: outputTopic_0)\n" +
                "      <-- KTABLE-TOSTREAM-0000000011\n" +
                "    Sink: KSTREAM-SINK-0000000019 (topic: outputTopic_1)\n" +
                "      <-- KTABLE-TOSTREAM-0000000018\n" +
                "    Sink: KSTREAM-SINK-0000000028 (topic: outputTopic_2)\n" +
                "      <-- KTABLE-TOSTREAM-0000000027\n" +
                "    Sink: KSTREAM-SINK-0000000038 (topic: outputTopicForJoin)\n" +
                "      <-- joined-stream-merge\n\n";


        private static readonly string EXPECTED_UNOPTIMIZED_TOPOLOGY = "Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n" +
                "      -=> KSTREAM-MAP-0000000001\n" +
                "    Processor: KSTREAM-MAP-0000000001 (stores: [])\n" +
                "      -=> KSTREAM-FILTER-0000000020, KSTREAM-FILTER-0000000002, KSTREAM-FILTER-0000000009, KSTREAM-FILTER-0000000016, KSTREAM-FILTER-0000000029\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n" +
                "    Processor: KSTREAM-FILTER-0000000020 (stores: [])\n" +
                "      -=> KSTREAM-PEEK-0000000021\n" +
                "      <-- KSTREAM-MAP-0000000001\n" +
                "    Processor: KSTREAM-FILTER-0000000002 (stores: [])\n" +
                "      -=> KSTREAM-MAPVALUES-0000000003\n" +
                "      <-- KSTREAM-MAP-0000000001\n" +
                "    Processor: KSTREAM-FILTER-0000000029 (stores: [])\n" +
                "      -=> KSTREAM-FILTER-0000000031\n" +
                "      <-- KSTREAM-MAP-0000000001\n" +
                "    Processor: KSTREAM-PEEK-0000000021 (stores: [])\n" +
                "      -=> KSTREAM-FILTER-0000000025\n" +
                "      <-- KSTREAM-FILTER-0000000020\n" +
                "    Processor: KSTREAM-FILTER-0000000009 (stores: [])\n" +
                "      -=> KSTREAM-SINK-0000000008\n" +
                "      <-- KSTREAM-MAP-0000000001\n" +
                "    Processor: KSTREAM-FILTER-0000000016 (stores: [])\n" +
                "      -=> KSTREAM-SINK-0000000015\n" +
                "      <-- KSTREAM-MAP-0000000001\n" +
                "    Processor: KSTREAM-FILTER-0000000025 (stores: [])\n" +
                "      -=> KSTREAM-SINK-0000000024\n" +
                "      <-- KSTREAM-PEEK-0000000021\n" +
                "    Processor: KSTREAM-FILTER-0000000031 (stores: [])\n" +
                "      -=> KSTREAM-SINK-0000000030\n" +
                "      <-- KSTREAM-FILTER-0000000029\n" +
                "    Processor: KSTREAM-MAPVALUES-0000000003 (stores: [])\n" +
                "      -=> KSTREAM-PROCESSOR-0000000004\n" +
                "      <-- KSTREAM-FILTER-0000000002\n" +
                "    Processor: KSTREAM-PROCESSOR-0000000004 (stores: [])\n" +
                "      -=> none\n" +
                "      <-- KSTREAM-MAPVALUES-0000000003\n" +
                "    Sink: KSTREAM-SINK-0000000008 (topic: count-stream-repartition)\n" +
                "      <-- KSTREAM-FILTER-0000000009\n" +
                "    Sink: KSTREAM-SINK-0000000015 (topic: aggregate-stream-repartition)\n" +
                "      <-- KSTREAM-FILTER-0000000016\n" +
                "    Sink: KSTREAM-SINK-0000000024 (topic: reduced-stream-repartition)\n" +
                "      <-- KSTREAM-FILTER-0000000025\n" +
                "    Sink: KSTREAM-SINK-0000000030 (topic: joined-stream-left-repartition)\n" +
                "      <-- KSTREAM-FILTER-0000000031\n" +
                "\n" +
                "  Sub-topology: 1\n" +
                "    Source: KSTREAM-SOURCE-0000000010 (topics: [count-stream-repartition])\n" +
                "      -=> KSTREAM-AGGREGATE-0000000007\n" +
                "    Processor: KSTREAM-AGGREGATE-0000000007 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000006])\n" +
                "      -=> KTABLE-TOSTREAM-0000000011\n" +
                "      <-- KSTREAM-SOURCE-0000000010\n" +
                "    Processor: KTABLE-TOSTREAM-0000000011 (stores: [])\n" +
                "      -=> KSTREAM-SINK-0000000012, joined-stream-other-windowed\n" +
                "      <-- KSTREAM-AGGREGATE-0000000007\n" +
                "    Source: KSTREAM-SOURCE-0000000032 (topics: [joined-stream-left-repartition])\n" +
                "      -=> joined-stream-this-windowed\n" +
                "    Processor: joined-stream-other-windowed (stores: [joined-stream-other-join-store])\n" +
                "      -=> joined-stream-other-join\n" +
                "      <-- KTABLE-TOSTREAM-0000000011\n" +
                "    Processor: joined-stream-this-windowed (stores: [joined-stream-this-join-store])\n" +
                "      -=> joined-stream-this-join\n" +
                "      <-- KSTREAM-SOURCE-0000000032\n" +
                "    Processor: joined-stream-other-join (stores: [joined-stream-this-join-store])\n" +
                "      -=> joined-stream-merge\n" +
                "      <-- joined-stream-other-windowed\n" +
                "    Processor: joined-stream-this-join (stores: [joined-stream-other-join-store])\n" +
                "      -=> joined-stream-merge\n" +
                "      <-- joined-stream-this-windowed\n" +
                "    Processor: joined-stream-merge (stores: [])\n" +
                "      -=> KSTREAM-SINK-0000000038\n" +
                "      <-- joined-stream-this-join, joined-stream-other-join\n" +
                "    Sink: KSTREAM-SINK-0000000012 (topic: outputTopic_0)\n" +
                "      <-- KTABLE-TOSTREAM-0000000011\n" +
                "    Sink: KSTREAM-SINK-0000000038 (topic: outputTopicForJoin)\n" +
                "      <-- joined-stream-merge\n" +
                "\n" +
                "  Sub-topology: 2\n" +
                "    Source: KSTREAM-SOURCE-0000000017 (topics: [aggregate-stream-repartition])\n" +
                "      -=> KSTREAM-AGGREGATE-0000000014\n" +
                "    Processor: KSTREAM-AGGREGATE-0000000014 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000013])\n" +
                "      -=> KTABLE-TOSTREAM-0000000018\n" +
                "      <-- KSTREAM-SOURCE-0000000017\n" +
                "    Processor: KTABLE-TOSTREAM-0000000018 (stores: [])\n" +
                "      -=> KSTREAM-SINK-0000000019\n" +
                "      <-- KSTREAM-AGGREGATE-0000000014\n" +
                "    Sink: KSTREAM-SINK-0000000019 (topic: outputTopic_1)\n" +
                "      <-- KTABLE-TOSTREAM-0000000018\n" +
                "\n" +
                "  Sub-topology: 3\n" +
                "    Source: KSTREAM-SOURCE-0000000026 (topics: [reduced-stream-repartition])\n" +
                "      -=> KSTREAM-REDUCE-0000000023\n" +
                "    Processor: KSTREAM-REDUCE-0000000023 (stores: [KSTREAM-REDUCE-STATE-STORE-0000000022])\n" +
                "      -=> KTABLE-TOSTREAM-0000000027\n" +
                "      <-- KSTREAM-SOURCE-0000000026\n" +
                "    Processor: KTABLE-TOSTREAM-0000000027 (stores: [])\n" +
                "      -=> KSTREAM-SINK-0000000028\n" +
                "      <-- KSTREAM-REDUCE-0000000023\n" +
                "    Sink: KSTREAM-SINK-0000000028 (topic: outputTopic_2)\n" +
                "      <-- KTABLE-TOSTREAM-0000000027\n\n";


    }
}
