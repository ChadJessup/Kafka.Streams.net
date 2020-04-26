using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Temporary;
using Kafka.Streams.Topologies;
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals.Graph
{
    public class StreamsGraphTest
    {
        private Regex repartitionTopicPattern = new Regex("Sink: .*-repartition", RegexOptions.Compiled);

        // Test builds topology in succesive manner but only graph node not yet processed written to topology

        [Fact]
        public void shouldBeAbleToBuildTopologyIncrementally()
        {
            var builder = new StreamsBuilder();

            IKStream<string, string> stream = builder.Stream<string, string>("topic");
            IKStream<string, string> streamII = builder.Stream<string, string>("other-topic");
            ValueJoiner<string, string, string> valueJoiner = (v, v2) => v + v2;
            IKStream<string, string> joinedStream = stream.Join(streamII, valueJoiner, JoinWindows.Of(TimeSpan.FromMilliseconds(5000)));

            // build step one
            Assert.Equal(expectedJoinedTopology, builder.Build().Describe().ToString());

            IKStream<string, string> filteredJoinStream = joinedStream.Filter((k, v) => v.Equals("foo"));
            // build step two
            Assert.Equal(expectedJoinedFilteredTopology, builder.Build().Describe().ToString());

            filteredJoinStream.MapValues(v => v + "some value").To("output-topic");
            // build step three
            Assert.Equal(expectedFullTopology, builder.Build().Describe().ToString());

        }

        [Fact]
        public void shouldBeAbleToProcessNestedMultipleKeyChangingNodes()
        {
            var properties = new StreamsConfig
            {
                ApplicationId = "test-application",
                BootstrapServers = "localhost:9092",
                TOPOLOGY_OPTIMIZATION = StreamsConfig.OPTIMIZE
            };

            var builder = new StreamsBuilder();
            IKStream<string, string> inputStream = builder.Stream("inputTopic");
            IKStream<string, string> changedKeyStream = inputStream.SelectKey((k, v) => v.Substring(0, 5));

            // first repartition
            changedKeyStream.GroupByKey(Grouped.As("count-repartition"))
                .Count(Materialized.As("count-store"))
                .ToStream().To("count-topic", Produced.With(Serdes.String(), Serdes.Long()));

            // second repartition
            changedKeyStream.GroupByKey(Grouped.As("windowed-repartition"))
                .WindowedBy(TimeWindows.Of(TimeSpan.FromSeconds(5)))
                .Count(Materialized.As("windowed-count-store"))
                .ToStream()
                .Map((k, v) => KeyValuePair.Create(k.Key, v)).To("windowed-count", Produced.With(Serdes.String(), Serdes.Long()));

            builder.Build(properties);
        }

        [Fact]
        public void shouldNotOptimizeWithValueOrKeyChangingOperatorsAfterInitialKeyChange()
        {

            Topology attemptedOptimize = getTopologyWithChangingValuesAfterChangingKey(StreamsConfig.OPTIMIZE);
            Topology noOptimization = getTopologyWithChangingValuesAfterChangingKey(StreamsConfig.NO_OPTIMIZATION);

            Assert.Equal(attemptedOptimize.Describe().ToString(), noOptimization.Describe().ToString());
            Assert.Equal(2, getCountOfRepartitionTopicsFound(attemptedOptimize.Describe().ToString()));
            Assert.Equal(2, getCountOfRepartitionTopicsFound(noOptimization.Describe().ToString()));
        }

        // no need to optimize.As user .As already performed the repartitioning manually
        [Fact]
        public void shouldNotOptimizeWhenAThroughOperationIsDone()
        {

            Topology attemptedOptimize = getTopologyWithThroughOperation(StreamsConfig.OPTIMIZE);
            Topology noOptimziation = getTopologyWithThroughOperation(StreamsConfig.NoOptimization);

            Assert.Equal(attemptedOptimize.Describe().ToString(), noOptimziation.Describe().ToString());
            Assert.Equal(0, getCountOfRepartitionTopicsFound(attemptedOptimize.Describe().ToString()));
            Assert.Equal(0, getCountOfRepartitionTopicsFound(noOptimziation.Describe().ToString()));

        }

        private Topology getTopologyWithChangingValuesAfterChangingKey(string optimizeConfig)
        {

            var builder = new StreamsBuilder();
            var properties = new StreamsConfig();
            properties.Set(StreamsConfig.TOPOLOGY_OPTIMIZATION, optimizeConfig);

            IKStream<string, string> inputStream = builder.Stream("input");
            IKStream<string, string> mappedKeyStream = inputStream.SelectKey((k, v) => k + v);

            mappedKeyStream.MapValues(v => v.ToUpper())
                .GroupByKey()
                .Count()
                .ToStream()
                .To("output");

            mappedKeyStream.FlatMapValues(v => Arrays.asList(v.Split("\\s")))
                .GroupByKey()
                .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(5000)))
                .Count()
                .ToStream()
                .To("windowed-output");

            return builder.Build(properties);
        }

        private Topology getTopologyWithThroughOperation(string optimizeConfig)
        {

            var builder = new StreamsBuilder();
            var properties = new StreamsConfig();
            properties.TOPOLOGY_OPTIMIZATION = optimizeConfig;

            IKStream<string, string> inputStream = builder.Stream("input");
            IKStream<string, string> mappedKeyStream = inputStream.SelectKey((k, v) => k + v).Through("through-topic");

            mappedKeyStream.GroupByKey().Count().ToStream().To("output");
            mappedKeyStream.GroupByKey().WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(5000))).Count().ToStream().To("windowed-output");

            return builder.Build(properties);

        }

        private int getCountOfRepartitionTopicsFound(string topologyString)
        {
            Matcher matcher = repartitionTopicPattern.matcher(topologyString);
            List<string> repartitionTopicsFound = new List<string>();
            while (matcher.find())
            {
                repartitionTopicsFound.Add(matcher.group());
            }

            return repartitionTopicsFound.Count;
        }

        private string expectedJoinedTopology = "Topologies:\n"
                                                + "   Sub-topology: 0\n"
                                                + "    Source: KSTREAM-SOURCE-0000000000 (topics: [topic])\n"
                                                + "      -=> KSTREAM-WINDOWED-0000000002\n"
                                                + "    Source: KSTREAM-SOURCE-0000000001 (topics: [other-topic])\n"
                                                + "      -=> KSTREAM-WINDOWED-0000000003\n"
                                                + "    Processor: KSTREAM-WINDOWED-0000000002 (stores: [KSTREAM-JOINTHIS-0000000004-store])\n"
                                                + "      -=> KSTREAM-JOINTHIS-0000000004\n"
                                                + "      <-- KSTREAM-SOURCE-0000000000\n"
                                                + "    Processor: KSTREAM-WINDOWED-0000000003 (stores: [KSTREAM-JOINOTHER-0000000005-store])\n"
                                                + "      -=> KSTREAM-JOINOTHER-0000000005\n"
                                                + "      <-- KSTREAM-SOURCE-0000000001\n"
                                                + "    Processor: KSTREAM-JOINOTHER-0000000005 (stores: [KSTREAM-JOINTHIS-0000000004-store])\n"
                                                + "      -=> KSTREAM-MERGE-0000000006\n"
                                                + "      <-- KSTREAM-WINDOWED-0000000003\n"
                                                + "    Processor: KSTREAM-JOINTHIS-0000000004 (stores: [KSTREAM-JOINOTHER-0000000005-store])\n"
                                                + "      -=> KSTREAM-MERGE-0000000006\n"
                                                + "      <-- KSTREAM-WINDOWED-0000000002\n"
                                                + "    Processor: KSTREAM-MERGE-0000000006 (stores: [])\n"
                                                + "      -=> none\n"
                                                + "      <-- KSTREAM-JOINTHIS-0000000004, KSTREAM-JOINOTHER-0000000005\n\n";

        private string expectedJoinedFilteredTopology = "Topologies:\n"
                                                        + "   Sub-topology: 0\n"
                                                        + "    Source: KSTREAM-SOURCE-0000000000 (topics: [topic])\n"
                                                        + "      -=> KSTREAM-WINDOWED-0000000002\n"
                                                        + "    Source: KSTREAM-SOURCE-0000000001 (topics: [other-topic])\n"
                                                        + "      -=> KSTREAM-WINDOWED-0000000003\n"
                                                        + "    Processor: KSTREAM-WINDOWED-0000000002 (stores: [KSTREAM-JOINTHIS-0000000004-store])\n"
                                                        + "      -=> KSTREAM-JOINTHIS-0000000004\n"
                                                        + "      <-- KSTREAM-SOURCE-0000000000\n"
                                                        + "    Processor: KSTREAM-WINDOWED-0000000003 (stores: [KSTREAM-JOINOTHER-0000000005-store])\n"
                                                        + "      -=> KSTREAM-JOINOTHER-0000000005\n"
                                                        + "      <-- KSTREAM-SOURCE-0000000001\n"
                                                        + "    Processor: KSTREAM-JOINOTHER-0000000005 (stores: [KSTREAM-JOINTHIS-0000000004-store])\n"
                                                        + "      -=> KSTREAM-MERGE-0000000006\n"
                                                        + "      <-- KSTREAM-WINDOWED-0000000003\n"
                                                        + "    Processor: KSTREAM-JOINTHIS-0000000004 (stores: [KSTREAM-JOINOTHER-0000000005-store])\n"
                                                        + "      -=> KSTREAM-MERGE-0000000006\n"
                                                        + "      <-- KSTREAM-WINDOWED-0000000002\n"
                                                        + "    Processor: KSTREAM-MERGE-0000000006 (stores: [])\n"
                                                        + "      -=> KSTREAM-FILTER-0000000007\n"
                                                        + "      <-- KSTREAM-JOINTHIS-0000000004, KSTREAM-JOINOTHER-0000000005\n"
                                                        + "    Processor: KSTREAM-FILTER-0000000007 (stores: [])\n"
                                                        + "      -=> none\n"
                                                        + "      <-- KSTREAM-MERGE-0000000006\n\n";

        private string expectedFullTopology = "Topologies:\n"
                                              + "   Sub-topology: 0\n"
                                              + "    Source: KSTREAM-SOURCE-0000000000 (topics: [topic])\n"
                                              + "      -=> KSTREAM-WINDOWED-0000000002\n"
                                              + "    Source: KSTREAM-SOURCE-0000000001 (topics: [other-topic])\n"
                                              + "      -=> KSTREAM-WINDOWED-0000000003\n"
                                              + "    Processor: KSTREAM-WINDOWED-0000000002 (stores: [KSTREAM-JOINTHIS-0000000004-store])\n"
                                              + "      -=> KSTREAM-JOINTHIS-0000000004\n"
                                              + "      <-- KSTREAM-SOURCE-0000000000\n"
                                              + "    Processor: KSTREAM-WINDOWED-0000000003 (stores: [KSTREAM-JOINOTHER-0000000005-store])\n"
                                              + "      -=> KSTREAM-JOINOTHER-0000000005\n"
                                              + "      <-- KSTREAM-SOURCE-0000000001\n"
                                              + "    Processor: KSTREAM-JOINOTHER-0000000005 (stores: [KSTREAM-JOINTHIS-0000000004-store])\n"
                                              + "      -=> KSTREAM-MERGE-0000000006\n"
                                              + "      <-- KSTREAM-WINDOWED-0000000003\n"
                                              + "    Processor: KSTREAM-JOINTHIS-0000000004 (stores: [KSTREAM-JOINOTHER-0000000005-store])\n"
                                              + "      -=> KSTREAM-MERGE-0000000006\n"
                                              + "      <-- KSTREAM-WINDOWED-0000000002\n"
                                              + "    Processor: KSTREAM-MERGE-0000000006 (stores: [])\n"
                                              + "      -=> KSTREAM-FILTER-0000000007\n"
                                              + "      <-- KSTREAM-JOINTHIS-0000000004, KSTREAM-JOINOTHER-0000000005\n"
                                              + "    Processor: KSTREAM-FILTER-0000000007 (stores: [])\n"
                                              + "      -=> KSTREAM-MAPVALUES-0000000008\n"
                                              + "      <-- KSTREAM-MERGE-0000000006\n"
                                              + "    Processor: KSTREAM-MAPVALUES-0000000008 (stores: [])\n"
                                              + "      -=> KSTREAM-SINK-0000000009\n"
                                              + "      <-- KSTREAM-FILTER-0000000007\n"
                                              + "    Sink: KSTREAM-SINK-0000000009 (topic: output-topic)\n"
                                              + "      <-- KSTREAM-MAPVALUES-0000000008\n\n";
    }
}
