using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Topologies;
using System.Collections.Generic;
using System.Text.RegularExpressions;

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

            IKStream<K, V>("topic");
            IKStream<K, V>("other-topic");
            IValueJoiner<string, string, string> valueJoiner = (v, v2) => v + v2;


            IKStream<K, V> joinedStream = stream.Join(streamII, valueJoiner, JoinWindows.of(TimeSpan.FromMilliseconds(5000)));

            // build step one
            Assert.Equal(expectedJoinedTopology, builder.Build().describe().ToString());

            IKStream<K, V> v.Equals("foo"));
            // build step two
            Assert.Equal(expectedJoinedFilteredTopology, builder.Build().describe().ToString());

            filteredJoinStream.MapValues(v => v + "some value").To("output-topic");
            // build step three
            Assert.Equal(expectedFullTopology, builder.Build().describe().ToString());

        }

        [Fact]
        public void shouldBeAbleToProcessNestedMultipleKeyChangingNodes()
        {
            var properties = new StreamsConfig();
            properties.Set(StreamsConfig.ApplicationId, "test-application");
            properties.Set(StreamsConfig.BootstrapServers, "localhost:9092");
            properties.Set(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

            var builder = new StreamsBuilder();
            IKStream<K, V>("inputTopic");

            IKStream<K, V> v.substring(0, 5));

            // first repartition
            changedKeyStream.GroupByKey(Grouped.As("count-repartition"))
                .Count(Materialized.As("count-store"))
                .ToStream().To("count-topic", Produced.With(Serdes.String(), Serdes.Long()));

            // second repartition
            changedKeyStream.GroupByKey(Grouped.As("windowed-repartition"))
                .WindowedBy(TimeWindows.of(TimeSpan.FromSeconds(5)))
                .Count(Materialized.As("windowed-count-store"))
                .ToStream()
                .map((k, v) => KeyValuePair.Create(k.Key, v)).To("windowed-count", Produced.With(Serdes.String(), Serdes.Long()));

            builder.Build(properties);
        }

        [Fact]
        public void shouldNotOptimizeWithValueOrKeyChangingOperatorsAfterInitialKeyChange()
        {

            Topology attemptedOptimize = getTopologyWithChangingValuesAfterChangingKey(StreamsConfig.OPTIMIZE);
            Topology noOptimization = getTopologyWithChangingValuesAfterChangingKey(StreamsConfig.NO_OPTIMIZATION);

            Assert.Equal(attemptedOptimize.describe().ToString(), noOptimization.describe().ToString());
            Assert.Equal(2, getCountOfRepartitionTopicsFound(attemptedOptimize.describe().ToString()));
            Assert.Equal(2, getCountOfRepartitionTopicsFound(noOptimization.describe().ToString()));
        }

        // no need to optimize.As user .As already performed the repartitioning manually
        [Fact]
        public void shouldNotOptimizeWhenAThroughOperationIsDone()
        {

            Topology attemptedOptimize = getTopologyWithThroughOperation(StreamsConfig.OPTIMIZE);
            Topology noOptimziation = getTopologyWithThroughOperation(StreamsConfig.NoOptimization);

            Assert.Equal(attemptedOptimize.describe().ToString(), noOptimziation.describe().ToString());
            Assert.Equal(0, getCountOfRepartitionTopicsFound(attemptedOptimize.describe().ToString()));
            Assert.Equal(0, getCountOfRepartitionTopicsFound(noOptimziation.describe().ToString()));

        }

        private Topology getTopologyWithChangingValuesAfterChangingKey(string optimizeConfig)
        {

            var builder = new StreamsBuilder();
            var properties = new StreamsConfig();
            properties.Set(StreamsConfig.TOPOLOGY_OPTIMIZATION, optimizeConfig);

            IKStream<K, V>("input");
            IKStream<K, V> k + v);

            mappedKeyStream.MapValues(v => v.toUppercase(Locale.getDefault())).GroupByKey().Count().ToStream().To("output");
            mappedKeyStream.flatMapValues(v => new List<string> { v.Split("\\s" })).GroupByKey().WindowedBy(TimeWindows.of(TimeSpan.FromMilliseconds(5000))).Count().ToStream().To("windowed-output");

            return builder.Build(properties);

        }

        private Topology getTopologyWithThroughOperation(string optimizeConfig)
        {

            var builder = new StreamsBuilder();
            var properties = new StreamsConfig();
            properties.Set(StreamsConfig.TOPOLOGY_OPTIMIZATION, optimizeConfig);

            IKStream<K, V>("input");
            IKStream<K, V> k + v).through("through-topic");

            mappedKeyStream.GroupByKey().Count().ToStream().To("output");
            mappedKeyStream.GroupByKey().WindowedBy(TimeWindows.of(TimeSpan.FromMilliseconds(5000))).Count().ToStream().To("windowed-output");

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
