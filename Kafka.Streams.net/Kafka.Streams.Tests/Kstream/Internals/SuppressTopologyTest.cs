namespace Kafka.Streams.Tests.Kstream.Internals
{
}
///*
//
//
//
//
//
//
// *
//
// *
//
//
//
//
//
// */
//using Kafka.Streams;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//

//namespace Kafka.Streams.KStream.Internals {
























//public class SuppressTopologyTest {
//    private static ISerde<string> STRING_SERDE = Serdes.String();

//    private static string NAMED_FINAL_TOPOLOGY = "Topologies:\n" +
//        "   Sub-topology: 0\n" +
//        "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n" +
//        "      -=> KSTREAM-KEY-SELECT-0000000001\n" +
//        "    Processor: KSTREAM-KEY-SELECT-0000000001 (stores: [])\n" +
//        "      -=> KSTREAM-FILTER-0000000004\n" +
//        "      <-- KSTREAM-SOURCE-0000000000\n" +
//        "    Processor: KSTREAM-FILTER-0000000004 (stores: [])\n" +
//        "      -=> KSTREAM-SINK-0000000003\n" +
//        "      <-- KSTREAM-KEY-SELECT-0000000001\n" +
//        "    Sink: KSTREAM-SINK-0000000003 (topic: counts-repartition)\n" +
//        "      <-- KSTREAM-FILTER-0000000004\n" +
//        "\n" +
//        "  Sub-topology: 1\n" +
//        "    Source: KSTREAM-SOURCE-0000000005 (topics: [counts-repartition])\n" +
//        "      -=> KSTREAM-AGGREGATE-0000000002\n" +
//        "    Processor: KSTREAM-AGGREGATE-0000000002 (stores: [counts])\n" +
//        "      -=> myname\n" +
//        "      <-- KSTREAM-SOURCE-0000000005\n" +
//        "    Processor: myname (stores: [myname-store])\n" +
//        "      -=> KTABLE-TOSTREAM-0000000006\n" +
//        "      <-- KSTREAM-AGGREGATE-0000000002\n" +
//        "    Processor: KTABLE-TOSTREAM-0000000006 (stores: [])\n" +
//        "      -=> KSTREAM-MAP-0000000007\n" +
//        "      <-- myname\n" +
//        "    Processor: KSTREAM-MAP-0000000007 (stores: [])\n" +
//        "      -=> KSTREAM-SINK-0000000008\n" +
//        "      <-- KTABLE-TOSTREAM-0000000006\n" +
//        "    Sink: KSTREAM-SINK-0000000008 (topic: output-suppressed)\n" +
//        "      <-- KSTREAM-MAP-0000000007\n" +
//        "\n";

//    private static string ANONYMOUS_FINAL_TOPOLOGY = "Topologies:\n" +
//        "   Sub-topology: 0\n" +
//        "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n" +
//        "      -=> KSTREAM-KEY-SELECT-0000000001\n" +
//        "    Processor: KSTREAM-KEY-SELECT-0000000001 (stores: [])\n" +
//        "      -=> KSTREAM-FILTER-0000000004\n" +
//        "      <-- KSTREAM-SOURCE-0000000000\n" +
//        "    Processor: KSTREAM-FILTER-0000000004 (stores: [])\n" +
//        "      -=> KSTREAM-SINK-0000000003\n" +
//        "      <-- KSTREAM-KEY-SELECT-0000000001\n" +
//        "    Sink: KSTREAM-SINK-0000000003 (topic: counts-repartition)\n" +
//        "      <-- KSTREAM-FILTER-0000000004\n" +
//        "\n" +
//        "  Sub-topology: 1\n" +
//        "    Source: KSTREAM-SOURCE-0000000005 (topics: [counts-repartition])\n" +
//        "      -=> KSTREAM-AGGREGATE-0000000002\n" +
//        "    Processor: KSTREAM-AGGREGATE-0000000002 (stores: [counts])\n" +
//        "      -=> KTABLE-SUPPRESS-0000000006\n" +
//        "      <-- KSTREAM-SOURCE-0000000005\n" +
//        "    Processor: KTABLE-SUPPRESS-0000000006 (stores: [KTABLE-SUPPRESS-STATE-STORE-0000000007])\n" +
//        "      -=> KTABLE-TOSTREAM-0000000008\n" +
//        "      <-- KSTREAM-AGGREGATE-0000000002\n" +
//        "    Processor: KTABLE-TOSTREAM-0000000008 (stores: [])\n" +
//        "      -=> KSTREAM-MAP-0000000009\n" +
//        "      <-- KTABLE-SUPPRESS-0000000006\n" +
//        "    Processor: KSTREAM-MAP-0000000009 (stores: [])\n" +
//        "      -=> KSTREAM-SINK-0000000010\n" +
//        "      <-- KTABLE-TOSTREAM-0000000008\n" +
//        "    Sink: KSTREAM-SINK-0000000010 (topic: output-suppressed)\n" +
//        "      <-- KSTREAM-MAP-0000000009\n" +
//        "\n";

//    private static string NAMED_INTERMEDIATE_TOPOLOGY = "Topologies:\n" +
//        "   Sub-topology: 0\n" +
//        "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n" +
//        "      -=> KSTREAM-AGGREGATE-0000000002\n" +
//        "    Processor: KSTREAM-AGGREGATE-0000000002 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000001])\n" +
//        "      -=>.Asdf\n" +
//        "      <-- KSTREAM-SOURCE-0000000000\n" +
//        "    Processor:.Asdf (stores: .Asdf-store])\n" +
//        "      -=> KTABLE-TOSTREAM-0000000003\n" +
//        "      <-- KSTREAM-AGGREGATE-0000000002\n" +
//        "    Processor: KTABLE-TOSTREAM-0000000003 (stores: [])\n" +
//        "      -=> KSTREAM-SINK-0000000004\n" +
//        "      <--.Asdf\n" +
//        "    Sink: KSTREAM-SINK-0000000004 (topic: output)\n" +
//        "      <-- KTABLE-TOSTREAM-0000000003\n" +
//        "\n";

//    private static string ANONYMOUS_INTERMEDIATE_TOPOLOGY = "Topologies:\n" +
//        "   Sub-topology: 0\n" +
//        "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n" +
//        "      -=> KSTREAM-AGGREGATE-0000000002\n" +
//        "    Processor: KSTREAM-AGGREGATE-0000000002 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000001])\n" +
//        "      -=> KTABLE-SUPPRESS-0000000003\n" +
//        "      <-- KSTREAM-SOURCE-0000000000\n" +
//        "    Processor: KTABLE-SUPPRESS-0000000003 (stores: [KTABLE-SUPPRESS-STATE-STORE-0000000004])\n" +
//        "      -=> KTABLE-TOSTREAM-0000000005\n" +
//        "      <-- KSTREAM-AGGREGATE-0000000002\n" +
//        "    Processor: KTABLE-TOSTREAM-0000000005 (stores: [])\n" +
//        "      -=> KSTREAM-SINK-0000000006\n" +
//        "      <-- KTABLE-SUPPRESS-0000000003\n" +
//        "    Sink: KSTREAM-SINK-0000000006 (topic: output)\n" +
//        "      <-- KTABLE-TOSTREAM-0000000005\n" +
//        "\n";


//    [Fact]
//    public void shouldUseNumberingForAnonymousFinalSuppressionNode() {
//        var anonymousNodeBuilder = new StreamsBuilder();
//        anonymousNodeBuilder
//            .Stream("input", Consumed.With(STRING_SERDE, STRING_SERDE))
//            .groupBy((string k, string v) => k, Grouped.with(STRING_SERDE, STRING_SERDE))
//            .windowedBy(SessionWindows.with(TimeSpan.FromMilliseconds(5L)).grace(TimeSpan.FromMilliseconds(5L)))
//            .count(Materialize.As<string, long, ISessionStore<Bytes, byte[]>("counts").withCachingDisabled())
//            .suppress(untilWindowCloses(unbounded()))
//            .toStream()
//            .map((IWindowed<string> k, long v) => KeyValuePair.Create(k.ToString(), v))
//            .To("output-suppressed", Produced.With(STRING_SERDE, Serdes.Long()));
//        string anonymousNodeTopology = anonymousNodeBuilder.Build().describe().ToString();

//        // without the Name, the suppression node increments the topology index
//        Assert.Equal(anonymousNodeTopology, (ANONYMOUS_FINAL_TOPOLOGY));
//    }

//    [Fact]
//    public void shouldApplyNameToFinalSuppressionNode() {
//        var namedNodeBuilder = new StreamsBuilder();
//        namedNodeBuilder
//            .Stream("input", Consumed.With(STRING_SERDE, STRING_SERDE))
//            .groupBy((string k, string v) => k, Grouped.with(STRING_SERDE, STRING_SERDE))
//            .windowedBy(SessionWindows.with(TimeSpan.FromMilliseconds(5L)).grace(TimeSpan.FromMilliseconds(5L)))
//            .count(Materialize.As<string, long, ISessionStore<Bytes, byte[]>("counts").withCachingDisabled())
//            .suppress(untilWindowCloses(unbounded()).withName("myname"))
//            .toStream()
//            .map((IWindowed<string> k, long v) => KeyValuePair.Create(k.ToString(), v))
//            .To("output-suppressed", Produced.With(STRING_SERDE, Serdes.Long()));
//        string namedNodeTopology = namedNodeBuilder.Build().describe().ToString();

//        // without the Name, the suppression node does not increment the topology index
//        Assert.Equal(namedNodeTopology, (NAMED_FINAL_TOPOLOGY));
//    }

//    [Fact]
//    public void shouldUseNumberingForAnonymousSuppressionNode() {
//        var anonymousNodeBuilder = new StreamsBuilder();
//        anonymousNodeBuilder
//            .Stream("input", Consumed.With(STRING_SERDE, STRING_SERDE))
//            .groupByKey()
//            .count()
//            .suppress(untilTimeLimit(TimeSpan.FromSeconds(1), unbounded()))
//            .toStream()
//            .To("output", Produced.With(STRING_SERDE, Serdes.Long()));
//        string anonymousNodeTopology = anonymousNodeBuilder.Build().describe().ToString();

//        // without the Name, the suppression node increments the topology index
//        Assert.Equal(anonymousNodeTopology, (ANONYMOUS_INTERMEDIATE_TOPOLOGY));
//    }

//    [Fact]
//    public void shouldApplyNameToSuppressionNode() {
//        var namedNodeBuilder = new StreamsBuilder();
//        namedNodeBuilder
//            .Stream("input", Consumed.With(STRING_SERDE, STRING_SERDE))
//            .groupByKey()
//            .count()
//            .suppress(untilTimeLimit(TimeSpan.ofSeconds(1), unbounded()).withName("asdf"))
//            .toStream()
//            .To("output", Produced.With(STRING_SERDE, Serdes.Long()));
//        string namedNodeTopology = namedNodeBuilder.Build().describe().ToString();

//        // without the Name, the suppression node does not increment the topology index
//        Assert.Equal(namedNodeTopology, (NAMED_INTERMEDIATE_TOPOLOGY));
//    }
//}
