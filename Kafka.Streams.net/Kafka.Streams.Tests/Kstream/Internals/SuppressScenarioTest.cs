//using Confluent.Kafka;
//using Kafka.Streams;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.Tests.Helpers;
//using Kafka.Streams.Topologies;
//using Microsoft.VisualBasic.CompilerServices;
//using System;
//using System.Collections.Generic;
//using System.Text;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class SuppressScenarioTest
//    {
//        private static StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
//        private static StringSerializer STRING_SERIALIZER = Serdes.String();
//        private static ISerde<string> STRING_SERDE = Serdes.String();
//        private static LongDeserializer LONG_DESERIALIZER = new LongDeserializer();
//        private StreamsConfig config = Utils.mkProperties(Utils.mkMap(
//            Utils.mkEntry(StreamsConfigPropertyNames.ApplicationId, typeof(SuppressScenarioTest).FullName.ToLower()),
//            Utils.mkEntry(StreamsConfigPropertyNames.STATE_DIR_CONFIG, TestUtils.GetTempDirectory()),
//            Utils.mkEntry(StreamsConfigPropertyNames.BootstrapServers, "bogus")
//        ));

//        [Fact]
//        public void shouldImmediatelyEmitEventsWithZeroEmitAfter()
//        {
//            var builder = new StreamsBuilder();

//            IKTable<string, long> valueCounts = builder
//                .Table(
//                    "input",
//                    Consumed.with(STRING_SERDE, STRING_SERDE),
//                    Materialized.with<string, string, IKeyValueStore<Bytes, byte[]>>(STRING_SERDE, STRING_SERDE)
//                        .withCachingDisabled()
//                        .withLoggingDisabled()
//                )
//                .groupBy((k, v) => new KeyValuePair<>(v, k), Grouped.with(STRING_SERDE, STRING_SERDE))
//                .count();

//            valueCounts
//                .suppress(untilTimeLimit(ZERO, unbounded()))
//                .toStream()
//                .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));

//            valueCounts
//                .toStream()
//                .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));

//            Topology topology = builder.Build();


//            ConsumerRecordFactory<string, string> recordFactory =
//                new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//            var driver = new TopologyTestDriver(topology, config);
//            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
//            driver.pipeInput(recordFactory.create("input", "k1", "v2", 1L));
//            driver.pipeInput(recordFactory.create("input", "k2", "v1", 2L));
//            verify(
//            drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//           asList(
//                new KeyValueTimestamp<>("v1", 1L, 0L),
//                new KeyValueTimestamp<>("v1", 0L, 1L),
//                new KeyValueTimestamp<>("v2", 1L, 1L),
//                new KeyValueTimestamp<>("v1", 1L, 2L)
//            )
//            );
//            verify(
//            drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//           asList(
//                new KeyValueTimestamp<>("v1", 1L, 0L),
//                new KeyValueTimestamp<>("v1", 0L, 1L),
//                new KeyValueTimestamp<>("v2", 1L, 1L),
//                new KeyValueTimestamp<>("v1", 1L, 2L)
//            )
//            );
//            driver.pipeInput(recordFactory.create("input", "x", "x", 3L));
//            verify(
//            drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//            singletonList(
//                new KeyValueTimestamp<>("x", 1L, 3L)
//            )
//            );
//            verify(
//            drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//            singletonList(
//                new KeyValueTimestamp<>("x", 1L, 3L)
//            )
//            );
//            driver.pipeInput(recordFactory.create("input", "x", "x", 4L));
//            verify(
//            drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//           asList(
//                new KeyValueTimestamp<>("x", 0L, 4L),
//                new KeyValueTimestamp<>("x", 1L, 4L)
//            )
//            );
//            verify(
//            drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//           asList(
//                new KeyValueTimestamp<>("x", 0L, 4L),
//                new KeyValueTimestamp<>("x", 1L, 4L)
//            )
//            );
//        }
//    }

//    [Fact]
//    public void shouldSuppressIntermediateEventsWithTimeLimit()
//    {
//        var builder = new StreamsBuilder();
//        IKTable<string, long> valueCounts = builder
//            .Table(
//                "input",
//                Consumed.with(STRING_SERDE, STRING_SERDE),
//                Materialized.with<string, string, IKeyValueStore<Bytes, byte[]>>(STRING_SERDE, STRING_SERDE)
//                    .withCachingDisabled()
//                    .withLoggingDisabled()
//            )
//            .groupBy((k, v) => new KeyValuePair<>(v, k), Grouped.with(STRING_SERDE, STRING_SERDE))
//            .count();
//        valueCounts
//            .suppress(untilTimeLimit(Duration.FromMilliseconds(2L), unbounded()))
//            .toStream()
//            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));
//        valueCounts
//            .toStream()
//            .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));
//        Topology topology = builder.Build();
//        ConsumerRecordFactory<string, string> recordFactory =
//            new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//        var driver = new TopologyTestDriver(topology, config);
//        driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
//        driver.pipeInput(recordFactory.create("input", "k1", "v2", 1L));
//        driver.pipeInput(recordFactory.create("input", "k2", "v1", 2L));

//        verify(
//        drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//       asList(
//            new KeyValueTimestamp<>("v1", 1L, 0L),
//            new KeyValueTimestamp<>("v1", 0L, 1L),
//            new KeyValueTimestamp<>("v2", 1L, 1L),
//            new KeyValueTimestamp<>("v1", 1L, 2L)
//        )
//        );

//        verify(
//        drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//        singletonList(new KeyValueTimestamp<>("v1", 1L, 2L))
//        );
//        // inserting a dummy "tick" record just to advance stream time
//        driver.pipeInput(recordFactory.create("input", "tick", "tick", 3L));

//        verify(
//        drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//        singletonList(new KeyValueTimestamp<>("tick", 1L, 3L))
//        );

//        // the stream time is now 3, so it's time to emit this record
//        verify(
//        drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//        singletonList(new KeyValueTimestamp<>("v2", 1L, 1L))
//        );


//        driver.pipeInput(recordFactory.create("input", "tick", "tick", 4L));
//        verify(
//        drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//       asList(
//            new KeyValueTimestamp<>("tick", 0L, 4L),
//            new KeyValueTimestamp<>("tick", 1L, 4L)
//        )
//        );
//        // tick is still buffered, since it .As first inserted at time 3, and it is only time 4 right now.
//        verify(
//        drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//        emptyList()
//    );
//    }
//}

//[Fact]
//public void shouldSuppressIntermediateEventsWithRecordLimit()
//{
//    var builder = new StreamsBuilder();
//    IKTable<string, long> valueCounts = builder
//        .Table(
//            "input",
//            Consumed.with(STRING_SERDE, STRING_SERDE),
//            Materialized..with<string, string, IKeyValueStore<Bytes, byte[]>>(STRING_SERDE, STRING_SERDE)
//                .withCachingDisabled()
//                .withLoggingDisabled()
//        )
//        .groupBy((k, v) => new KeyValuePair<>(v, k), Grouped.with(STRING_SERDE, STRING_SERDE))
//        .count(Materialized.with(STRING_SERDE, Serdes.Long()));
//    valueCounts
//        .suppress(untilTimeLimit(Duration.FromMilliseconds(Long.MaxValue), maxRecords(1L).emitEarlyWhenFull()))
//        .toStream()
//        .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));
//    valueCounts
//        .toStream()
//        .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));
//    Topology topology = builder.Build();
//    System.Console.Out.println(topology.describe());
//    ConsumerRecordFactory<string, string> recordFactory =
//        new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//    var driver = new TopologyTestDriver(topology, config);
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
//    driver.pipeInput(recordFactory.create("input", "k1", "v2", 1L));
//    driver.pipeInput(recordFactory.create("input", "k2", "v1", 2L));
//    verify(
//      drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//     asList(
//          new KeyValueTimestamp<>("v1", 1L, 0L),
//          new KeyValueTimestamp<>("v1", 0L, 1L),
//          new KeyValueTimestamp<>("v2", 1L, 1L),
//          new KeyValueTimestamp<>("v1", 1L, 2L)
//      )
//      );

//    verify(
//    drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//   asList(
//        // consecutive updates to v1 get suppressed into only the latter.
//        new KeyValueTimestamp<>("v1", 0L, 1L),
//        new KeyValueTimestamp<>("v2", 1L, 1L)
//    // the .Ast update won't be evicted until another key comes along.
//    )
//    );
//    driver.pipeInput(recordFactory.create("input", "x", "x", 3L));

//    verify(
//    drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//    singletonList(
//        new KeyValueTimestamp<>("x", 1L, 3L)
//    )
//    );
//    verify(
//    drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//    singletonList(
//        // now we see that .Ast update to v1, but we won't see the update to x until it gets evicted
//        new KeyValueTimestamp<>("v1", 1L, 2L)
//    )
//    );
//}
//}

//[Fact]
//public void shouldSuppressIntermediateEventsWithBytesLimit()
//{
//    var builder = new StreamsBuilder();
//    IKTable<string, long> valueCounts = builder
//        .Table(
//            "input",
//            Consumed.with(STRING_SERDE, STRING_SERDE),
//            Materialized.with<string, string, IKeyValueStore<Bytes, byte[]>>(STRING_SERDE, STRING_SERDE)
//                .withCachingDisabled()
//                .withLoggingDisabled()
//        )
//        .groupBy((k, v) => new KeyValuePair<>(v, k), Grouped.with(STRING_SERDE, STRING_SERDE))
//        .count();
//    valueCounts
//        // this is a bit brittle, but I happen to know that the entries are a little over 100 bytes in size.
//        .suppress(untilTimeLimit(Duration.FromMilliseconds(Long.MaxValue), maxBytes(200L).emitEarlyWhenFull()))
//        .toStream()
//        .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));
//    valueCounts
//        .toStream()
//        .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));
//    Topology topology = builder.Build();
//    System.Console.Out.WriteLine(topology.describe());
//    ConsumerRecordFactory<string, string> recordFactory =
//        new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//    var driver = new TopologyTestDriver(topology, config);
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
//    driver.pipeInput(recordFactory.create("input", "k1", "v2", 1L));
//    driver.pipeInput(recordFactory.create("input", "k2", "v1", 2L));

//    verify(
//    drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//   asList(
//        new KeyValueTimestamp<>("v1", 1L, 0L),
//        new KeyValueTimestamp<>("v1", 0L, 1L),
//        new KeyValueTimestamp<>("v2", 1L, 1L),
//        new KeyValueTimestamp<>("v1", 1L, 2L)
//    )
//    );
//    verify(
//    drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//   asList(
//        // consecutive updates to v1 get suppressed into only the latter.
//        new KeyValueTimestamp<>("v1", 0L, 1L),
//        new KeyValueTimestamp<>("v2", 1L, 1L)
//    // the .Ast update won't be evicted until another key comes along.
//    )
//    );
//    driver.pipeInput(recordFactory.create("input", "x", "x", 3L));
//    verify(
//    drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//    singletonList(
//        new KeyValueTimestamp<>("x", 1L, 3L)
//    )
//    );

//    verify(
//    drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//    singletonList(
//        // now we see that .Ast update to v1, but we won't see the update to x until it gets evicted
//        new KeyValueTimestamp<>("v1", 1L, 2L)
//    )
//    );
//}
//}

//[Fact]
//public void shouldSupportFinalResultsForTimeWindows()
//{
//    var builder = new StreamsBuilder();
//    IKTable<Windowed<string>, long> valueCounts = builder
//        .Stream("input", Consumed.with(STRING_SERDE, STRING_SERDE))
//        .groupBy((string k, string v) => k, Grouped.with(STRING_SERDE, STRING_SERDE))
//        .windowedBy(TimeWindows.of(Duration.FromMilliseconds(2L)).grace(Duration.FromMilliseconds(1L)))
//        .count(Materialize.As < string, long, IWindowStore<Bytes, byte[]>("counts").withCachingDisabled());
//    valueCounts
//        .suppress(untilWindowCloses(unbounded()))
//        .toStream()
//        .map((Windowed<string> k, long v) => new KeyValuePair<>(k.ToString(), v))
//        .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));
//    valueCounts
//        .toStream()
//        .map((Windowed<string> k, long v) => new KeyValuePair<>(k.ToString(), v))
//        .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));
//    Topology topology = builder.Build();
//    System.Console.Out.println(topology.describe());
//    ConsumerRecordFactory<string, string> recordFactory =
//        new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//    var driver = new TopologyTestDriver(topology, config);
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 1L));
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 2L));
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 1L));
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 5L));
//    // note this .Ast record gets dropped because it is out of the grace period
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
//    verify(
//    drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//               asList(
//                    new KeyValueTimestamp<>("[k1@0/2]", 1L, 0L),
//                    new KeyValueTimestamp<>("[k1@0/2]", 2L, 1L),
//                    new KeyValueTimestamp<>("[k1@2/4]", 1L, 2L),
//                    new KeyValueTimestamp<>("[k1@0/2]", 3L, 1L),
//                    new KeyValueTimestamp<>("[k1@0/2]", 4L, 1L),
//                    new KeyValueTimestamp<>("[k1@4/6]", 1L, 5L)
//                )
//                );
//    verify(
//    drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//   asList(
//        new KeyValueTimestamp<>("[k1@0/2]", 4L, 1L),
//        new KeyValueTimestamp<>("[k1@2/4]", 1L, 2L)
//    )
//    );
//}
//}

//[Fact]
//public void shouldSupportFinalResultsForTimeWindowsWithLargeJump()
//{
//    var builder = new StreamsBuilder();
//    IKTable<Windowed<string>, long> valueCounts = builder
//        .Stream("input", Consumed.with(STRING_SERDE, STRING_SERDE))
//        .groupBy((string k, string v) => k, Grouped.with(STRING_SERDE, STRING_SERDE))
//        .windowedBy(TimeWindows.of(Duration.FromMilliseconds(2L)).grace(Duration.FromMilliseconds(2L)))
//        .count(Materialize.As < string, long, IWindowStore<Bytes, byte[]>("counts").withCachingDisabled().withKeySerde(STRING_SERDE));
//    valueCounts
//        .suppress(untilWindowCloses(unbounded()))
//        .toStream()
//        .map((Windowed<string> k, Long v) => new KeyValuePair<>(k.ToString(), v))
//        .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));
//    valueCounts
//        .toStream()
//        .map((Windowed<string> k, Long v) => new KeyValuePair<>(k.ToString(), v))
//        .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));
//    Topology topology = builder.Build();
//    System.Console.Out.println(topology.describe());
//    ConsumerRecordFactory<string, string> recordFactory =
//        new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//    var driver = new TopologyTestDriver(topology, config);
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 1L));
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 2L));
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 3L));
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 4L));
//    // this update should get dropped, since the previous event advanced the stream time and closed the window.
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 30L));
//    verify(
//    drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//   asList(
//        new KeyValueTimestamp<>("[k1@0/2]", 1L, 0L),
//        new KeyValueTimestamp<>("[k1@0/2]", 2L, 1L),
//        new KeyValueTimestamp<>("[k1@2/4]", 1L, 2L),
//        new KeyValueTimestamp<>("[k1@0/2]", 3L, 1L),
//        new KeyValueTimestamp<>("[k1@2/4]", 2L, 3L),
//        new KeyValueTimestamp<>("[k1@0/2]", 4L, 1L),
//        new KeyValueTimestamp<>("[k1@4/6]", 1L, 4L),
//        new KeyValueTimestamp<>("[k1@30/32]", 1L, 30L)
//    )
//    );
//    verify(
//    drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//               asList(
//                    new KeyValueTimestamp<>("[k1@0/2]", 4L, 1L),
//                    new KeyValueTimestamp<>("[k1@2/4]", 2L, 3L),
//                    new KeyValueTimestamp<>("[k1@4/6]", 1L, 4L)
//                )
//                );
//}

//[Fact]
//public void shouldSupportFinalResultsForSessionWindows()
//{
//    var builder = new StreamsBuilder();
//    IKTable<Windowed<string>, long> valueCounts = builder
//        .Stream("input", Consumed.with(STRING_SERDE, STRING_SERDE))
//        .groupBy((string k, string v) => k, Grouped.with(STRING_SERDE, STRING_SERDE))
//        .windowedBy(SessionWindows.with(Duration.FromMilliseconds(5L)).grace(Duration.FromMilliseconds(0L)))
//        .count(Materialize.As < string, long, ISessionStore<Bytes, byte[]>("counts").withCachingDisabled());
//    valueCounts
//        .suppress(untilWindowCloses(unbounded()))
//        .toStream()
//        .map((Windowed<string> k, Long v) => new KeyValuePair<>(k.ToString(), v))
//        .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));
//    valueCounts
//        .toStream()
//        .map((Windowed<string> k, Long v) => new KeyValuePair<>(k.ToString(), v))
//        .to("output-raw", Produced.with(STRING_SERDE, Serdes.Long()));
//    Topology topology = builder.Build();
//    System.Console.Out.println(topology.describe());
//    ConsumerRecordFactory<string, string> recordFactory =
//        new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//    var driver = new TopologyTestDriver(topology, config);
//    // first window
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 5L));
//    // arbitrarily disordered records are admitted, because the *window* is not closed until stream-time > window-end + grace
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 1L));
//    // any record in the same partition advances stream time (note the key is different)
//    driver.pipeInput(recordFactory.create("input", "k2", "v1", 6L));
//    // late event for first window - this should get dropped from all streams, since the first window is now closed.
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 5L));
//    // just pushing stream time forward to flush the other events through.
//    driver.pipeInput(recordFactory.create("input", "k1", "v1", 30L));
//    verify(
//    drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//               asList(
//                    new KeyValueTimestamp<>("[k1@0/0]", 1L, 0L),
//                    new KeyValueTimestamp<>("[k1@0/0]", null, 0L),
//                    new KeyValueTimestamp<>("[k1@0/5]", 2L, 5L),
//                    new KeyValueTimestamp<>("[k1@0/5]", null, 5L),
//                    new KeyValueTimestamp<>("[k1@0/5]", 3L, 5L),
//                    new KeyValueTimestamp<>("[k2@6/6]", 1L, 6L),
//                    new KeyValueTimestamp<>("[k1@30/30]", 1L, 30L)
//                )
//                );
//    verify(
//    drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//               asList(
//                    new KeyValueTimestamp<>("[k1@0/5]", 3L, 5L),
//                    new KeyValueTimestamp<>("[k2@6/6]", 1L, 6L)
//                )
//                );
//}

//[Fact]
//public void shouldWorkBeforeGroupBy()
//{
//    var builder = new StreamsBuilder();

//    builder
//        .Table("topic", Consumed.with(Serdes.String(), Serdes.String()))
//        .suppress(untilTimeLimit(Duration.FromMilliseconds(10), unbounded()))
//        .groupBy(KeyValuePair.Create, Grouped.with(Serdes.String(), Serdes.String()))
//        .count()
//        .toStream()
//        .to("output", Produced.with(Serdes.String(), Serdes.Long()));

//    var driver = new TopologyTestDriver(builder.Build(), config);
//    ConsumerRecordFactory<string, string> recordFactory =
//        new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//    driver.pipeInput(recordFactory.create("topic", "A", "a", 0L));
//    driver.pipeInput(recordFactory.create("topic", "tick", "tick", 10L));

//    verify(
//    drainProducerRecords(driver, "output", STRING_DESERIALIZER, LONG_DESERIALIZER),
//        singletonList(new KeyValueTimestamp<>("A", 1L, 0L))
//        );
//}

//[Fact]
//public void shouldWorkBeforeJoinRight()
//{
//    var builder = new StreamsBuilder();

//    IKTable<string, string> left = builder
//        .Table("left", Consumed.with(Serdes.String(), Serdes.String()));

//    IKTable<string, string> right = builder
//        .Table("right", Consumed.with(Serdes.String(), Serdes.String()))
//        .suppress(untilTimeLimit(Duration.FromMilliseconds(10), unbounded()));

//    left
//        .outerJoin(right, (l, r) => string.Format("(%s,%s)", l, r))
//        .toStream()
//        .to("output", Produced.with(Serdes.String(), Serdes.String()));

//    var driver = new TopologyTestDriver(builder.Build(), config);
//    ConsumerRecordFactory<string, string> recordFactory =
//        new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//    driver.pipeInput(recordFactory.create("right", "B", "1", 0L));
//    driver.pipeInput(recordFactory.create("right", "A", "1", 0L));
//    // buffered, no output
//    verify(
//    drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//            //  emptyList()
//            //);


//            driver.pipeInput(recordFactory.create("right", "tick", "tick", 10L));
//    // flush buffer
//    verify(
//    drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//               asList(
//                    new KeyValueTimestamp<>("A", "(null,1)", 0L),
//                    new KeyValueTimestamp<>("B", "(null,1)", 0L)
//                )
//                        );


//    driver.pipeInput(recordFactory.create("right", "A", "2", 11L));
//    // buffered, no output
//    verify(
//    drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//            //                emptyList()
//            //            );


//            driver.pipeInput(recordFactory.create("left", "A", "a", 12L));
//    // should join with previously emitted right side
//    verify(
//    drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//                singletonList(new KeyValueTimestamp<>("A", "(a,1)", 12L))
//                        );


//    driver.pipeInput(recordFactory.create("left", "B", "b", 12L));
//    // should view through to the parent IKTable, since B is no longer buffered
//    verify(
//    drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//                singletonList(new KeyValueTimestamp<>("B", "(b,1)", 12L))
//                        );


//    driver.pipeInput(recordFactory.create("left", "A", "b", 13L));
//    // should join with previously emitted right side
//    verify(
//    drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//                singletonList(new KeyValueTimestamp<>("A", "(b,1)", 13L))
//                        );


//    driver.pipeInput(recordFactory.create("right", "tick", "tick", 21L));
//    verify(
//    drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//               asList(
//                    new KeyValueTimestamp<>("tick", "(null,tick)", 21), // just a testing artifact
//                    new KeyValueTimestamp<>("A", "(b,2)", 13L)
//                )
//                        );
//}

//private void drainProducerRecords(var driver, string v, StringDeserializer sTRING_DESERIALIZER1, StringDeserializer sTRING_DESERIALIZER2)
//{
//    throw new NotImplementedException();
//}
//}


//[Fact]
//public void shouldWorkBeforeJoinLeft()
//{
//    var builder = new StreamsBuilder();

//    IKTable<string, string> left = builder
//        .Table("left", Consumed.with(Serdes.String(), Serdes.String()))
//        .suppress(untilTimeLimit(Duration.FromMilliseconds(10), unbounded()));

//    IKTable<string, string> right = builder
//        .Table("right", Consumed.with(Serdes.String(), Serdes.String()));

//    left
//        .outerJoin(right, (l, r) => string.Format("(%s,%s)", l, r))
//        .toStream()
//        .to("output", Produced.with(Serdes.String(), Serdes.String()));

//    Topology topology = builder.Build();
//    System.Console.Out.WriteLine(topology.describe());
//    try
//    {
//        var driver = new TopologyTestDriver(topology, config){
//            ConsumerRecordFactory<string, string> recordFactory =
//                new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//        driver.pipeInput(recordFactory.create("left", "B", "1", 0L));
//        driver.pipeInput(recordFactory.create("left", "A", "1", 0L));
//        // buffered, no output
//        verify(
//        drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//            //                emptyList()
//            //            );


//            driver.pipeInput(recordFactory.create("left", "tick", "tick", 10L));
//        // flush buffer
//        verify(
//        drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//               asList(
//                    new KeyValueTimestamp<>("A", "(1,null)", 0L),
//                    new KeyValueTimestamp<>("B", "(1,null)", 0L)
//                )
//                    );


//        driver.pipeInput(recordFactory.create("left", "A", "2", 11L));
//        // buffered, no output
//        verify(
//        drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//            //                emptyList()
//            //            );


//            driver.pipeInput(recordFactory.create("right", "A", "a", 12L));
//        // should join with previously emitted left side
//        verify(
//        drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//                singletonList(new KeyValueTimestamp<>("A", "(1,a)", 12L))
//                    );


//        driver.pipeInput(recordFactory.create("right", "B", "b", 12L));
//        // should view through to the parent IKTable, since B is no longer buffered
//        verify(
//        drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//                singletonList(new KeyValueTimestamp<>("B", "(1,b)", 12L))
//                    );


//        driver.pipeInput(recordFactory.create("right", "A", "b", 13L));
//        // should join with previously emitted left side
//        verify(
//        drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//                singletonList(new KeyValueTimestamp<>("A", "(1,b)", 13L))
//                    );


//        driver.pipeInput(recordFactory.create("left", "tick", "tick", 21L));
//        verify(
//        drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//               asList(
//                    new KeyValueTimestamp<>("tick", "(tick,null)", 21), // just a testing artifact
//                    new KeyValueTimestamp<>("A", "(2,b)", 13L)
//                )
//                    );
//    }

//    }


//private static void verify<K, V>(List<Message<K, V>> results,
//                                  List<KeyValueTimestamp<K, V>> expectedResults)
//{
//    if (results.size() != expectedResults.size())
//    {
//        throw new AssertionError(printRecords(results) + " != " + expectedResults);
//    }
//    Iterator<KeyValueTimestamp<K, V>> expectedIterator = expectedResults.iterator();
//    foreach (Message<K, V> result in results)
//    {
//        KeyValueTimestamp<K, V> expected = expectedIterator.next();
//        try
//        {
//            OutputVerifier.compareKeyValueTimestamp(result, expected.Key, expected.Value, expected.timestamp());
//        }
//        catch (AssertionError e)
//        {
//            throw new AssertionError(printRecords(results) + " != " + expectedResults, e);
//        }
//    }
//}

//private static List<Message<K, V>> drainProducerRecords<K, V>(var driver,
//                                                                      string topic,
//                                                                      IDeserializer<K> keyDeserializer,
//                                                                      IDeserializer<V> valueDeserializer)
//{
//    List<Message<K, V>> result = new LinkedList<>();
//    for (Message<K, V> next = driver.readOutput(topic, keyDeserializer, valueDeserializer);
//         next != null;
//         next = driver.readOutput(topic, keyDeserializer, valueDeserializer))
//    {
//        result.add(next);
//    }
//    return new List<>(result);
//}

//private static string printRecords<K, V>(List<Message<K, V>> result)
//{
//    var resultStr = new StringBuilder();
//    resultStr.Append("[\n");
//    foreach (Message<object, object> record in result)
//    {
//        resultStr.Append("  ").Append(record).Append("\n");
//    }

//    resultStr.Append("]");
//    return resultStr.ToString();
//}
//}
