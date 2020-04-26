namespace Kafka.Streams.Tests.Kstream.Internals
{
}
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
//        private static Serdes.String().Deserializer STRING_DESERIALIZER = new Serdes.String().Deserializer();
//        private static Serdes.String().Serializer STRING_SERIALIZER = Serdes.String();
//        private static ISerde<string> STRING_SERDE = Serdes.String();
//        private static LongDeserializer LONG_DESERIALIZER = new LongDeserializer();
//        private StreamsConfig config = Utils.mkProperties(Utils.mkMap(
//            Utils.mkEntry(StreamsConfig.ApplicationId, typeof(SuppressScenarioTest).FullName.ToLower()),
//            Utils.mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory()),
//            Utils.mkEntry(StreamsConfig.BootstrapServers, "bogus")
//        ));

//        [Fact]
//        public void shouldImmediatelyEmitEventsWithZeroEmitAfter()
//        {
//            var builder = new StreamsBuilder();

//            IKTable<string, long> valueCounts = builder
//                .Table(
//                    "input",
//                    Consumed.With(STRING_SERDE, STRING_SERDE),
//                    Materialized.with<string, string, IKeyValueStore<Bytes, byte[]>>(STRING_SERDE, STRING_SERDE)
//                        .WithCachingDisabled()
//                        .WithLoggingDisabled()
//                )
//                .GroupBy((k, v) => KeyValuePair.Create(v, k), Grouped.With(STRING_SERDE, STRING_SERDE))
//                .Count();

//            valueCounts
//                .suppress(untilTimeLimit(TimeSpan.Zero, unbounded()))
//                .ToStream()
//                .To("output-suppressed", Produced.With(STRING_SERDE, Serdes.Long()));

//            valueCounts
//                .ToStream()
//                .To("output-raw", Produced.With(STRING_SERDE, Serdes.Long()));

//            Topology topology = builder.Build();


//            ConsumerRecordFactory<string, string> recordFactory =
//                new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//            var driver = new TopologyTestDriver(topology, config);
//            driver.PipeInput(recordFactory.Create("input", "k1", "v1", 0L));
//            driver.PipeInput(recordFactory.Create("input", "k1", "v2", 1L));
//            driver.PipeInput(recordFactory.Create("input", "k2", "v1", 2L));
//            verify(
//            drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//           Arrays.asList(
//                new KeyValueTimestamp<string, string>("v1", 1L, 0L),
//                new KeyValueTimestamp<string, string>("v1", 0L, 1L),
//                new KeyValueTimestamp<string, string>("v2", 1L, 1L),
//                new KeyValueTimestamp<string, string>("v1", 1L, 2L)
//            )
//            );
//            verify(
//            drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//           Arrays.asList(
//                new KeyValueTimestamp<string, string>("v1", 1L, 0L),
//                new KeyValueTimestamp<string, string>("v1", 0L, 1L),
//                new KeyValueTimestamp<string, string>("v2", 1L, 1L),
//                new KeyValueTimestamp<string, string>("v1", 1L, 2L)
//            )
//            );
//            driver.PipeInput(recordFactory.Create("input", "x", "x", 3L));
//            verify(
//            drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//            Collections.singletonList(
//                new KeyValueTimestamp<string, string>("x", 1L, 3L)
//            )
//            );
//            verify(
//            drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//            Collections.singletonList(
//                new KeyValueTimestamp<string, string>("x", 1L, 3L)
//            )
//            );
//            driver.PipeInput(recordFactory.Create("input", "x", "x", 4L));
//            verify(
//            drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//           Arrays.asList(
//                new KeyValueTimestamp<string, string>("x", 0L, 4L),
//                new KeyValueTimestamp<string, string>("x", 1L, 4L)
//            )
//            );
//            verify(
//            drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//           Arrays.asList(
//                new KeyValueTimestamp<string, string>("x", 0L, 4L),
//                new KeyValueTimestamp<string, string>("x", 1L, 4L)
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
//                Consumed.With(STRING_SERDE, STRING_SERDE),
//                Materialized.with<string, string, IKeyValueStore<Bytes, byte[]>>(STRING_SERDE, STRING_SERDE)
//                    .WithCachingDisabled()
//                    .WithLoggingDisabled()
//            )
//            .GroupBy((k, v) => KeyValuePair.Create(v, k), Grouped.With(STRING_SERDE, STRING_SERDE))
//            .Count();
//        valueCounts
//            .suppress(untilTimeLimit(TimeSpan.FromMilliseconds(2L), unbounded()))
//            .ToStream()
//            .To("output-suppressed", Produced.With(STRING_SERDE, Serdes.Long()));
//        valueCounts
//            .ToStream()
//            .To("output-raw", Produced.With(STRING_SERDE, Serdes.Long()));
//        Topology topology = builder.Build();
//        ConsumerRecordFactory<string, string> recordFactory =
//            new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//        var driver = new TopologyTestDriver(topology, config);
//        driver.PipeInput(recordFactory.Create("input", "k1", "v1", 0L));
//        driver.PipeInput(recordFactory.Create("input", "k1", "v2", 1L));
//        driver.PipeInput(recordFactory.Create("input", "k2", "v1", 2L));

//        verify(
//        drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//       Arrays.asList(
//            new KeyValueTimestamp<string, string>("v1", 1L, 0L),
//            new KeyValueTimestamp<string, string>("v1", 0L, 1L),
//            new KeyValueTimestamp<string, string>("v2", 1L, 1L),
//            new KeyValueTimestamp<string, string>("v1", 1L, 2L)
//        )
//        );

//        verify(
//        drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//        Collections.singletonList(new KeyValueTimestamp<string, string>("v1", 1L, 2L))
//        );
//        // inserting a dummy "tick" record just to advance stream time
//        driver.PipeInput(recordFactory.Create("input", "tick", "tick", 3L));

//        verify(
//        drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//        Collections.singletonList(new KeyValueTimestamp<string, string>("tick", 1L, 3L))
//        );

//        // the stream time is now 3, so it's time to emit this record
//        verify(
//        drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//        Collections.singletonList(new KeyValueTimestamp<string, string>("v2", 1L, 1L))
//        );


//        driver.PipeInput(recordFactory.Create("input", "tick", "tick", 4L));
//        verify(
//        drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//       Arrays.asList(
//            new KeyValueTimestamp<string, string>("tick", 0L, 4L),
//            new KeyValueTimestamp<string, string>("tick", 1L, 4L)
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
//            Consumed.With(STRING_SERDE, STRING_SERDE),
//            Materialized..with<string, string, IKeyValueStore<Bytes, byte[]>>(STRING_SERDE, STRING_SERDE)
//                .WithCachingDisabled()
//                .WithLoggingDisabled()
//        )
//        .GroupBy((k, v) => KeyValuePair.Create(v, k), Grouped.With(STRING_SERDE, STRING_SERDE))
//        .Count(Materialized.With(STRING_SERDE, Serdes.Long()));
//    valueCounts
//        .suppress(untilTimeLimit(TimeSpan.FromMilliseconds(long.MaxValue), maxRecords(1L).emitEarlyWhenFull()))
//        .ToStream()
//        .To("output-suppressed", Produced.With(STRING_SERDE, Serdes.Long()));
//    valueCounts
//        .ToStream()
//        .To("output-raw", Produced.With(STRING_SERDE, Serdes.Long()));
//    Topology topology = builder.Build();
//    System.Console.Out.WriteLine(topology.Describe());
//    ConsumerRecordFactory<string, string> recordFactory =
//        new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//    var driver = new TopologyTestDriver(topology, config);
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 0L));
//    driver.PipeInput(recordFactory.Create("input", "k1", "v2", 1L));
//    driver.PipeInput(recordFactory.Create("input", "k2", "v1", 2L));
//    verify(
//      drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//     Arrays.asList(
//          new KeyValueTimestamp<string, string>("v1", 1L, 0L),
//          new KeyValueTimestamp<string, string>("v1", 0L, 1L),
//          new KeyValueTimestamp<string, string>("v2", 1L, 1L),
//          new KeyValueTimestamp<string, string>("v1", 1L, 2L)
//      )
//      );

//    verify(
//    drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//   Arrays.asList(
//        // consecutive updates to v1 get suppressed into only the latter.
//        new KeyValueTimestamp<string, string>("v1", 0L, 1L),
//        new KeyValueTimestamp<string, string>("v2", 1L, 1L)
//    // the .Ast update won't be evicted until another key comes along.
//    )
//    );
//    driver.PipeInput(recordFactory.Create("input", "x", "x", 3L));

//    verify(
//    drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//    Collections.singletonList(
//        new KeyValueTimestamp<string, string>("x", 1L, 3L)
//    )
//    );
//    verify(
//    drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//    Collections.singletonList(
//        // now we see that .Ast update to v1, but we won't see the update to x until it gets evicted
//        new KeyValueTimestamp<string, string>("v1", 1L, 2L)
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
//            Consumed.With(STRING_SERDE, STRING_SERDE),
//            Materialized.with<string, string, IKeyValueStore<Bytes, byte[]>>(STRING_SERDE, STRING_SERDE)
//                .WithCachingDisabled()
//                .WithLoggingDisabled()
//        )
//        .GroupBy((k, v) => KeyValuePair.Create(v, k), Grouped.With(STRING_SERDE, STRING_SERDE))
//        .Count();
//    valueCounts
//        // this is a bit brittle, but I happen to know that the entries are a little over 100 bytes in size.
//        .suppress(untilTimeLimit(TimeSpan.FromMilliseconds(long.MaxValue), maxBytes(200L).emitEarlyWhenFull()))
//        .ToStream()
//        .To("output-suppressed", Produced.With(STRING_SERDE, Serdes.Long()));
//    valueCounts
//        .ToStream()
//        .To("output-raw", Produced.With(STRING_SERDE, Serdes.Long()));
//    Topology topology = builder.Build();
//    System.Console.Out.WriteLine(topology.Describe());
//    ConsumerRecordFactory<string, string> recordFactory =
//        new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//    var driver = new TopologyTestDriver(topology, config);
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 0L));
//    driver.PipeInput(recordFactory.Create("input", "k1", "v2", 1L));
//    driver.PipeInput(recordFactory.Create("input", "k2", "v1", 2L));

//    verify(
//    drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//   Arrays.asList(
//        new KeyValueTimestamp<string, string>("v1", 1L, 0L),
//        new KeyValueTimestamp<string, string>("v1", 0L, 1L),
//        new KeyValueTimestamp<string, string>("v2", 1L, 1L),
//        new KeyValueTimestamp<string, string>("v1", 1L, 2L)
//    )
//    );
//    verify(
//    drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//   Arrays.asList(
//        // consecutive updates to v1 get suppressed into only the latter.
//        new KeyValueTimestamp<string, string>("v1", 0L, 1L),
//        new KeyValueTimestamp<string, string>("v2", 1L, 1L)
//    // the .Ast update won't be evicted until another key comes along.
//    )
//    );
//    driver.PipeInput(recordFactory.Create("input", "x", "x", 3L));
//    verify(
//    drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//    Collections.singletonList(
//        new KeyValueTimestamp<string, string>("x", 1L, 3L)
//    )
//    );

//    verify(
//    drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//    Collections.singletonList(
//        // now we see that .Ast update to v1, but we won't see the update to x until it gets evicted
//        new KeyValueTimestamp<string, string>("v1", 1L, 2L)
//    )
//    );
//}
//}

//[Fact]
//public void shouldSupportFinalResultsForTimeWindows()
//{
//    var builder = new StreamsBuilder();
//    IKTable<IWindowed<string>, long> valueCounts = builder
//        .Stream("input", Consumed.With(STRING_SERDE, STRING_SERDE))
//        .GroupBy((string k, string v) => k, Grouped.With(STRING_SERDE, STRING_SERDE))
//        .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(2L)).Grace(TimeSpan.FromMilliseconds(1L)))
//        .Count(Materialized.As < string, long, IWindowStore<Bytes, byte[]>>("counts").WithCachingDisabled());
//    valueCounts
//        .suppress(untilWindowCloses(unbounded()))
//        .ToStream()
//        .Map((IWindowed<string> k, long v) => KeyValuePair.Create(k.ToString(), v))
//        .To("output-suppressed", Produced.With(STRING_SERDE, Serdes.Long()));
//    valueCounts
//        .ToStream()
//        .Map((IWindowed<string> k, long v) => KeyValuePair.Create(k.ToString(), v))
//        .To("output-raw", Produced.With(STRING_SERDE, Serdes.Long()));
//    Topology topology = builder.Build();
//    System.Console.Out.WriteLine(topology.Describe());
//    ConsumerRecordFactory<string, string> recordFactory =
//        new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//    var driver = new TopologyTestDriver(topology, config);
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 0L));
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 1L));
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 2L));
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 1L));
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 0L));
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 5L));
//    // note this .Ast record gets dropped because it is out of the grace period
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 0L));
//    verify(
//    drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//               Arrays.asList(
//                    new KeyValueTimestamp<string, string>("[k1@0/2]", 1L, 0L),
//                    new KeyValueTimestamp<string, string>("[k1@0/2]", 2L, 1L),
//                    new KeyValueTimestamp<string, string>("[k1@2/4]", 1L, 2L),
//                    new KeyValueTimestamp<string, string>("[k1@0/2]", 3L, 1L),
//                    new KeyValueTimestamp<string, string>("[k1@0/2]", 4L, 1L),
//                    new KeyValueTimestamp<string, string>("[k1@4/6]", 1L, 5L)
//                )
//                );
//    verify(
//    drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//   Arrays.asList(
//        new KeyValueTimestamp<string, string>("[k1@0/2]", 4L, 1L),
//        new KeyValueTimestamp<string, string>("[k1@2/4]", 1L, 2L)
//    )
//    );
//}
//}

//[Fact]
//public void shouldSupportFinalResultsForTimeWindowsWithLargeJump()
//{
//    var builder = new StreamsBuilder();
//    IKTable<IWindowed<string>, long> valueCounts = builder
//        .Stream("input", Consumed.With(STRING_SERDE, STRING_SERDE))
//        .GroupBy((string k, string v) => k, Grouped.With(STRING_SERDE, STRING_SERDE))
//        .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(2L)).Grace(TimeSpan.FromMilliseconds(2L)))
//        .Count(Materialized.As < string, long, IWindowStore<Bytes, byte[]>>("counts").WithCachingDisabled().WithKeySerde(STRING_SERDE));
//    valueCounts
//        .suppress(untilWindowCloses(unbounded()))
//        .ToStream()
//        .Map((IWindowed<string> k, long v) => KeyValuePair.Create(k.ToString(), v))
//        .To("output-suppressed", Produced.With(STRING_SERDE, Serdes.Long()));
//    valueCounts
//        .ToStream()
//        .Map((IWindowed<string> k, long v) => KeyValuePair.Create(k.ToString(), v))
//        .To("output-raw", Produced.With(STRING_SERDE, Serdes.Long()));
//    Topology topology = builder.Build();
//    System.Console.Out.WriteLine(topology.Describe());
//    ConsumerRecordFactory<string, string> recordFactory =
//        new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//    var driver = new TopologyTestDriver(topology, config);
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 0L));
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 1L));
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 2L));
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 0L));
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 3L));
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 0L));
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 4L));
//    // this update should get dropped, since the previous event advanced the stream time and closed the window.
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 0L));
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 30L));
//    verify(
//    drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//   Arrays.asList(
//        new KeyValueTimestamp<string, string>("[k1@0/2]", 1L, 0L),
//        new KeyValueTimestamp<string, string>("[k1@0/2]", 2L, 1L),
//        new KeyValueTimestamp<string, string>("[k1@2/4]", 1L, 2L),
//        new KeyValueTimestamp<string, string>("[k1@0/2]", 3L, 1L),
//        new KeyValueTimestamp<string, string>("[k1@2/4]", 2L, 3L),
//        new KeyValueTimestamp<string, string>("[k1@0/2]", 4L, 1L),
//        new KeyValueTimestamp<string, string>("[k1@4/6]", 1L, 4L),
//        new KeyValueTimestamp<string, string>("[k1@30/32]", 1L, 30L)
//    )
//    );
//    verify(
//    drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//               Arrays.asList(
//                    new KeyValueTimestamp<string, string>("[k1@0/2]", 4L, 1L),
//                    new KeyValueTimestamp<string, string>("[k1@2/4]", 2L, 3L),
//                    new KeyValueTimestamp<string, string>("[k1@4/6]", 1L, 4L)
//                )
//                );
//}

//[Fact]
//public void shouldSupportFinalResultsForSessionWindows()
//{
//    var builder = new StreamsBuilder();
//    IKTable<IWindowed<string>, long> valueCounts = builder
//        .Stream("input", Consumed.With(STRING_SERDE, STRING_SERDE))
//        .GroupBy((string k, string v) => k, Grouped.With(STRING_SERDE, STRING_SERDE))
//        .WindowedBy(SessionWindows.With(TimeSpan.FromMilliseconds(5L)).Grace(TimeSpan.FromMilliseconds(0L)))
//        .Count(Materialized.As < string, long, ISessionStore<Bytes, byte[]>>("counts").WithCachingDisabled());
//    valueCounts
//        .suppress(untilWindowCloses(unbounded()))
//        .ToStream()
//        .Map((IWindowed<string> k, long v) => KeyValuePair.Create(k.ToString(), v))
//        .To("output-suppressed", Produced.With(STRING_SERDE, Serdes.Long()));
//    valueCounts
//        .ToStream()
//        .Map((IWindowed<string> k, long v) => KeyValuePair.Create(k.ToString(), v))
//        .To("output-raw", Produced.With(STRING_SERDE, Serdes.Long()));
//    Topology topology = builder.Build();
//    System.Console.Out.WriteLine(topology.Describe());
//    ConsumerRecordFactory<string, string> recordFactory =
//        new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//    var driver = new TopologyTestDriver(topology, config);
//    // first window
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 0L));
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 5L));
//    // arbitrarily disordered records are admitted, because the *window* is not closed until stream-time > window-end + grace
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 1L));
//    // any record in the same partition advances stream time (note the key is different)
//    driver.PipeInput(recordFactory.Create("input", "k2", "v1", 6L));
//    // late event for first window - this should get dropped from All streams, since the first window is now closed.
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 5L));
//    // just pushing stream time forward to Flush the other events through.
//    driver.PipeInput(recordFactory.Create("input", "k1", "v1", 30L));
//    verify(
//    drainProducerRecords(driver, "output-raw", STRING_DESERIALIZER, LONG_DESERIALIZER),
//               Arrays.asList(
//                    new KeyValueTimestamp<string, string>("[k1@0/0]", 1L, 0L),
//                    new KeyValueTimestamp<string, string>("[k1@0/0]", null, 0L),
//                    new KeyValueTimestamp<string, string>("[k1@0/5]", 2L, 5L),
//                    new KeyValueTimestamp<string, string>("[k1@0/5]", null, 5L),
//                    new KeyValueTimestamp<string, string>("[k1@0/5]", 3L, 5L),
//                    new KeyValueTimestamp<string, string>("[k2@6/6]", 1L, 6L),
//                    new KeyValueTimestamp<string, string>("[k1@30/30]", 1L, 30L)
//                )
//                );
//    verify(
//    drainProducerRecords(driver, "output-suppressed", STRING_DESERIALIZER, LONG_DESERIALIZER),
//               Arrays.asList(
//                    new KeyValueTimestamp<string, string>("[k1@0/5]", 3L, 5L),
//                    new KeyValueTimestamp<string, string>("[k2@6/6]", 1L, 6L)
//                )
//                );
//}

//[Fact]
//public void shouldWorkBeforeGroupBy()
//{
//    var builder = new StreamsBuilder();

//    builder
//        .Table("topic", Consumed.With(Serdes.String(), Serdes.String()))
//        .suppress(untilTimeLimit(TimeSpan.FromMilliseconds(10), unbounded()))
//        .GroupBy(KeyValuePair.Create, Grouped.With(Serdes.String(), Serdes.String()))
//        .Count()
//        .ToStream()
//        .To("output", Produced.With(Serdes.String(), Serdes.Long()));

//    var driver = new TopologyTestDriver(builder.Build(), config);
//    ConsumerRecordFactory<string, string> recordFactory =
//        new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//    driver.PipeInput(recordFactory.Create("topic", "A", "a", 0L));
//    driver.PipeInput(recordFactory.Create("topic", "tick", "tick", 10L));

//    verify(
//    drainProducerRecords(driver, "output", STRING_DESERIALIZER, LONG_DESERIALIZER),
//        Collections.singletonList(new KeyValueTimestamp<string, string>("A", 1L, 0L))
//        );
//}

//[Fact]
//public void shouldWorkBeforeJoinRight()
//{
//    var builder = new StreamsBuilder();

//    IKTable<string, string> left = builder
//        .Table("left", Consumed.With(Serdes.String(), Serdes.String()));

//    IKTable<string, string> right = builder
//        .Table("right", Consumed.With(Serdes.String(), Serdes.String()))
//        .suppress(untilTimeLimit(TimeSpan.FromMilliseconds(10), unbounded()));

//    left
//        .OuterJoin(right, (l, r) => string.Format("(%s,%s)", l, r))
//        .ToStream()
//        .To("output", Produced.With(Serdes.String(), Serdes.String()));

//    var driver = new TopologyTestDriver(builder.Build(), config);
//    ConsumerRecordFactory<string, string> recordFactory =
//        new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//    driver.PipeInput(recordFactory.Create("right", "B", "1", 0L));
//    driver.PipeInput(recordFactory.Create("right", "A", "1", 0L));
//    // buffered, no output
//    verify(
//    drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//            //  emptyList()
//            //);


//            driver.PipeInput(recordFactory.Create("right", "tick", "tick", 10L));
//    // Flush buffer
//    verify(
//    drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//               Arrays.asList(
//                    new KeyValueTimestamp<string, string>("A", "(null,1)", 0L),
//                    new KeyValueTimestamp<string, string>("B", "(null,1)", 0L)
//                )
//                        );


//    driver.PipeInput(recordFactory.Create("right", "A", "2", 11L));
//    // buffered, no output
//    verify(
//    drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//            //                emptyList()
//            //            );


//            driver.PipeInput(recordFactory.Create("left", "A", "a", 12L));
//    // should join with previously emitted right side
//    verify(
//    drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//                Collections.singletonList(new KeyValueTimestamp<string, string>("A", "(a,1)", 12L))
//                        );


//    driver.PipeInput(recordFactory.Create("left", "B", "b", 12L));
//    // should view through to the parent IKTable, since B is no longer buffered
//    verify(
//    drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//                Collections.singletonList(new KeyValueTimestamp<string, string>("B", "(b,1)", 12L))
//                        );


//    driver.PipeInput(recordFactory.Create("left", "A", "b", 13L));
//    // should join with previously emitted right side
//    verify(
//    drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//                Collections.singletonList(new KeyValueTimestamp<string, string>("A", "(b,1)", 13L))
//                        );


//    driver.PipeInput(recordFactory.Create("right", "tick", "tick", 21L));
//    verify(
//    drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//               Arrays.asList(
//                    new KeyValueTimestamp<string, string>("tick", "(null,tick)", 21), // just a testing artifact
//                    new KeyValueTimestamp<string, string>("A", "(b,2)", 13L)
//                )
//                        );
//}

//private void drainProducerRecords(var driver, string v, Serdes.String().Deserializer sTRING_DESERIALIZER1, Serdes.String().Deserializer sTRING_DESERIALIZER2)
//{
//    throw new NotImplementedException();
//}
//}


//[Fact]
//public void shouldWorkBeforeJoinLeft()
//{
//    var builder = new StreamsBuilder();

//    IKTable<string, string> left = builder
//        .Table("left", Consumed.With(Serdes.String(), Serdes.String()))
//        .suppress(untilTimeLimit(TimeSpan.FromMilliseconds(10), unbounded()));

//    IKTable<string, string> right = builder
//        .Table("right", Consumed.With(Serdes.String(), Serdes.String()));

//    left
//        .OuterJoin(right, (l, r) => string.Format("(%s,%s)", l, r))
//        .ToStream()
//        .To("output", Produced.With(Serdes.String(), Serdes.String()));

//    Topology topology = builder.Build();
//    System.Console.Out.WriteLine(topology.Describe());
//    try
//    {
//        var driver = new TopologyTestDriver(topology, config){
//            ConsumerRecordFactory<string, string> recordFactory =
//                new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

//        driver.PipeInput(recordFactory.Create("left", "B", "1", 0L));
//        driver.PipeInput(recordFactory.Create("left", "A", "1", 0L));
//        // buffered, no output
//        verify(
//        drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//            //                emptyList()
//            //            );


//            driver.PipeInput(recordFactory.Create("left", "tick", "tick", 10L));
//        // Flush buffer
//        verify(
//        drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//               Arrays.asList(
//                    new KeyValueTimestamp<string, string>("A", "(1,null)", 0L),
//                    new KeyValueTimestamp<string, string>("B", "(1,null)", 0L)
//                )
//                    );


//        driver.PipeInput(recordFactory.Create("left", "A", "2", 11L));
//        // buffered, no output
//        verify(
//        drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//            //                emptyList()
//            //            );


//            driver.PipeInput(recordFactory.Create("right", "A", "a", 12L));
//        // should join with previously emitted left side
//        verify(
//        drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//                Collections.singletonList(new KeyValueTimestamp<string, string>("A", "(1,a)", 12L))
//                    );


//        driver.PipeInput(recordFactory.Create("right", "B", "b", 12L));
//        // should view through to the parent IKTable, since B is no longer buffered
//        verify(
//        drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//                Collections.singletonList(new KeyValueTimestamp<string, string>("B", "(1,b)", 12L))
//                    );


//        driver.PipeInput(recordFactory.Create("right", "A", "b", 13L));
//        // should join with previously emitted left side
//        verify(
//        drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//                Collections.singletonList(new KeyValueTimestamp<string, string>("A", "(1,b)", 13L))
//                    );


//        driver.PipeInput(recordFactory.Create("left", "tick", "tick", 21L));
//        verify(
//        drainProducerRecords(driver, "output", STRING_DESERIALIZER, STRING_DESERIALIZER),
//               Arrays.asList(
//                    new KeyValueTimestamp<string, string>("tick", "(tick,null)", 21), // just a testing artifact
//                    new KeyValueTimestamp<string, string>("A", "(2,b)", 13L)
//                )
//                    );
//    }

//    }


//private static void verify<K, V>(List<Message<K, V>> results,
//                                  List<KeyValueTimestamp<K, V>> expectedResults)
//{
//    if (results.Count != expectedResults.Count)
//    {
//        throw new AssertionError(printRecords(results) + " != " + expectedResults);
//    }
//    Iterator<KeyValueTimestamp<K, V>> expectedIterator = expectedResults.iterator();
//    foreach (Message<K, V> result in results)
//    {
//        KeyValueTimestamp<K, V> expected = expectedIterator.MoveNext();
//        try
//        {
//            OutputVerifier.compareKeyValueTimestamp(result, expected.Key, expected.Value, expected.Timestamp);
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
//        result.Add(next);
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
