using Kafka.Streams;
using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State.Windowed;
using Kafka.Streams.Temporary;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KStreamWindowAggregateTest
    {
        private readonly ConsumerRecordFactory<string, string> recordFactory =
            new ConsumerRecordFactory<string, string>(Serdes.String(), Serdes.String());

        private readonly StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());

        [Fact]
        public void TestAggBasic()
        {
            var builder = new StreamsBuilder();
            var topic1 = "topic1";

            IKTable<IWindowed<string>, string> table2 = builder
                .Stream(topic1, Consumed.With(Serdes.String(), Serdes.String()))
                .GroupByKey(Grouped.With(Serdes.String(), Serdes.String()))
                .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(10)).AdvanceBy(TimeSpan.FromMilliseconds(5)))
                .Aggregate(MockInitializer.STRING_INIT, MockAggregator<string, string>.TOSTRING_ADDER, Materialized.As<string, string, IWindowStore<Bytes, byte[]>>("topic1-Canonized")
                    .WithValueSerde(Serdes.String()));

            MockProcessorSupplier<IWindowed<string>, string> supplier = new MockProcessorSupplier<IWindowed<string>, string>();
            table2.ToStream().Process(supplier);

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
            driver.PipeInput(recordFactory.Create(topic1, "A", "1", 0L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "2", 1L));
            driver.PipeInput(recordFactory.Create(topic1, "C", "3", 2L));
            driver.PipeInput(recordFactory.Create(topic1, "D", "4", 3L));
            driver.PipeInput(recordFactory.Create(topic1, "A", "1", 4L));

            driver.PipeInput(recordFactory.Create(topic1, "A", "1", 5L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "2", 6L));
            driver.PipeInput(recordFactory.Create(topic1, "D", "4", 7L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "2", 8L));
            driver.PipeInput(recordFactory.Create(topic1, "C", "3", 9L));

            driver.PipeInput(recordFactory.Create(topic1, "A", "1", 10L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "2", 11L));
            driver.PipeInput(recordFactory.Create(topic1, "D", "4", 12L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "2", 13L));
            driver.PipeInput(recordFactory.Create(topic1, "C", "3", 14L));

            driver.PipeInput(recordFactory.Create(topic1, "B", "1", 3L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "2", 2L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "3", 9L));

            Assert.Equal(
                Arrays.asList(
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(0, 10)), "0+1", 0),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(0, 10)), "0+2", 1),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("C", new TimeWindow(0, 10)), "0+3", 2),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("D", new TimeWindow(0, 10)), "0+4", 3),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(0, 10)), "0+1+1", 4),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(0, 10)), "0+1+1+1", 5),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(5, 15)), "0+1", 5),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(0, 10)), "0+2+2", 6),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(5, 15)), "0+2", 6),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("D", new TimeWindow(0, 10)), "0+4+4", 7),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("D", new TimeWindow(5, 15)), "0+4", 7),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(0, 10)), "0+2+2+2", 8),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(5, 15)), "0+2+2", 8),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("C", new TimeWindow(0, 10)), "0+3+3", 9),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("C", new TimeWindow(5, 15)), "0+3", 9),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(5, 15)), "0+1+1", 10),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(10, 20)), "0+1", 10),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(5, 15)), "0+2+2+2", 11),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(10, 20)), "0+2", 11),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("D", new TimeWindow(5, 15)), "0+4+4", 12),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("D", new TimeWindow(10, 20)), "0+4", 12),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(5, 15)), "0+2+2+2+2", 13),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(10, 20)), "0+2+2", 13),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("C", new TimeWindow(5, 15)), "0+3+3", 14),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("C", new TimeWindow(10, 20)), "0+3", 14),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(0, 10)), "0+2+2+2+1", 8),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(0, 10)), "0+2+2+2+1+2", 8),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(0, 10)), "0+2+2+2+1+2+3", 9),
                     new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(5, 15)), "0+2+2+2+2+3", 13)),
                 supplier.TheCapturedProcessor().processed);
        }

        [Fact]
        public void testJoin()
        {
            var builder = new StreamsBuilder();
            var topic1 = "topic1";
            var topic2 = "topic2";

            IKTable<IWindowed<string>, string> table1 = builder
                .Stream(topic1, Consumed.With(Serdes.String(), Serdes.String()))
                .GroupByKey(Grouped.With(Serdes.String(), Serdes.String()))
                .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(10)).AdvanceBy(TimeSpan.FromMilliseconds(5)))
                .Aggregate(MockInitializer.STRING_INIT, MockAggregator<string, string>.TOSTRING_ADDER, Materialized.As<string, string, IWindowStore<Bytes, byte[]>>("topic1-Canonized")
                    .WithValueSerde(Serdes.String()));

            MockProcessorSupplier<IWindowed<string>, string> supplier = new MockProcessorSupplier<IWindowed<string>, string>();
            table1.ToStream().Process(supplier);

            IKTable<IWindowed<string>, string> table2 = builder
                .Stream(topic2, Consumed.With(Serdes.String(), Serdes.String()))
                .GroupByKey(Grouped.With(Serdes.String(), Serdes.String()))
                .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(10)).AdvanceBy(TimeSpan.FromMilliseconds(5)))
                .Aggregate(MockInitializer.STRING_INIT, MockAggregator<string, string>.TOSTRING_ADDER, Materialized.As<string, string, IWindowStore<Bytes, byte[]>>("topic2-Canonized")
                    .WithValueSerde(Serdes.String()));
            table2.ToStream().Process(supplier);

            table1.Join(table2, (p1, p2) => p1 + "%" + p2).ToStream().Process(supplier);

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
            driver.PipeInput(recordFactory.Create(topic1, "A", "1", 0L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "2", 1L));
            driver.PipeInput(recordFactory.Create(topic1, "C", "3", 2L));
            driver.PipeInput(recordFactory.Create(topic1, "D", "4", 3L));
            driver.PipeInput(recordFactory.Create(topic1, "A", "1", 9L));

            List<MockProcessor<IWindowed<string>, string>> processors = supplier.CapturedProcessors(3);

            processors.First().CheckAndClearProcessResult(
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(0, 10)), "0+1", 0),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(0, 10)), "0+2", 1),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("C", new TimeWindow(0, 10)), "0+3", 2),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("D", new TimeWindow(0, 10)), "0+4", 3),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(0, 10)), "0+1+1", 9),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(5, 15)), "0+1", 9));

            processors.ElementAt(1).CheckAndClearProcessResult(Array.Empty<KeyValueTimestamp<IWindowed<string>, string>>());
            processors.ElementAt(2).CheckAndClearProcessResult(Array.Empty<KeyValueTimestamp<IWindowed<string>, string>>());

            driver.PipeInput(recordFactory.Create(topic1, "A", "1", 5L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "2", 6L));
            driver.PipeInput(recordFactory.Create(topic1, "D", "4", 7L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "2", 8L));
            driver.PipeInput(recordFactory.Create(topic1, "C", "3", 9L));

            processors.First().CheckAndClearProcessResult(
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(0, 10)), "0+1+1+1", 9),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(5, 15)), "0+1+1", 9),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(0, 10)), "0+2+2", 6),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(5, 15)), "0+2", 6),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("D", new TimeWindow(0, 10)), "0+4+4", 7),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("D", new TimeWindow(5, 15)), "0+4", 7),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(0, 10)), "0+2+2+2", 8),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(5, 15)), "0+2+2", 8),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("C", new TimeWindow(0, 10)), "0+3+3", 9),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("C", new TimeWindow(5, 15)), "0+3", 9)
            );
            processors.ElementAt(1).CheckAndClearProcessResult(Array.Empty<KeyValueTimestamp<IWindowed<string>, string>>());
            processors.ElementAt(2).CheckAndClearProcessResult(Array.Empty<KeyValueTimestamp<IWindowed<string>, string>>());

            driver.PipeInput(recordFactory.Create(topic2, "A", "a", 0L));
            driver.PipeInput(recordFactory.Create(topic2, "B", "b", 1L));
            driver.PipeInput(recordFactory.Create(topic2, "C", "c", 2L));
            driver.PipeInput(recordFactory.Create(topic2, "D", "d", 20L));
            driver.PipeInput(recordFactory.Create(topic2, "A", "a", 20L));

            processors.ElementAt(0).CheckAndClearProcessResult(Array.Empty<KeyValueTimestamp<IWindowed<string>, string>>());
            processors.ElementAt(1).CheckAndClearProcessResult(
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(0, 10)), "0+a", 0),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(0, 10)), "0+b", 1),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("C", new TimeWindow(0, 10)), "0+c", 2),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("D", new TimeWindow(15, 25)), "0+d", 20),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("D", new TimeWindow(20, 30)), "0+d", 20),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(15, 25)), "0+a", 20),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(20, 30)), "0+a", 20)
            );
            processors.ElementAt(2).CheckAndClearProcessResult(
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(0, 10)), "0+1+1+1%0+a", 9),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(0, 10)), "0+2+2+2%0+b", 8),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("C", new TimeWindow(0, 10)), "0+3+3%0+c", 9));

            driver.PipeInput(recordFactory.Create(topic2, "A", "a", 5L));
            driver.PipeInput(recordFactory.Create(topic2, "B", "b", 6L));
            driver.PipeInput(recordFactory.Create(topic2, "D", "d", 7L));
            driver.PipeInput(recordFactory.Create(topic2, "D", "d", 18L));
            driver.PipeInput(recordFactory.Create(topic2, "A", "a", 21L));

            processors.ElementAt(0).CheckAndClearProcessResult(Array.Empty<KeyValueTimestamp<IWindowed<string>, string>>());
            processors.ElementAt(1).CheckAndClearProcessResult(
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(0, 10)), "0+a+a", 5),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(5, 15)), "0+a", 5),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(0, 10)), "0+b+b", 6),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(5, 15)), "0+b", 6),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("D", new TimeWindow(0, 10)), "0+d", 7),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("D", new TimeWindow(5, 15)), "0+d", 7),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("D", new TimeWindow(10, 20)), "0+d", 18),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("D", new TimeWindow(15, 25)), "0+d+d", 20),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(15, 25)), "0+a+a", 21),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(20, 30)), "0+a+a", 21)
            );
            processors.ElementAt(2).CheckAndClearProcessResult(
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(0, 10)), "0+1+1+1%0+a+a", 9),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("A", new TimeWindow(5, 15)), "0+1+1%0+a", 9),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(0, 10)), "0+2+2+2%0+b+b", 8),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("B", new TimeWindow(5, 15)), "0+2+2%0+b", 8),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("D", new TimeWindow(0, 10)), "0+4+4%0+d", 7),
                new KeyValueTimestamp<IWindowed<string>, string>(new Windowed<string>("D", new TimeWindow(5, 15)), "0+4%0+d", 7)
            );
        }

        [Fact]
        public void ShouldLogAndMeterWhenSkippingNullKey()
        {
            var builder = new StreamsBuilder();
            var topic = "topic";

            builder
                .Stream(topic, Consumed.With(Serdes.String(), Serdes.String()))
                .GroupByKey(Grouped.With(Serdes.String(), Serdes.String()))
                .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(10)).AdvanceBy(TimeSpan.FromMilliseconds(5)))
                .Aggregate(
                    MockInitializer.STRING_INIT,
                    MockAggregator<string, string>.ToStringInstance("+"),
                    Materialized.As<string, string, IWindowStore<Bytes, byte[]>>("topic1-Canonicalized")
                    .WithValueSerde(Serdes.String())
                );

            //LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
            driver.PipeInput(recordFactory.Create(topic, null, "1"));
            //LogCaptureAppender.unregister(appender);

            //Assert.Equal(1.0, getMetricByName(driver.metrics(), "skipped-records-total", "stream-metrics").metricValue());
            //Assert.Equal(appender.getMessages(), Arrays.asItems("Skipping record due to null key. value=[1] topic=[topic] partition=[0] offset=[0]"));
        }

        // testing deprecated functionality (behavior of until)
        [Fact]
        public void shouldLogAndMeterWhenSkippingExpiredWindow()
        {
            var builder = new StreamsBuilder();
            var topic = "topic";

            IKStream<string, string> stream1 = builder.Stream(topic, Consumed.With(Serdes.String(), Serdes.String()));
            //stream1.GroupByKey(Grouped.With(Serdes.String(), Serdes.String()))
            //       .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(10)).advanceBy(TimeSpan.FromMilliseconds(5)).until(100))
            //       .Aggregate(
            //           () => "",
            //           MockAggregator.toStringInstance("+"),
            //           Materialized.As < string, string, IWindowStore<Bytes, byte[]>>("topic1-Canonicalized").WithValueSerde(Serdes.String()).WithCachingDisabled().WithLoggingDisabled()
            //       )
            //       .ToStream()
            //       .Map((key, value) => KeyValuePair.Create(key.ToString(), value))
            //       .To("output");

            // LogCaptureAppender.setClassLoggerToDebug(KStreamWindowAggregate));
            // LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
            driver.PipeInput(recordFactory.Create(topic, "k", "100", 100L));
            driver.PipeInput(recordFactory.Create(topic, "k", "0", 0L));
            driver.PipeInput(recordFactory.Create(topic, "k", "1", 1L));
            driver.PipeInput(recordFactory.Create(topic, "k", "2", 2L));
            driver.PipeInput(recordFactory.Create(topic, "k", "3", 3L));
            driver.PipeInput(recordFactory.Create(topic, "k", "4", 4L));
            driver.PipeInput(recordFactory.Create(topic, "k", "5", 5L));
            driver.PipeInput(recordFactory.Create(topic, "k", "6", 6L));
            //LogCaptureAppender.unregister(appender);

            //.AssertLatenessMetrics(
            //     driver,
            //     (7.0), // how many events get dropped
            //     (100.0), // k:0 is 100ms late, since its time is 0, but it arrives at stream time 100.
            //     (84.875) // (0 + 100 + 99 + 98 + 97 + 96 + 95 + 94) / 8
            // );

            //Assert.Equal(appender.getMessages(), Arrays.asItems(
            //    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[1] timestamp=[0] window=[0,10) expiration=[10] streamTime=[100]",
            //    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[2] timestamp=[1] window=[0,10) expiration=[10] streamTime=[100]",
            //    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[3] timestamp=[2] window=[0,10) expiration=[10] streamTime=[100]",
            //    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[4] timestamp=[3] window=[0,10) expiration=[10] streamTime=[100]",
            //    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[5] timestamp=[4] window=[0,10) expiration=[10] streamTime=[100]",
            //    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[6] timestamp=[5] window=[0,10) expiration=[10] streamTime=[100]",
            //    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[7] timestamp=[6] window=[0,10) expiration=[10] streamTime=[100]"
            //));

            //        OutputVerifier.compareKeyValueTimestamp(getOutput(driver), "[k@95/105]", "+100", 100);
            //        OutputVerifier.compareKeyValueTimestamp(getOutput(driver), "[k@100/110]", "+100", 100);
            //        OutputVerifier.compareKeyValueTimestamp(getOutput(driver), "[k@5/15]", "+5", 5);
            //        OutputVerifier.compareKeyValueTimestamp(getOutput(driver), "[k@5/15]", "+5+6", 6);
            //        Assert.Equal(driver.readOutput("output"), nullValue());
        }

        [Fact]
        public void ShouldLogAndMeterWhenSkippingExpiredWindowByGrace()
        {
            var builder = new StreamsBuilder();
            var topic = "topic";

            IKStream<string, string> stream1 = builder.Stream(topic, Consumed.With(Serdes.String(), Serdes.String()));
            stream1.GroupByKey(Grouped.With(Serdes.String(), Serdes.String()))
                   .WindowedBy(TimeWindows.Of(TimeSpan.FromMilliseconds(10)).AdvanceBy(TimeSpan.FromMilliseconds(10)).Grace(TimeSpan.FromMilliseconds(90L)))
                   .Aggregate(
                       () => "",
                       MockAggregator<string, string>.ToStringInstance("+"),
                       Materialized.As<string, string, IWindowStore<Bytes, byte[]>>("topic1-Canonicalized")
                       .WithValueSerde(Serdes.String())
                       .WithCachingDisabled()
                       .WithLoggingDisabled()
                   )
                   .ToStream()
                   .Map((key, value) => KeyValuePair.Create(key.ToString(), value))
                   .To("output");

            // LogCaptureAppender.setClassLoggerToDebug(KStreamWindowAggregate));
            // LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
            driver.PipeInput(recordFactory.Create(topic, "k", "100", 200L));
            driver.PipeInput(recordFactory.Create(topic, "k", "0", 100L));
            driver.PipeInput(recordFactory.Create(topic, "k", "1", 101L));
            driver.PipeInput(recordFactory.Create(topic, "k", "2", 102L));
            driver.PipeInput(recordFactory.Create(topic, "k", "3", 103L));
            driver.PipeInput(recordFactory.Create(topic, "k", "4", 104L));
            driver.PipeInput(recordFactory.Create(topic, "k", "5", 105L));
            driver.PipeInput(recordFactory.Create(topic, "k", "6", 6L));
            //LogCaptureAppender.unregister(appender);

            //.AssertLatenessMetrics(driver, (7.0), (194.0), (97.375));

            //Assert.Equal(appender.getMessages(), Arrays.asItems(
            //    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[1] timestamp=[100] window=[100,110) expiration=[110] streamTime=[200]",
            //    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[2] timestamp=[101] window=[100,110) expiration=[110] streamTime=[200]",
            //    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[3] timestamp=[102] window=[100,110) expiration=[110] streamTime=[200]",
            //    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[4] timestamp=[103] window=[100,110) expiration=[110] streamTime=[200]",
            //    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[5] timestamp=[104] window=[100,110) expiration=[110] streamTime=[200]",
            //    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[6] timestamp=[105] window=[100,110) expiration=[110] streamTime=[200]",
            //    "Skipping record for expired window. key=[k] topic=[topic] partition=[0] offset=[7] timestamp=[6] window=[0,10) expiration=[110] streamTime=[200]"
            //));

            //OutputVerifier.compareKeyValueTimestamp(getOutput(driver), "[k@200/210]", "+100", 200);
            //Assert.Equal(driver.ReadOutput("output"), nullValue());
        }

        //    private void assertLatenessMetrics(TopologyTestDriver driver,
        //                                       Matcher<object> dropTotal,
        //                                       Matcher<object> maxLateness,
        //                                       Matcher<object> avgLateness)
        //    {
        //        var dropMetric = new MetricName(
        //            "late-record-drop-total",
        //            "stream-processor-node-metrics",
        //            "The total number of occurrence of late-record-drop operations.",
        //            mkMap(
        //                mkEntry("client-id", "topology-test-driver-virtual-thread"),
        //                mkEntry("Task-id", "0_0"),
        //                mkEntry("processor-node-id", "KSTREAM-AGGREGATE-0000000001")
        //            )
        //        );
        //        Assert.Equal(driver.metrics().Get(dropMetric).metricValue(), dropTotal);
        //
        //        var dropRate = new MetricName(
        //            "late-record-drop-rate",
        //            "stream-processor-node-metrics",
        //            "The average number of occurrence of late-record-drop operations.",
        //            mkMap(
        //                mkEntry("client-id", "topology-test-driver-virtual-thread"),
        //                mkEntry("Task-id", "0_0"),
        //                mkEntry("processor-node-id", "KSTREAM-AGGREGATE-0000000001")
        //            )
        //        );
        //        Assert.Equal(driver.metrics().Get(dropRate).metricValue(), not(0.0));
        //
        //        var latenessMaxMetric = new MetricName(
        //            "record-lateness-max",
        //            "stream-Task-metrics",
        //            "The max observed lateness of records.",
        //            mkMap(
        //                mkEntry("client-id", "topology-test-driver-virtual-thread"),
        //                mkEntry("Task-id", "0_0")
        //            )
        //        );
        //        Assert.Equal(driver.metrics().Get(latenessMaxMetric).metricValue(), maxLateness);
        //
        //        var latenessAvgMetric = new MetricName(
        //            "record-lateness-avg",
        //            "stream-Task-metrics",
        //            "The average observed lateness of records.",
        //            mkMap(
        //                mkEntry("client-id", "topology-test-driver-virtual-thread"),
        //                mkEntry("Task-id", "0_0")
        //            )
        //        );
        //        Assert.Equal(driver.metrics().Get(latenessAvgMetric).metricValue(), avgLateness);
        //    }

        //    private Message<string, string> getOutput(TopologyTestDriver driver)
        //    {
        //        return driver.readOutput("output", Serdes.String().Deserializer, Serdes.String().Deserializer);
        //    }
    }
}