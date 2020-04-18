using Kafka.Streams;
using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Mappers;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.Tests.Helpers;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KTableAggregateTest
    {
        private ISerde<string> stringSerde = Serdes.String();
        private Consumed<string, string> consumed = null; // Consumed.With(stringSerde, stringSerde);
        private Grouped<string, string> stringSerialized = null;// Grouped.With(stringSerde, stringSerde);
        private MockProcessorSupplier<string, object> supplier = new MockProcessorSupplier<>();

        [Fact]
        public void testAggBasic()
        {
            var builder = new StreamsBuilder();
            var topic1 = "topic1";

            IKTable<string, string> table1 = builder.Table(topic1, consumed);
            IKTable<string, string> table2 = table1
                .GroupBy(
                    MockMapper.noOpKeyValueMapper(),
                    stringSerialized)
                .Aggregate(
                    MockInitializer.STRING_INIT,
                    MockAggregator.TOSTRING_ADDER,
                    MockAggregator.TOSTRING_REMOVER,
                    Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("topic1-Canonized")
                        .WithValueSerde(stringSerde));

            table2.ToStream().Process(supplier);

            var driver = new TopologyTestDriver(
                builder.Build(),
                mkProperties(mkMap(
                    mkEntry(StreamsConfig.BootstrapServers, "dummy"),
                    mkEntry(StreamsConfig.ApplicationId, "test"),
                    mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory("kafka-test"))
                )),
                0L);

            ConsumerRecordFactory<string, string> recordFactory =
                new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L, 0L);

            driver.PipeInput(recordFactory.Create(topic1, "A", "1", 10L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "2", 15L));
            driver.PipeInput(recordFactory.Create(topic1, "A", "3", 20L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "4", 18L));
            driver.PipeInput(recordFactory.Create(topic1, "C", "5", 5L));
            driver.PipeInput(recordFactory.Create(topic1, "D", "6", 25L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "7", 15L));
            driver.PipeInput(recordFactory.Create(topic1, "C", "8", 10L));

            Assert.Equal(
       Arrays.asList(
                     new KeyValueTimestamp<>("A", "0+1", 10L),
                     new KeyValueTimestamp<>("B", "0+2", 15L),
                     new KeyValueTimestamp<>("A", "0+1-1", 20L),
                     new KeyValueTimestamp<>("A", "0+1-1+3", 20L),
                     new KeyValueTimestamp<>("B", "0+2-2", 18L),
                     new KeyValueTimestamp<>("B", "0+2-2+4", 18L),
                     new KeyValueTimestamp<>("C", "0+5", 5L),
                     new KeyValueTimestamp<>("D", "0+6", 25L),
                     new KeyValueTimestamp<>("B", "0+2-2+4-4", 18L),
                     new KeyValueTimestamp<>("B", "0+2-2+4-4+7", 18L),
                     new KeyValueTimestamp<>("C", "0+5-5", 10L),
                     new KeyValueTimestamp<>("C", "0+5-5+8", 10L)),
                 supplier.TheCapturedProcessor().processed);
        }

        [Fact]
        public void testAggRepartition()
        {
            var builder = new StreamsBuilder();
            var topic1 = "topic1";

            IKTable<string, string> table1 = builder.Table(topic1, consumed);
            IKTable<string, string> table2 = table1
                .GroupBy(
                    (key, value) =>
                    {
                        switch (key)
                        {
                            case "null":
                                return KeyValuePair.Create(null, value);
                            case "NULL":
                                return null;
                            default:
                                return KeyValuePair.Create(value, value);
                        }
                    },
                stringSerialized)
            .Aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                MockAggregator.TOSTRING_REMOVER,
                Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("topic1-Canonized")
                    .WithValueSerde(stringSerde));

            table2.ToStream().Process(supplier);

            var driver = new TopologyTestDriver(
                builder.Build(),
                mkProperties(mkMap(
                    mkEntry(StreamsConfig.BootstrapServers, "dummy"),
                    mkEntry(StreamsConfig.ApplicationId, "test"),
                    mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory("kafka-test"))
                )),
                0L);
            ConsumerRecordFactory<string, string> recordFactory =
                new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L, 0L);

            driver.PipeInput(recordFactory.Create(topic1, "A", "1", 10L));
            driver.PipeInput(recordFactory.Create(topic1, "A", (string)null, 15L));
            driver.PipeInput(recordFactory.Create(topic1, "A", "1", 12L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "2", 20L));
            driver.PipeInput(recordFactory.Create(topic1, "null", "3", 25L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "4", 23L));
            driver.PipeInput(recordFactory.Create(topic1, "NULL", "5", 24L));
            driver.PipeInput(recordFactory.Create(topic1, "B", "7", 22L));

            Assert.Equal(
       Arrays.asList(
                     new KeyValueTimestamp<>("1", "0+1", 10),
                     new KeyValueTimestamp<>("1", "0+1-1", 15),
                     new KeyValueTimestamp<>("1", "0+1-1+1", 15),
                     new KeyValueTimestamp<>("2", "0+2", 20),
                         new KeyValueTimestamp<>("2", "0+2-2", 23),
                         new KeyValueTimestamp<>("4", "0+4", 23),
                         new KeyValueTimestamp<>("4", "0+4-4", 23),
                         new KeyValueTimestamp<>("7", "0+7", 22)),
                 supplier.TheCapturedProcessor().processed);
        }

        private static void testCountHelper(StreamsBuilder builder,
                                            string input,
                                            MockProcessorSupplier<string, object> supplier)
        {
            var driver = new TopologyTestDriver(
                builder.Build(),
                mkProperties(mkMap(
                    mkEntry(StreamsConfig.BootstrapServers, "dummy"),
                    mkEntry(StreamsConfig.ApplicationId, "test"),
                    mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory("kafka-test"))
                )),
                0L);
            ConsumerRecordFactory<string, string> recordFactory =
                new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L, 0L);

            driver.PipeInput(recordFactory.Create(input, "A", "green", 10L));
            driver.PipeInput(recordFactory.Create(input, "B", "green", 9L));
            driver.PipeInput(recordFactory.Create(input, "A", "blue", 12L));
            driver.PipeInput(recordFactory.Create(input, "C", "yellow", 15L));
            driver.PipeInput(recordFactory.Create(input, "D", "green", 11L));

            Assert.Equal(
           Arrays.asList(
                     new KeyValueTimestamp<>("green", 1L, 10),
                     new KeyValueTimestamp<>("green", 2L, 10),
                     new KeyValueTimestamp<>("green", 1L, 12),
                     new KeyValueTimestamp<>("blue", 1L, 12),
                     new KeyValueTimestamp<>("yellow", 1L, 15),
                     new KeyValueTimestamp<>("green", 2L, 12)),
                 supplier.TheCapturedProcessor().processed);
        }


        [Fact]
        public void testCount()
        {
            var builder = new StreamsBuilder();
            var input = "count-test-input";

            builder
                .Table(input, consumed)
                .GroupBy(MockMapper.selectValueKeyValueMapper(), stringSerialized)
                .Count(Materialized.As("count"))
                .ToStream()
                .Process(supplier);

            testCountHelper(builder, input, supplier);
        }

        [Fact]
        public void testCountWithInternalStore()
        {
            var builder = new StreamsBuilder();
            var input = "count-test-input";

            builder
                .Table(input, consumed)
                .GroupBy(MockMapper.selectValueKeyValueMapper(), stringSerialized)
                .Count()
                .ToStream()
                .Process(supplier);

            testCountHelper(builder, input, supplier);
        }

        [Fact]
        public void testRemoveOldBeforeAddNew()
        {
            var builder = new StreamsBuilder();
            var input = "count-test-input";
            MockProcessorSupplier<string, string> supplier = new MockProcessorSupplier<>();

            builder
                .Table(input, consumed)
                .GroupBy(new KeyValueMapper<string, string, string>(
                    (key, value) => KeyValuePair<string, string>.Pair(
                        key[0].ToString(),
                        key[1].ToString())),
                    stringSerialized)
                .Aggregate(
                    () => "",
                    (aggKey, value, aggregate) => aggregate + value,
                    (key, value, aggregate) => aggregate.replaceAll(value, ""),
                    Materialized.As<string, string, IKeyValueStore<Bytes, byte[]>>("someStore")
                        .WithValueSerde(Serdes.String()))
                .ToStream()
                .Process(supplier);

            var driver = new TopologyTestDriver(
                builder.Build(),
                mkProperties(mkMap(
                    mkEntry(StreamsConfig.BootstrapServers, "dummy"),
                    mkEntry(StreamsConfig.ApplicationId, "test"),
                    mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory("kafka-test"))
                )),
                0L);

            ConsumerRecordFactory<string, string> recordFactory =
                new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L, 0L);

            MockProcessor<string, string> proc = supplier.TheCapturedProcessor();

            driver.PipeInput(recordFactory.Create(input, "11", "A", 10L));
            driver.PipeInput(recordFactory.Create(input, "12", "B", 8L));
            driver.PipeInput(recordFactory.Create(input, "11", (string)null, 12L));
            driver.PipeInput(recordFactory.Create(input, "12", "C", 6L));

            Assert.Equal(
       Arrays.asList(
                     new KeyValueTimestamp<>("1", "1", 10),
                     new KeyValueTimestamp<>("1", "12", 10),
                     new KeyValueTimestamp<>("1", "2", 12),
                     new KeyValueTimestamp<>("1", "", 12),
                     new KeyValueTimestamp<>("1", "2", 12L)
                 ),
                 proc.processed
             );
        }
    }
}
