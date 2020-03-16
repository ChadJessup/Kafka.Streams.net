//using Kafka.Streams;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Mappers;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.Tests.Helpers;
//using Xunit;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableAggregateTest
//    {
//        private ISerde<string> stringSerde = Serdes.String();
//        private Consumed<string, string> consumed = null; // Consumed.with(stringSerde, stringSerde);
//        private Grouped<string, string> stringSerialized = null;// Grouped.with(stringSerde, stringSerde);
//        private MockProcessorSupplier<string, object> supplier = new MockProcessorSupplier<>();

//        [Fact]
//        public void testAggBasic()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";

//            IKTable<string, string> table1 = builder.Table(topic1, consumed);
//            IKTable<string, string> table2 = table1
//                .groupBy(
//                    MockMapper.noOpKeyValueMapper(),
//                    stringSerialized)
//                .aggregate(
//                    MockInitializer.STRING_INIT,
//                    MockAggregator.TOSTRING_ADDER,
//                    MockAggregator.TOSTRING_REMOVER,
//                    Materialize.As<string, string, IKeyValueStore<Bytes, byte[]>>("topic1-Canonized")
//                        .withValueSerde(stringSerde));

//            table2.toStream().process(supplier);

//            var driver = new TopologyTestDriver(
//                builder.Build(),
//                mkProperties(mkMap(
//                    mkEntry(StreamsConfigPropertyNames.BootstrapServers, "dummy"),
//                    mkEntry(StreamsConfigPropertyNames.ApplicationId, "test"),
//                    mkEntry(StreamsConfigPropertyNames.STATE_DIR_CONFIG, TestUtils.GetTempDirectory("kafka-test"))
//                )),
//                0L);

//            ConsumerRecordFactory<string, string> recordFactory =
//                new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L, 0L);

//            driver.pipeInput(recordFactory.create(topic1, "A", "1", 10L));
//            driver.pipeInput(recordFactory.create(topic1, "B", "2", 15L));
//            driver.pipeInput(recordFactory.create(topic1, "A", "3", 20L));
//            driver.pipeInput(recordFactory.create(topic1, "B", "4", 18L));
//            driver.pipeInput(recordFactory.create(topic1, "C", "5", 5L));
//            driver.pipeInput(recordFactory.create(topic1, "D", "6", 25L));
//            driver.pipeInput(recordFactory.create(topic1, "B", "7", 15L));
//            driver.pipeInput(recordFactory.create(topic1, "C", "8", 10L));

//            Assert.Equal(
//       asList(
//                     new KeyValueTimestamp<>("A", "0+1", 10L),
//                     new KeyValueTimestamp<>("B", "0+2", 15L),
//                     new KeyValueTimestamp<>("A", "0+1-1", 20L),
//                     new KeyValueTimestamp<>("A", "0+1-1+3", 20L),
//                     new KeyValueTimestamp<>("B", "0+2-2", 18L),
//                     new KeyValueTimestamp<>("B", "0+2-2+4", 18L),
//                     new KeyValueTimestamp<>("C", "0+5", 5L),
//                     new KeyValueTimestamp<>("D", "0+6", 25L),
//                     new KeyValueTimestamp<>("B", "0+2-2+4-4", 18L),
//                     new KeyValueTimestamp<>("B", "0+2-2+4-4+7", 18L),
//                     new KeyValueTimestamp<>("C", "0+5-5", 10L),
//                     new KeyValueTimestamp<>("C", "0+5-5+8", 10L)),
//                 supplier.theCapturedProcessor().processed);
//        }

//        [Fact]
//        public void testAggRepartition()
//        {
//            var builder = new StreamsBuilder();
//            var topic1 = "topic1";

//            IKTable<string, string> table1 = builder.Table(topic1, consumed);
//            IKTable<string, string> table2 = table1
//                .groupBy(
//                    (key, value) =>
//                    {
//                        switch (key)
//                        {
//                            case "null":
//                                return KeyValue.Pair(null, value);
//                            case "NULL":
//                                return null;
//                            default:
//                                return KeyValue.Pair(value, value);
//                        }
//                    },
//                stringSerialized)
//            .aggregate(
//                MockInitializer.STRING_INIT,
//                MockAggregator.TOSTRING_ADDER,
//                MockAggregator.TOSTRING_REMOVER,
//                Materialize.As<string, string, IKeyValueStore<Bytes, byte[]>>("topic1-Canonized")
//                    .withValueSerde(stringSerde));

//            table2.toStream().process(supplier);

//            var driver = new TopologyTestDriver(
//                builder.Build(),
//                mkProperties(mkMap(
//                    mkEntry(StreamsConfigPropertyNames.BootstrapServers, "dummy"),
//                    mkEntry(StreamsConfigPropertyNames.ApplicationId, "test"),
//                    mkEntry(StreamsConfigPropertyNames.STATE_DIR_CONFIG, TestUtils.GetTempDirectory("kafka-test"))
//                )),
//                0L);
//            ConsumerRecordFactory<string, string> recordFactory =
//                new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L, 0L);

//            driver.pipeInput(recordFactory.create(topic1, "A", "1", 10L));
//            driver.pipeInput(recordFactory.create(topic1, "A", (string)null, 15L));
//            driver.pipeInput(recordFactory.create(topic1, "A", "1", 12L));
//            driver.pipeInput(recordFactory.create(topic1, "B", "2", 20L));
//            driver.pipeInput(recordFactory.create(topic1, "null", "3", 25L));
//            driver.pipeInput(recordFactory.create(topic1, "B", "4", 23L));
//            driver.pipeInput(recordFactory.create(topic1, "NULL", "5", 24L));
//            driver.pipeInput(recordFactory.create(topic1, "B", "7", 22L));

//            Assert.Equal(
//       asList(
//                     new KeyValueTimestamp<>("1", "0+1", 10),
//                     new KeyValueTimestamp<>("1", "0+1-1", 15),
//                     new KeyValueTimestamp<>("1", "0+1-1+1", 15),
//                     new KeyValueTimestamp<>("2", "0+2", 20),
//                         new KeyValueTimestamp<>("2", "0+2-2", 23),
//                         new KeyValueTimestamp<>("4", "0+4", 23),
//                         new KeyValueTimestamp<>("4", "0+4-4", 23),
//                         new KeyValueTimestamp<>("7", "0+7", 22)),
//                 supplier.theCapturedProcessor().processed);
//        }

//        private static void testCountHelper(StreamsBuilder builder,
//                                            string input,
//                                            MockProcessorSupplier<string, object> supplier)
//        {
//            var driver = new TopologyTestDriver(
//                builder.Build(),
//                mkProperties(mkMap(
//                    mkEntry(StreamsConfigPropertyNames.BootstrapServers, "dummy"),
//                    mkEntry(StreamsConfigPropertyNames.ApplicationId, "test"),
//                    mkEntry(StreamsConfigPropertyNames.STATE_DIR_CONFIG, TestUtils.GetTempDirectory("kafka-test"))
//                )),
//                0L);
//            ConsumerRecordFactory<string, string> recordFactory =
//                new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L, 0L);

//            driver.pipeInput(recordFactory.create(input, "A", "green", 10L));
//            driver.pipeInput(recordFactory.create(input, "B", "green", 9L));
//            driver.pipeInput(recordFactory.create(input, "A", "blue", 12L));
//            driver.pipeInput(recordFactory.create(input, "C", "yellow", 15L));
//            driver.pipeInput(recordFactory.create(input, "D", "green", 11L));

//            Assert.Equal(
//           asList(
//                     new KeyValueTimestamp<>("green", 1L, 10),
//                     new KeyValueTimestamp<>("green", 2L, 10),
//                     new KeyValueTimestamp<>("green", 1L, 12),
//                     new KeyValueTimestamp<>("blue", 1L, 12),
//                     new KeyValueTimestamp<>("yellow", 1L, 15),
//                     new KeyValueTimestamp<>("green", 2L, 12)),
//                 supplier.theCapturedProcessor().processed);
//        }


//        [Fact]
//        public void testCount()
//        {
//            var builder = new StreamsBuilder();
//            var input = "count-test-input";

//            builder
//                .Table(input, consumed)
//                .groupBy(MockMapper.selectValueKeyValueMapper(), stringSerialized)
//                .count(Materialized.As("count"))
//                .toStream()
//                .process(supplier);

//            testCountHelper(builder, input, supplier);
//        }

//        [Fact]
//        public void testCountWithInternalStore()
//        {
//            var builder = new StreamsBuilder();
//            var input = "count-test-input";

//            builder
//                .Table(input, consumed)
//                .groupBy(MockMapper.selectValueKeyValueMapper(), stringSerialized)
//                .count()
//                .toStream()
//                .process(supplier);

//            testCountHelper(builder, input, supplier);
//        }

//        [Fact]
//        public void testRemoveOldBeforeAddNew()
//        {
//            var builder = new StreamsBuilder();
//            var input = "count-test-input";
//            MockProcessorSupplier<string, string> supplier = new MockProcessorSupplier<>();

//            builder
//                .Table(input, consumed)
//                .groupBy(new KeyValueMapper<string, string, string>(
//                    (key, value) => KeyValue<string, string>.Pair(
//                        key[0].ToString(),
//                        key[1].ToString())),
//                    stringSerialized)
//                .aggregate(
//                    () => "",
//                    (aggKey, value, aggregate) => aggregate + value,
//                    (key, value, aggregate) => aggregate.replaceAll(value, ""),
//                    Materialize.As<string, string, IKeyValueStore<Bytes, byte[]>>("someStore")
//                        .withValueSerde(Serdes.String()))
//                .toStream()
//                .process(supplier);

//            var driver = new TopologyTestDriver(
//                builder.Build(),
//                mkProperties(mkMap(
//                    mkEntry(StreamsConfigPropertyNames.BootstrapServers, "dummy"),
//                    mkEntry(StreamsConfigPropertyNames.ApplicationId, "test"),
//                    mkEntry(StreamsConfigPropertyNames.STATE_DIR_CONFIG, TestUtils.GetTempDirectory("kafka-test"))
//                )),
//                0L);

//            ConsumerRecordFactory<string, string> recordFactory =
//                new ConsumerRecordFactory<>(Serdes.String(), Serdes.String(), 0L, 0L);

//            MockProcessor<string, string> proc = supplier.theCapturedProcessor();

//            driver.pipeInput(recordFactory.create(input, "11", "A", 10L));
//            driver.pipeInput(recordFactory.create(input, "12", "B", 8L));
//            driver.pipeInput(recordFactory.create(input, "11", (string)null, 12L));
//            driver.pipeInput(recordFactory.create(input, "12", "C", 6L));

//            Assert.Equal(
//       asList(
//                     new KeyValueTimestamp<>("1", "1", 10),
//                     new KeyValueTimestamp<>("1", "12", 10),
//                     new KeyValueTimestamp<>("1", "2", 12),
//                     new KeyValueTimestamp<>("1", "", 12),
//                     new KeyValueTimestamp<>("1", "2", 12L)
//                 ),
//                 proc.processed
//             );
//        }
//    }
//}
