using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Integration;
using Kafka.Streams.Tests.Mocks;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KStreamGlobalKTableLeftJoinTest
    {
        private static readonly KeyValueTimestamp<int, string>[] EMPTY = Array.Empty<KeyValueTimestamp<int, string>>();

        private readonly string streamTopic = "streamTopic";
        private readonly string globalTableTopic = "globalTableTopic";
        private readonly int[] expectedKeys = { 0, 1, 2, 3 };

        private readonly MockProcessor<int, string> processor;
        private readonly TopologyTestDriver driver;
        private readonly StreamsBuilder builder;

        public KStreamGlobalKTableLeftJoinTest()
        {
            builder = new StreamsBuilder();
            IKStream<int, string> stream;
            IGlobalKTable<string, string> table; // value of stream optionally.Contains key of table
            KeyValueMapper<int, string, string> keyMapper;

            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<int, string>();
            Consumed<int, string> streamConsumed = Consumed.With(Serdes.Int(), Serdes.String());
            Consumed<string, string> tableConsumed = Consumed.With(Serdes.String(), Serdes.String());

            stream = builder.Stream(streamTopic, streamConsumed);
            table = builder.GlobalTable(globalTableTopic, tableConsumed);
            keyMapper = new KeyValueMapper<int, string, string>((key, value) =>
            {
                string[] tokens = value.Split(",");
                // Value is comma delimited. If second token is present, it's the key to the global ktable.
                // If not present, use null to indicate no match
                return tokens.Length > 1 ? tokens[1] : null;
            });

            stream.LeftJoin(table, keyMapper, MockValueJoiner.TOSTRING_JOINER()).Process(supplier);

            StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());
            driver = new TopologyTestDriver(builder.Context, builder.Build(), props);

            processor = supplier.TheCapturedProcessor();
        }


        public void cleanup()
        {
            driver.Close();
        }

        private void pushToStream(int messageCount, string valuePrefix, bool includeForeignKey)
        {
            ConsumerRecordFactory<int, string> recordFactory =
                new ConsumerRecordFactory<int, string>(Serdes.Int().Serializer, Serdes.String().Serializer, 0L, 1L);
            for (var i = 0; i < messageCount; i++)
            {
                var value = valuePrefix + expectedKeys[i];
                if (includeForeignKey)
                {
                    value = value + ",FKey" + expectedKeys[i];
                }
                driver.PipeInput(recordFactory.Create(streamTopic, expectedKeys[i], value));
            }
        }

        private void pushToGlobalTable(int messageCount, string valuePrefix)
        {
            ConsumerRecordFactory<string, string> recordFactory =
                new ConsumerRecordFactory<string, string>(Serdes.String().Serializer, Serdes.String().Serializer, 0L, 1L);
            for (var i = 0; i < messageCount; i++)
            {
                driver.PipeInput(recordFactory.Create(globalTableTopic, "FKey" + expectedKeys[i], valuePrefix + expectedKeys[i]));
            }
        }

        private void pushNullValueToGlobalTable(int messageCount)
        {
            ConsumerRecordFactory<string, string> recordFactory =
                new ConsumerRecordFactory<string, string>(Serdes.String().Serializer, Serdes.String().Serializer, 0L, 1L);
            for (var i = 0; i < messageCount; i++)
            {
                driver.PipeInput(recordFactory.Create(globalTableTopic, "FKey" + expectedKeys[i], (string)null));
            }
        }

        [Fact]
        public void shouldNotRequireCopartitioning()
        {
            var CopartitionGroups =
                TopologyWrapper.getInternalTopologyBuilder(builder.Build()).CopartitionGroups();

            // "KStream-GlobalKTable joins do not need to be co-partitioned"
            Assert.Empty(CopartitionGroups);
        }

        [Fact]
        public void shouldNotJoinWithEmptyGlobalTableOnStreamUpdates()
        {

            // push two items to the primary stream. the globalTable is empty

            pushToStream(2, "X", true);
            processor.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, "X0,FKey0+null", 0),
                new KeyValueTimestamp<int, string>(1, "X1,FKey1+null", 1));
        }

        [Fact]
        public void shouldNotJoinOnGlobalTableUpdates()
        {

            // push two items to the primary stream. the globalTable is empty

            pushToStream(2, "X", true);
            processor.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, "X0,FKey0+null", 0),
                new KeyValueTimestamp<int, string>(1, "X1,FKey1+null", 1));

            // push two items to the globalTable. this should not produce any item.

            pushToGlobalTable(2, "Y");
            processor.CheckAndClearProcessResult(EMPTY);

            // push All four items to the primary stream. this should produce four items.

            pushToStream(4, "X", true);
            processor.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, "X0,FKey0+Y0", 0),
                new KeyValueTimestamp<int, string>(1, "X1,FKey1+Y1", 1),
                new KeyValueTimestamp<int, string>(2, "X2,FKey2+null", 2),
                new KeyValueTimestamp<int, string>(3, "X3,FKey3+null", 3));

            // push All items to the globalTable. this should not produce any item

            pushToGlobalTable(4, "YY");
            processor.CheckAndClearProcessResult(EMPTY);

            // push All four items to the primary stream. this should produce four items.

            pushToStream(4, "X", true);
            processor.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, "X0,FKey0+YY0", 0),
                new KeyValueTimestamp<int, string>(1, "X1,FKey1+YY1", 1),
                new KeyValueTimestamp<int, string>(2, "X2,FKey2+YY2", 2),
                new KeyValueTimestamp<int, string>(3, "X3,FKey3+YY3", 3));

            // push All items to the globalTable. this should not produce any item

            pushToGlobalTable(4, "YYY");
            processor.CheckAndClearProcessResult(EMPTY);
        }

        [Fact]
        public void shouldJoinRegardlessIfMatchFoundOnStreamUpdates()
        {
            // push two items to the globalTable. this should not produce any item.

            pushToGlobalTable(2, "Y");
            processor.CheckAndClearProcessResult(EMPTY);

            // push All four items to the primary stream. this should produce four items.

            pushToStream(4, "X", true);
            processor.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, "X0,FKey0+Y0", 0),
                new KeyValueTimestamp<int, string>(1, "X1,FKey1+Y1", 1),
                new KeyValueTimestamp<int, string>(2, "X2,FKey2+null", 2),
                new KeyValueTimestamp<int, string>(3, "X3,FKey3+null", 3));

        }

        [Fact]
        public void shouldClearGlobalTableEntryOnNullValueUpdates()
        {

            // push All four items to the globalTable. this should not produce any item.

            pushToGlobalTable(4, "Y");
            processor.CheckAndClearProcessResult(EMPTY);

            // push All four items to the primary stream. this should produce four items.

            pushToStream(4, "X", true);
            processor.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, "X0,FKey0+Y0", 0),
                new KeyValueTimestamp<int, string>(1, "X1,FKey1+Y1", 1),
                new KeyValueTimestamp<int, string>(2, "X2,FKey2+Y2", 2),
                new KeyValueTimestamp<int, string>(3, "X3,FKey3+Y3", 3));

            // push two items with null to the globalTable.As deletes. this should not produce any item.

            pushNullValueToGlobalTable(2);
            processor.CheckAndClearProcessResult(EMPTY);

            // push All four items to the primary stream. this should produce four items.

            pushToStream(4, "XX", true);
            processor.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, "XX0,FKey0+null", 0),
                new KeyValueTimestamp<int, string>(1, "XX1,FKey1+null", 1),
                new KeyValueTimestamp<int, string>(2, "XX2,FKey2+Y2", 2),
                new KeyValueTimestamp<int, string>(3, "XX3,FKey3+Y3", 3));
        }

        [Fact]
        public void shouldJoinOnNullKeyMapperValues()
        {

            // push All items to the globalTable. this should not produce any item

            pushToGlobalTable(4, "Y");
            processor.CheckAndClearProcessResult(EMPTY);

            // push All four items to the primary stream with no foreign key, resulting in null keyMapper values.
            // this should produce four items.

            pushToStream(4, "XXX", false);
            processor.CheckAndClearProcessResult(
                new KeyValueTimestamp<int, string>(0, "XXX0+null", 0),
                new KeyValueTimestamp<int, string>(1, "XXX1+null", 1),
                new KeyValueTimestamp<int, string>(2, "XXX2+null", 2),
                new KeyValueTimestamp<int, string>(3, "XXX3+null", 3));
        }
    }
}
