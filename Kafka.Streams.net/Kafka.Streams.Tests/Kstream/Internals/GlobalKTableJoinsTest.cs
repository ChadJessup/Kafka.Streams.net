using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class GlobalKTableJoinsTest
    {
        private readonly StreamsBuilder builder = new StreamsBuilder();
        private readonly string streamTopic = "stream";
        private readonly string globalTopic = "global";
        private readonly IGlobalKTable<string, string> global;
        private readonly IKStream<string, string> stream;
        private readonly KeyValueMapper<string, string, string> keyValueMapper;


        public GlobalKTableJoinsTest()
        {
            Consumed<string, string> consumed = Consumed.With(Serdes.String(), Serdes.String());
            global = builder.GlobalTable(globalTopic, consumed);
            stream = builder.Stream(streamTopic, consumed);
            keyValueMapper = (key, value) => value;
        }

        [Fact]
        public void ShouldLeftJoinWithStream()
        {
            MockProcessorSupplier<string, string> supplier = new MockProcessorSupplier<string, string>();
            stream
                .LeftJoin(global, keyValueMapper, MockValueJoiner.TOSTRING_JOINER())
                .Process(supplier);

            Dictionary<string, IValueAndTimestamp<string>?> expected = new Dictionary<string, IValueAndTimestamp<string>?>
            {
                { "1", ValueAndTimestamp.Make("a+A", 2L) },
                { "2", ValueAndTimestamp.Make("b+B", 10L) },
                { "3", ValueAndTimestamp.Make("c+null", 3L) }
            };

            VerifyJoin(expected, supplier);
        }

        [Fact]
        public void ShouldInnerJoinWithStream()
        {
            MockProcessorSupplier<string, string> supplier = new MockProcessorSupplier<string, string>();
            stream
                .Join(global, keyValueMapper, MockValueJoiner.TOSTRING_JOINER())
                .Process(supplier);

            Dictionary<string, IValueAndTimestamp<string>?> expected = new Dictionary<string, IValueAndTimestamp<string>?>
            {
                { "1", ValueAndTimestamp.Make("a+A", 2L) },
                { "2", ValueAndTimestamp.Make("b+B", 10L) }
            };

            VerifyJoin(expected, supplier);
        }

        private void VerifyJoin(
            Dictionary<string, IValueAndTimestamp<string>?> expected,
            MockProcessorSupplier<string, string> supplier)
        {
            ConsumerRecordFactory<string, string> recordFactory = new ConsumerRecordFactory<string, string>(Serdes.String().Serializer, Serdes.String().Serializer);
            StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.String(), Serdes.String());

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
            // write some data to the global table
            driver.PipeInput(recordFactory.Create(globalTopic, "a", "A", 1L));
            driver.PipeInput(recordFactory.Create(globalTopic, "b", "B", 5L));
            //write some data to the stream
            driver.PipeInput(recordFactory.Create(streamTopic, "1", "a", 2L));
            driver.PipeInput(recordFactory.Create(streamTopic, "2", "b", 10L));
            driver.PipeInput(recordFactory.Create(streamTopic, "3", "c", 3L));

            Assert.Equal(expected, supplier.TheCapturedProcessor().LastValueAndTimestampPerKey);
        }
    }
}
