using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KStreamFlatMapValuesTest
    {
        private string topicName = "topic";
        private ConsumerRecordFactory<int, int> recordFactory =
            new ConsumerRecordFactory<int, int>(Serdes.Int(), Serdes.Int(), 0L);
        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());

        [Fact]
        public void testFlatMapValues()
        {
            var builder = new StreamsBuilder();

            ValueMapper<int, IEnumerable<string>> mapper =
                value =>
                {
                    List<string> result = new List<string>
                    {
                        "v" + value,
                        "V" + value
                    };
                    return result;
                };

            int[] expectedKeys = { 0, 1, 2, 3 };

            IKStream<int, int> stream = builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.Int()));
            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<int, string>();
            stream.FlatMapValues(mapper).Process(supplier);

            var driver = new TopologyTestDriver(builder.Build(), props);
            foreach (var expectedKey in expectedKeys)
            {
                // .Assing the timestamp to recordFactory.Create to disambiguate the call
                driver.PipeInput(recordFactory.Create(topicName, expectedKey, expectedKey, 0L));
            }

            KeyValueTimestamp<int, string>[] expected =
            {
                new KeyValueTimestamp<int, string>(0, "v0", 0), new KeyValueTimestamp<int, string>(0, "V0", 0),
                new KeyValueTimestamp<int, string>(1, "v1", 0), new KeyValueTimestamp<int, string>(1, "V1", 0),
                new KeyValueTimestamp<int, string>(2, "v2", 0), new KeyValueTimestamp<int, string>(2, "V2", 0),
                new KeyValueTimestamp<int, string>(3, "v3", 0), new KeyValueTimestamp<int, string>(3, "V3", 0),
            };

            Assert.Equal(expected, supplier.TheCapturedProcessor().processed.ToArray());
        }


        [Fact]
        public void testFlatMapValuesWithKeys()
        {
            var builder = new StreamsBuilder();

            ValueMapperWithKey<int, int, IEnumerable<string>> mapper =
                (readOnlyKey, value) =>
                {
                    return new List<string>
                    {
                        "v" + value,
                        "k" + readOnlyKey
                    };
                };

            int[] expectedKeys = { 0, 1, 2, 3 };

            IKStream<int, int> stream = builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.Int()));
            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<int, string>();

            stream.FlatMapValues(mapper).Process(supplier);


            var driver = new TopologyTestDriver(builder.Build(), props);
            foreach (var expectedKey in expectedKeys)
            {
                // .Assing the timestamp to recordFactory.Create to disambiguate the call
                driver.PipeInput(recordFactory.Create(topicName, expectedKey, expectedKey, 0L));
            }

            KeyValueTimestamp<int, string>[] expected =
            {
                new KeyValueTimestamp<int, string>(0, "v0", 0),
                new KeyValueTimestamp<int, string>(0, "k0", 0),
                new KeyValueTimestamp<int, string>(1, "v1", 0),
                new KeyValueTimestamp<int, string>(1, "k1", 0),
                new KeyValueTimestamp<int, string>(2, "v2", 0),
                new KeyValueTimestamp<int, string>(2, "k2", 0),
                new KeyValueTimestamp<int, string>(3, "v3", 0),
                new KeyValueTimestamp<int, string>(3, "k3", 0),
            };

            //Assert.Equal(expected, supplier.TheCapturedProcessor().Processor.ToArray());
        }
    }
}
