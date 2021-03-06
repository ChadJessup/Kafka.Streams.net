using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Tests.Mocks;
using System;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KStreamTransformValuesTest
    {
        private readonly string topicName = "topic";
        private readonly MockProcessorSupplier<int, int> supplier = new MockProcessorSupplier<int, int>();
        private readonly ConsumerRecordFactory<int, int> recordFactory =
            new ConsumerRecordFactory<int, int>(Serdes.Int(), Serdes.Int(), 0L);

        private readonly StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.Int());
        private readonly IProcessorContext context;

        private class ValueTransformerSupplier<K, V> : IValueTransformerSupplier<K, V>
        {
            private class ValueTransformer : IValueTransformer<K, V>
            {
                private int total = 0;

                public void Close()
                {
                }

                public void Init(IProcessorContext context)
                {
                }

                public V Transform(K key, V value)
                {
                    total += (int)(object)value;
                    return (V)(object)total;
                }
            }

            public IValueTransformer<K, V> Get()
            {
                return new ValueTransformer();
            }
        }

        public KStreamTransformValuesTest()
        {

        }

        [Fact]
        public void TestTransform()
        {
            var builder = new StreamsBuilder();

            IValueTransformerSupplier<int, int> valueTransformerSupplier =
                new ValueTransformerSupplier<int, int>();

            int[] expectedKeys = { 1, 10, 100, 1000 };

            IKStream<int, int> stream;
            stream = builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.Int()));
            stream.TransformValues(valueTransformerSupplier).Process(supplier);

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
            foreach (var expectedKey in expectedKeys)
            {
                driver.PipeInput(recordFactory.Create(topicName, expectedKey, expectedKey * 10, expectedKey / 2L));
            }
            //}

            var expected = new KeyValueTimestamp<int, int>[]
            {
                new KeyValueTimestamp<int, int>(1, 10, 0),
                new KeyValueTimestamp<int, int>(10, 110, 5),
                new KeyValueTimestamp<int, int>(100, 1110, 50),
                new KeyValueTimestamp<int, int>(1000, 11110, 500),
            };

            Assert.Equal(expected, supplier.TheCapturedProcessor().processed.ToArray());
        }

        // [Fact]
        // public void testTransformWithKey()
        // {
        //     var builder = new StreamsBuilder();
        //
        //     ValueTransformerWithKeySupplier<int, int, int> valueTransformerSupplier = null;
        //     //            () => new ValueTransformerWithKey<int, int, int>()
        //     //            {
        //     //                    private int total = 0;
        //
        //
        //     //    public void Init(IProcessorContext context) { }
        //
        //
        //     //    public int transform(int readOnlyKey, int value)
        //     //    {
        //     //        total += value.intValue() + readOnlyKey;
        //     //        return total;
        //     //    }
        //
        //
        //     //    public void Close() { }
        //     //};
        //
        //     int[] expectedKeys = { 1, 10, 100, 1000 };
        //
        //     IKStream<int, int> stream;
        //     stream = builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.Int()));
        //     stream.TransformValues(valueTransformerSupplier).Process(supplier);
        //
        //     var driver = new TopologyTestDriver(builder.Build(), props);
        //     foreach (var expectedKey in expectedKeys)
        //     {
        //         driver.PipeInput(recordFactory.Create(topicName, expectedKey, expectedKey * 10, expectedKey / 2L));
        //     }
        //     //}
        //
        //     var expected = new KeyValueTimestamp<int, int>[]
        //     {
        //         new KeyValueTimestamp<int, int>(1, 11, 0),
        //         new KeyValueTimestamp<int, int>(10, 121, 5),
        //         new KeyValueTimestamp<int, int>(100, 1221, 50),
        //         new KeyValueTimestamp<int, int>(1000, 12221, 500),
        //     };
        //
        //     Assert.Equal(expected, supplier.TheCapturedProcessor().processed.ToArray());
        // }


        //[Fact]
        //public void shouldInitializeTransformerWithForwardDisabledProcessorContext()
        //{
        //    SingletonNoOpValueTransformer<string, string> transformer = new SingletonNoOpValueTransformer<>();
        //    KStreamTransformValues<string, string, string> transformValues = new KStreamTransformValues<>(transformer);
        //    Processor<string, string> processor = transformValues.Get();
        //
        //    processor.Init(context);
        //
        //    Assert.Equal(transformer.context, typeof(ForwardingDisabledProcessorContext<string, string>));
        //}
    }
}
