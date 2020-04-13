using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.KStream;
using Kafka.Streams.Tests.Helpers;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class TimeWindowedDeserializerTest
    {
        private readonly long windowSize = 5000000;
        private readonly TimeWindowedDeserializer<string> timeWindowedKeyDeserializer;
        private readonly TimeWindowedDeserializer<byte[]> timeWindowedValueDeserializer;
        private readonly Dictionary<string, string?> props = new Dictionary<string, string?>();

        public TimeWindowedDeserializerTest()
        {
            this.props.Add(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.String().GetType().FullName);
            this.props.Add(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, Serdes.ByteArray().GetType().FullName);

            var streamsBuilder = TestUtils.GetStreamsBuilder(new StreamsConfig(this.props));
            this.timeWindowedKeyDeserializer = new TimeWindowedDeserializer<string>(streamsBuilder.Context.Services, null, this.windowSize);
            this.timeWindowedValueDeserializer = new TimeWindowedDeserializer<byte[]>(streamsBuilder.Context.Services, null, this.windowSize);
        }

        [Fact]
        public void TestWindowedKeyDeserializerNoArgConstructors()
        {
            this.timeWindowedKeyDeserializer.Configure(this.props, isKey: true);
            var inner = this.timeWindowedKeyDeserializer.InnerDeserializer();
            Assert.NotNull(inner);
            Assert.IsAssignableFrom<IDeserializer<string>>(inner);
        }

        [Fact]
        public void TestWindowedValueDeserializerNoArgConstructors()
        {
            this.timeWindowedValueDeserializer.Configure(this.props, isKey: false);
            var inner = this.timeWindowedValueDeserializer.InnerDeserializer();
            Assert.NotNull(inner);
            Assert.IsAssignableFrom<IDeserializer<byte[]>>(inner);
        }
    }
}
