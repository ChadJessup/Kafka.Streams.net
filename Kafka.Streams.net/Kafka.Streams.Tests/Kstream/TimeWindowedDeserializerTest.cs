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
        private long windowSize = 5000000;
        private TimeWindowedDeserializer<string> timeWindowedKeyDeserializer;
        private TimeWindowedDeserializer<byte[]> timeWindowedValueDeserializer;
        private Dictionary<string, string?> props = new Dictionary<string, string?>();

        public TimeWindowedDeserializerTest()
        {
            props.Add(StreamsConfigPropertyNames.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.String().GetType().FullName);
            props.Add(StreamsConfigPropertyNames.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, Serdes.ByteArray().GetType().FullName);

            var streamsBuilder = TestUtils.GetStreamsBuilder(new StreamsConfig(this.props));
            timeWindowedKeyDeserializer = new TimeWindowedDeserializer<string>(streamsBuilder.Services, null, windowSize);
            timeWindowedValueDeserializer = new TimeWindowedDeserializer<byte[]>(streamsBuilder.Services, null, windowSize);
        }

        [Fact]
        public void TestWindowedKeyDeserializerNoArgConstructors()
        {
            timeWindowedKeyDeserializer.Configure(props, isKey: true);
            var inner = timeWindowedKeyDeserializer.innerDeserializer();
            Assert.NotNull(inner);
            Assert.IsAssignableFrom<IDeserializer<string>>(inner);
        }

        [Fact]
        public void TestWindowedValueDeserializerNoArgConstructors()
        {
            timeWindowedValueDeserializer.Configure(props, isKey: false);
            var inner = timeWindowedValueDeserializer.innerDeserializer();
            Assert.NotNull(inner);
            Assert.IsAssignableFrom<IDeserializer<byte[]>>(inner);
        }
    }
}
