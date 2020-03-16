using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.KStream;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class TimeWindowedDeserializerTest
    {
        private long windowSize = 5000000;
        private TimeWindowedDeserializer<object> timeWindowedDeserializer;
        private Dictionary<string, string> props = new Dictionary<string, string>();

        public TimeWindowedDeserializerTest()
        {
            timeWindowedDeserializer = new TimeWindowedDeserializer<object>(null, windowSize);
            props.Add(StreamsConfigPropertyNames.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.String().GetType().FullName);
            props.Add(StreamsConfigPropertyNames.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, Serdes.ByteArray().GetType().FullName);
        }

        [Fact]
        public void TestWindowedKeyDeserializerNoArgConstructors()
        {
            timeWindowedDeserializer.configure(props, true);
            var inner = timeWindowedDeserializer.innerDeserializer();
            Assert.NotNull(inner);
            Assert.IsAssignableFrom<IDeserializer<string>>(inner);
        }

        [Fact]
        public void TestWindowedValueDeserializerNoArgConstructors()
        {
            timeWindowedDeserializer.configure(props, false);
            var inner = timeWindowedDeserializer.innerDeserializer();
            Assert.NotNull(inner);
            Assert.IsAssignableFrom<ISerializer<byte[]>>(inner);
        }
    }
}
