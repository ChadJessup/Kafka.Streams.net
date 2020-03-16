using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.KStream;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class SessionWindowedDeserializerTest
    {
        private SessionWindowedDeserializer<object> sessionWindowedDeserializer;
        private Dictionary<string, string?> props = new Dictionary<string, string?>();

        public SessionWindowedDeserializerTest()
        {
            this.sessionWindowedDeserializer = new SessionWindowedDeserializer<object>();

            props.Add(StreamsConfigPropertyNames.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.String().GetType().FullName);
            props.Add(StreamsConfigPropertyNames.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, Serdes.ByteArray().GetType().FullName);
        }

        [Fact]
        public void TestWindowedKeyDeserializerNoArgConstructors()
        {
            sessionWindowedDeserializer.configure(props, true);
            var inner = sessionWindowedDeserializer.innerDeserializer();
            var stringSerType = Serdes.String().GetType();

            Assert.NotNull(inner);
            Assert.True(inner.GetType().Equals(stringSerType), "Inner deserializer type should be StringDeserializer");
        }

        [Fact]
        public void TestWindowedValueDeserializerNoArgConstructors()
        {
            sessionWindowedDeserializer.configure(props, false);
            var inner = sessionWindowedDeserializer.innerDeserializer();
            Assert.NotNull(inner);

            Assert.IsAssignableFrom<IDeserializer<string>>(inner);
        }
    }
}
