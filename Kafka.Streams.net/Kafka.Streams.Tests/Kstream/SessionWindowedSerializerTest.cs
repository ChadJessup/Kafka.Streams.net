using Kafka.Streams.Configs;
using Kafka.Streams.KStream;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class SessionWindowedSerializerTest
    {
        private SessionWindowedSerializer<object> sessionWindowedSerializer;
        private Dictionary<string, string?> props = new Dictionary<string, string?>();

        public SessionWindowedSerializerTest()
        {
            this.sessionWindowedSerializer = new SessionWindowedSerializer<object>();

            props.Add(StreamsConfigPropertyNames.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.String().GetType().FullName);
            props.Add(StreamsConfigPropertyNames.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, Serdes.ByteArray().GetType().FullName);
        }

        [Fact]
        public void TestWindowedKeySerializerNoArgConstructors()
        {
            sessionWindowedSerializer.configure(props, true);
            var inner = sessionWindowedSerializer.innerSerializer();
            Assert.NotNull(inner);
            var stringSerdesType = Serdes.String().GetType();
            Assert.True(inner.GetType().Equals(stringSerdesType), "Inner serializer type should be StringSerializer");
        }

        [Fact]
        public void TestWindowedValueSerializerNoArgConstructors()
        {
            sessionWindowedSerializer.configure(props, false);
            var inner = sessionWindowedSerializer.innerSerializer();

            var byteArrayType = Serdes.ByteArray().GetType();
            Assert.NotNull(inner);
            Assert.True(inner.GetType().Equals(byteArrayType), "Inner serializer type should be ByteArraySerializer");
        }
    }
}
