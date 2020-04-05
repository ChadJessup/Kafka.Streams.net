using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.KStream;
using Kafka.Streams.Tests.Helpers;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class SessionWindowedDeserializerTest
    {
        private readonly SessionWindowedDeserializer<string> sessionWindowedKeyDeserializer;
        private readonly SessionWindowedDeserializer<byte[]> sessionWindowedValueDeserializer;
        private readonly Dictionary<string, string?> props = new Dictionary<string, string?>();

        public SessionWindowedDeserializerTest()
        {
            var streamsBuilder = TestUtils.GetStreamsBuilder(new StreamsConfig(this.props));
            this.sessionWindowedKeyDeserializer = new SessionWindowedDeserializer<string>(streamsBuilder.Services);
            this.sessionWindowedValueDeserializer = new SessionWindowedDeserializer<byte[]>(streamsBuilder.Services);

            props.Add(StreamsConfigPropertyNames.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.String().GetType().FullName);
            props.Add(StreamsConfigPropertyNames.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, Serdes.ByteArray().GetType().FullName);
        }

        [Fact]
        public void TestWindowedKeyDeserializerNoArgConstructors()
        {
            sessionWindowedKeyDeserializer.Configure(props, true);
            var inner = sessionWindowedKeyDeserializer.InnerDeserializer();

            Assert.NotNull(inner);
            Assert.IsAssignableFrom<IDeserializer<string>>(inner);
        }

        [Fact]
        public void TestWindowedValueDeserializerNoArgConstructors()
        {
            sessionWindowedValueDeserializer.Configure(props, false);
            var inner = sessionWindowedValueDeserializer.InnerDeserializer();

            Assert.NotNull(inner);
            Assert.IsAssignableFrom<IDeserializer<byte[]>>(inner);
        }
    }
}
