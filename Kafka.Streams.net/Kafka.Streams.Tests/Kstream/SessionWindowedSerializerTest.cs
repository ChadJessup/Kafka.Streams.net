using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.KStream;
using Kafka.Streams.Tests.Helpers;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class SessionWindowedSerializerTest
    {
        private readonly SessionWindowedSerializer<string> sessionWindowedKeySerializer;
        private readonly SessionWindowedSerializer<byte[]> sessionWindowedValueSerializer;
        private readonly Dictionary<string, string?> props = new Dictionary<string, string?>();

        public SessionWindowedSerializerTest()
        {
            var streamsBuilder = TestUtils.GetStreamsBuilder(new StreamsConfig(this.props));
            this.sessionWindowedKeySerializer = new SessionWindowedSerializer<string>(streamsBuilder.Services);
            this.sessionWindowedValueSerializer = new SessionWindowedSerializer<byte[]>(streamsBuilder.Services);

            props.Add(StreamsConfigPropertyNames.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.String().GetType().FullName);
            props.Add(StreamsConfigPropertyNames.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, Serdes.ByteArray().GetType().FullName);
        }

        [Fact]
        public void TestWindowedKeySerializerNoArgConstructors()
        {
            sessionWindowedKeySerializer.Configure(props, true);
            var inner = sessionWindowedKeySerializer.InnerSerializer();

            Assert.NotNull(inner);
            Assert.IsAssignableFrom<ISerializer<string>>(inner);
        }

        [Fact]
        public void TestWindowedValueSerializerNoArgConstructors()
        {
            sessionWindowedValueSerializer.Configure(props, false);
            var inner = sessionWindowedValueSerializer.InnerSerializer();

            Assert.NotNull(inner);
            Assert.IsAssignableFrom<ISerializer<byte[]>>(inner);
        }
    }
}
