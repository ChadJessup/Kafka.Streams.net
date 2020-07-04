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
            this.sessionWindowedKeyDeserializer = new SessionWindowedDeserializer<string>(streamsBuilder.Context.Services);
            this.sessionWindowedValueDeserializer = new SessionWindowedDeserializer<byte[]>(streamsBuilder.Context.Services);

            this.props.Add(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASSConfig, Serdes.String().GetType().FullName);
            this.props.Add(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASSConfig, Serdes.ByteArray().GetType().FullName);
        }

        [Fact]
        public void TestWindowedKeyDeserializerNoArgConstructors()
        {
            this.sessionWindowedKeyDeserializer.Configure(this.props, true);
            var inner = this.sessionWindowedKeyDeserializer.InnerDeserializer();

            Assert.NotNull(inner);
            Assert.IsAssignableFrom<IDeserializer<string>>(inner);
        }

        [Fact]
        public void TestWindowedValueDeserializerNoArgConstructors()
        {
            this.sessionWindowedValueDeserializer.Configure(this.props, false);
            var inner = this.sessionWindowedValueDeserializer.InnerDeserializer();

            Assert.NotNull(inner);
            Assert.IsAssignableFrom<IDeserializer<byte[]>>(inner);
        }
    }
}
