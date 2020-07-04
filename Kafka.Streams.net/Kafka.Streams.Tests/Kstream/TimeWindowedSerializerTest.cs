using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.KStream;
using Kafka.Streams.Tests.Helpers;
using Moq;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class TimeWindowedSerializerTest
    {
        private readonly TimeWindowedSerializer<string> timeWindowedKeySerializer;
        private readonly TimeWindowedSerializer<byte[]> timeWindowedValueSerializer;
        private readonly Dictionary<string, string?> props = new Dictionary<string, string?>();

        public TimeWindowedSerializerTest()
        {
            this.props.Add(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASSConfig, Serdes.String().GetType().FullName);
            this.props.Add(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASSConfig, Serdes.ByteArray().GetType().FullName);

            var streamsBuilder = TestUtils.GetStreamsBuilder(new StreamsConfig(this.props));
            this.timeWindowedKeySerializer = new TimeWindowedSerializer<string>(streamsBuilder.Context.Services);
            this.timeWindowedValueSerializer = new TimeWindowedSerializer<byte[]>(streamsBuilder.Context.Services);
        }

        [Fact]
        public void TestWindowedKeySerializerNoArgConstructors()
        {
            this.timeWindowedKeySerializer.Configure(this.props, isKey: true);
            var inner = this.timeWindowedKeySerializer.InnerSerializer();

            Assert.NotNull(inner);
            Assert.IsAssignableFrom<ISerializer<string>>(inner);
        }

        [Fact]
        public void TestWindowedValueSerializerNoArgConstructors()
        {
            this.timeWindowedValueSerializer.Configure(this.props, isKey: false);
            var inner = this.timeWindowedValueSerializer.InnerSerializer();

            Assert.NotNull(inner);
            Assert.IsAssignableFrom<ISerializer<byte[]>>(inner);
        }
    }
}
