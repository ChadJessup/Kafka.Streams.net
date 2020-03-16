using Kafka.Streams.Configs;
using Kafka.Streams.KStream;
using Moq;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class TimeWindowedSerializerTest
    {
        private TimeWindowedSerializer<object> timeWindowedSerializer;
        private Dictionary<string, string?> props = new Dictionary<string, string?>();

        public TimeWindowedSerializerTest()
        {
            this.timeWindowedSerializer = new TimeWindowedSerializer<object>(Mock.Of<IServiceProvider>());

            props.Add(StreamsConfigPropertyNames.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.String().GetType().FullName);
            props.Add(StreamsConfigPropertyNames.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, Serdes.ByteArray().GetType().FullName);
        }

        [Fact]
        public void TestWindowedKeySerializerNoArgConstructors()
        {
            timeWindowedSerializer.Configure(props, isKey: true);
            var inner = timeWindowedSerializer.InnerSerializer();
            Assert.NotNull(inner);
            var stringType = Serdes.String().GetType();
            Assert.True(inner.GetType().Equals(stringType), "Inner serializer type should be StringSerializer");
        }

        [Fact]
        public void TestWindowedValueSerializerNoArgConstructors()
        {
            timeWindowedSerializer.Configure(props, false);
            var inner = timeWindowedSerializer.InnerSerializer();
            Assert.NotNull(inner);
            Assert.IsAssignableFrom<byte[]>(inner);
        }
    }
}
