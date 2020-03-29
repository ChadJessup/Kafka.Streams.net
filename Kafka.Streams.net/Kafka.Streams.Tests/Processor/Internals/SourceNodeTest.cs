using Confluent.Kafka;
using Kafka.Streams.Nodes;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class SourceNodeTest
    {
        [Xunit.Fact]
        public void shouldProvideTopicHeadersAndDataToKeyDeserializer()
        {
            SourceNode<string, string> sourceNode = new MockSourceNode<>(new string[] { "" }, new TheDeserializer(), new TheDeserializer());
            Headers headers = new Headers();
            string deserializeKey = sourceNode.deserializeKey("topic", headers, "data".getBytes(StandardCharsets.UTF_8));
            Assert.Equal(deserializeKey, ("topic" + headers + "data"));
        }

        [Xunit.Fact]
        public void shouldProvideTopicHeadersAndDataToValueDeserializer()
        {
            SourceNode<string, string> sourceNode = new MockSourceNode<>(new string[] { "" }, new TheDeserializer(), new TheDeserializer());
            Headers headers = new Headers();
            string deserializedValue = sourceNode.deserializeValue("topic", headers, "data".getBytes(StandardCharsets.UTF_8));
            Assert.Equal(deserializedValue, ("topic" + headers + "data"));
        }

        public class TheDeserializer : IDeserializer<string>
        {

            public string Deserialize(string topic, Headers headers, byte[] data)
            {
                return topic + headers + new string(data, StandardCharsets.UTF_8);
            }


            public string Deserialize(string topic, byte[] data)
            {
                return Deserialize(topic, null, data);
            }
        }
    }
}
