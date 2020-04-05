//using Confluent.Kafka;
//using Kafka.Streams.Nodes;
//using System;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    public class SourceNodeTest
//    {
//        [Fact]
//        public void ShouldProvideTopicHeadersAndDataToKeyDeserializer()
//        {
//            SourceNode<string, string> sourceNode = new MockSourceNode<>(new string[] { "" }, new TheDeserializer(), new TheDeserializer());
//            Headers headers = new Headers();
//            string deserializeKey = sourceNode.DeserializeKey("topic", headers, "data".getBytes(StandardCharsets.UTF_8));
//            Assert.Equal(deserializeKey, "topic" + headers + "data");
//        }

//        [Fact]
//        public void ShouldProvideTopicHeadersAndDataToValueDeserializer()
//        {
//            SourceNode<string, string> sourceNode = new MockSourceNode<>(new string[] { "" }, new TheDeserializer(), new TheDeserializer());
//            Headers headers = new Headers();
//            string deserializedValue = sourceNode.DeserializeValue("topic", headers, "data".getBytes(StandardCharsets.UTF_8));
//            Assert.Equal(deserializedValue, "topic" + headers + "data");
//        }

//        public class TheDeserializer : IDeserializer<string>
//        {
//            public string Deserialize(string topic, Headers headers, byte[] data)
//            {
//                return topic + headers + new string(data, StandardCharsets.UTF_8);
//            }

//            public string Deserialize(string topic, byte[] data)
//            {
//                return Deserialize(topic, null, data);
//            }

//            public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
//            {
//                throw new NotImplementedException();
//            }
//        }
//    }
//}
