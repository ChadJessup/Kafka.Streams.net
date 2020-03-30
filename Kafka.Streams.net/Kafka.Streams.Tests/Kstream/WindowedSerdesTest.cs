namespace Kafka.Streams.Tests.Kstream
{
}
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Xunit;

//namespace Kafka.Streams.Tests
//{
//    public class WindowedSerdesTest
//    {

//        private string topic = "sample";

//        [Fact]
//        public void shouldWrapForTimeWindowedSerde()
//        {
//            ISerde<Windowed<string>> serde = WindowedSerdes.timeWindowedSerdeFrom(string));
//            Assert.True(serde.Serializer is TimeWindowedSerializer);
//            Assert.True(serde.deserializer() is TimeWindowedDeserializer);
//            Assert.True(((TimeWindowedSerializer)serde.Serializer).innerSerializer() is StringSerializer);
//            Assert.True(((TimeWindowedDeserializer)serde.deserializer()).innerDeserializer() is StringDeserializer);
//        }

//        [Fact]
//        public void shouldWrapForSessionWindowedSerde()
//        {
//            ISerde<Windowed<string>> serde = WindowedSerdes.sessionWindowedSerdeFrom<string>());
//            Assert.True(serde.Serializer is SessionWindowedSerializer);
//            Assert.True(serde.deserializer() is SessionWindowedDeserializer);
//            Assert.True(((SessionWindowedSerializer)serde.Serializer).innerSerializer() is StringSerializer);
//            Assert.True(((SessionWindowedDeserializer)serde.deserializer()).innerDeserializer() is StringDeserializer);
//        }

//        [Fact]
//        public void testTimeWindowSerdeFrom()
//        {
//            Windowed<int> timeWindowed = new Windowed<>(10, new TimeWindow(0, long.MaxValue));
//            ISerde<Windowed<int>> timeWindowedSerde = WindowedSerdes.timeWindowedSerdeFrom<int>();
//            byte[] bytes = timeWindowedSerde.Serializer.serialize(topic, timeWindowed);
//            Windowed<int> windowed = timeWindowedSerde.deserializer().Deserialize(topic, bytes);
//            Assert.Equal(timeWindowed, windowed);
//        }

//        [Fact]
//        public void testSessionWindowedSerdeFrom()
//        {
//            Windowed<int> sessionWindowed = new Windowed<>(10, new SessionWindow(0, 1));
//            ISerde<Windowed<int>> sessionWindowedSerde = WindowedSerdes.sessionWindowedSerdeFrom(int));
//            byte[] bytes = sessionWindowedSerde.Serializer.serialize(topic, sessionWindowed);
//            Windowed<int> windowed = sessionWindowedSerde.deserializer().Deserialize(topic, bytes);
//            Assert.Equal(sessionWindowed, windowed);
//        }

//        //        [Fact]
//        //        public void timeWindowedSerializerShouldThrowNpeIfNotInitializedProperly()
//        //        {
//        //            TimeWindowedSerializer<byte[]> serializer = new TimeWindowedSerializer<>();
//        //            NullPointerException exception =Assert.Throws(
//        //        NullPointerException),
//        //                () => serializer.serialize("topic", new Windowed<>(new byte[0], new TimeWindow(0, 1))));
//        //            Assert.Equal(
//        //                exception.getMessage(),
//        //                equalTo("Inner serializer is `null`. User code must use constructor " +
//        //                    "`TimeWindowedSerializer(Serializer<T> inner)` instead of the no-arg constructor."));
//        //        }

//        [Fact]
//        public void timeWindowedSerializerShouldThrowNpeOnSerializingBaseKeyIfNotInitializedProperly()
//        {
//            TimeWindowedSerializer<byte[]> serializer = new TimeWindowedSerializer<>();
//            NullPointerException exception = Assert.Throws(
//        NullPointerException),
//                () => serializer.serialize.AseKey("topic", new Windowed<>(new byte[0], new TimeWindow(0, 1))));
//            Assert.Equal(
//                exception.getMessage(),
//                equalTo("Inner serializer is `null`. User code must use constructor " +
//                    "`TimeWindowedSerializer(Serializer<T> inner)` instead of the no-arg constructor."));
//        }

//        [Fact]
//        public void timeWindowedDeserializerShouldThrowNpeIfNotInitializedProperly()
//        {
//            TimeWindowedDeserializer<byte[]> deserializer = new TimeWindowedDeserializer<>();
//            NullPointerException exception = Assert.Throws(
//        NullPointerException),
//                () => deserializer.Deserialize("topic", new byte[0]));
//            Assert.Equal(
//                exception.getMessage(),
//                equalTo("Inner deserializer is `null`. User code must use constructor " +
//                    "`TimeWindowedDeserializer(Deserializer<T> inner)` instead of the no-arg constructor."));
//        }

//        [Fact]
//        public void sessionWindowedSerializerShouldThrowNpeIfNotInitializedProperly()
//        {
//            SessionWindowedSerializer<byte[]> serializer = new SessionWindowedSerializer<>();
//            NullPointerException exception = Assert.Throws(
//        NullPointerException),
//                () => serializer.serialize("topic", new Windowed<>(new byte[0], new SessionWindow(0, 0))));
//            Assert.Equal(
//                exception.getMessage(),
//                equalTo("Inner serializer is `null`. User code must use constructor " +
//                    "`SessionWindowedSerializer(Serializer<T> inner)` instead of the no-arg constructor."));
//        }

//        [Fact]
//        public void sessionWindowedSerializerShouldThrowNpeOnSerializingBaseKeyIfNotInitializedProperly()
//        {
//            SessionWindowedSerializer<byte[]> serializer = new SessionWindowedSerializer<>();
//            NullPointerException exception = Assert.Throws(
//        NullPointerException),
//                () => serializer.serialize.AseKey("topic", new Windowed<>(new byte[0], new SessionWindow(0, 0))));
//            Assert.Equal(
//                exception.getMessage(),
//                equalTo("Inner serializer is `null`. User code must use constructor " +
//                    "`SessionWindowedSerializer(Serializer<T> inner)` instead of the no-arg constructor."));
//        }

//        [Fact]
//        public void sessionWindowedDeserializerShouldThrowNpeIfNotInitializedProperly()
//        {
//            SessionWindowedDeserializer<byte[]> deserializer = new SessionWindowedDeserializer<>();
//            NullPointerException exception = Assert.Throws(
//        NullPointerException),
//                () => deserializer.Deserialize("topic", new byte[0]));
//            Assert.Equal(
//                exception.getMessage(),
//                equalTo("Inner deserializer is `null`. User code must use constructor " +
//                    "`SessionWindowedDeserializer(Deserializer<T> inner)` instead of the no-arg constructor."));
//        }

//        [Fact]
//        public void timeWindowedSerializerShouldNotThrowOnCloseIfNotInitializedProperly()
//        {
//            new TimeWindowedSerializer<>().close();
//        }

//        [Fact]
//        public void timeWindowedDeserializerShouldNotThrowOnCloseIfNotInitializedProperly()
//        {
//            new TimeWindowedDeserializer<>().close();
//        }

//        [Fact]
//        public void sessionWindowedSerializerShouldNotThrowOnCloseIfNotInitializedProperly()
//        {
//            new SessionWindowedSerializer<>().close();
//        }

//        [Fact]
//        public void sessionWindowedDeserializerShouldNotThrowOnCloseIfNotInitializedProperly()
//        {
//            new SessionWindowedDeserializer<>().close();
//        }
//    }
//}
