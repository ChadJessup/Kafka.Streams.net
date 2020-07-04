//using Confluent.Kafka;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using System;
//using Xunit;

//namespace Kafka.Streams.Tests.Kstream
//{
//    public class WindowedSerdesTest
//    {

//        private string topic = "sample";

//        [Fact]
//        public void shouldWrapForTimeWindowedSerde()
//        {
//            ISerde<IWindowed<string>> serde = WindowedSerdes.TimeWindowedSerdeFrom<string>();
//            Assert.True(serde.Serializer is TimeWindowedSerializer);
//            Assert.True(serde.Deserializer is TimeWindowedDeserializer);
//            Assert.True(((TimeWindowedSerializer)serde.Serializer).innerSerializer() is ISerializer<string>);
//            Assert.True(((TimeWindowedDeserializer)serde.Deserializer).innerDeserializer() is IDeserializer<string>);
//        }

//        [Fact]
//        public void shouldWrapForSessionWindowedSerde()
//        {
//            ISerde<IWindowed<string>> serde = WindowedSerdes.SessionWindowedSerdeFrom<string>();
//            Assert.True(serde.Serializer is SessionWindowedSerializer);
//            Assert.True(serde.Deserializer is SessionWindowedDeserializer);
//            Assert.True(((SessionWindowedSerializer)serde.Serializer).innerSerializer() is ISerializer<string>);
//            Assert.True(((SessionWindowedDeserializer)serde.Deserializer).innerDeserializer() is IDeserializer<string>);
//        }

//        [Fact]
//        public void testTimeWindowSerdeFrom()
//        {
//            IWindowed<int> timeWindowed = new Windowed<int>(10, new TimeWindow(0, long.MaxValue));
//            ISerde<IWindowed<int>> timeWindowedSerde = WindowedSerdes.TimeWindowedSerdeFrom<int>();
//            byte[] bytes = timeWindowedSerde.Serializer.Serialize(topic, timeWindowed);
//            IWindowed<int> windowed = timeWindowedSerde.Deserializer.Deserialize(topic, bytes);
//            Assert.Equal(timeWindowed, windowed);
//        }

//        [Fact]
//        public void testSessionWindowedSerdeFrom()
//        {
//            IWindowed<int> sessionWindowed = new Windowed<int>(10, new SessionWindow(0, 1));
//            ISerde<IWindowed<int>> sessionWindowedSerde = WindowedSerdes.SessionWindowedSerdeFrom<int>();
//            byte[] bytes = sessionWindowedSerde.Serializer.Serialize(topic, sessionWindowed);
//            IWindowed<int> windowed = sessionWindowedSerde.Deserializer.Deserialize(topic, bytes);
//            Assert.Equal(sessionWindowed, windowed);
//        }

//        //        [Fact]
//        //        public void timeWindowedSerializerShouldThrowNpeIfNotInitializedProperly()
//        //        {
//        //            TimeWindowedSerializer<byte[]> serializer = new TimeWindowedSerializer<>();
//        //            NullReferenceException exception =Assert.Throws(
//        //        NullReferenceException),
//        //                () => serializer.Serialize("topic", new Windowed<>(new byte[0], new TimeWindow(0, 1))));
//        //            Assert.Equal(
//        //                exception.Message,
//        //                equalTo("Inner serializer is `null`. User code must use constructor " +
//        //                    "`TimeWindowedSerializer(Serializer<T> inner)` instead of the no-arg constructor."));
//        //        }

//        [Fact]
//        public void timeWindowedSerializerShouldThrowNpeOnSerializingBaseKeyIfNotInitializedProperly()
//        {
//            TimeWindowedSerializer<byte[]> serializer = new TimeWindowedSerializer<byte[]>();
//            var exception = Assert.Throws<NullReferenceException>(
//                () => serializer.Serialize("topic", new Windowed<string>(Array.Empty<byte>(), new TimeWindow(0, 1))));

//            Assert.Equal(
//                exception.Message,
//                "Inner serializer is `null`. User code must use constructor " +
//                    "`TimeWindowedSerializer(Serializer<T> inner)` instead of the no-arg constructor.");
//        }

//        [Fact]
//        public void timeWindowedDeserializerShouldThrowNpeIfNotInitializedProperly()
//        {
//            TimeWindowedDeserializer<byte[]> deserializer = new TimeWindowedDeserializer<byte[]>();
//            var exception = Assert.Throws<NullReferenceException>(
//                () => deserializer.Deserialize("topic", System.Array.Empty<byte>()));

//            Assert.Equal(
//                exception.Message,
//                "Inner deserializer is `null`. User code must use constructor " +
//                    "`TimeWindowedDeserializer(Deserializer<T> inner)` instead of the no-arg constructor.");
//        }

//        [Fact]
//        public void sessionWindowedSerializerShouldThrowNpeIfNotInitializedProperly()
//        {
//            SessionWindowedSerializer<byte[]> serializer = new SessionWindowedSerializer<>();
//            var exception = Assert.Throws<NullReferenceException>(
//                () => serializer.Serialize("topic", new Windowed<string>(System.Array.Empty<byte>(), new SessionWindow(0, 0))));
//            Assert.Equal(
//                exception.Message,
//                "Inner serializer is `null`. User code must use constructor " +
//                    "`SessionWindowedSerializer(Serializer<T> inner)` instead of the no-arg constructor.");
//        }

//        [Fact]
//        public void sessionWindowedSerializerShouldThrowNpeOnSerializingBaseKeyIfNotInitializedProperly()
//        {
//            SessionWindowedSerializer<byte[]> serializer = new SessionWindowedSerializer<>();
//            NullReferenceException exception = Assert.Throws<NullReferenceException>(
//                () => serializer.Serialize.SerializeBaseKey("topic", new Windowed<string>(System.Array.Empty<byte>(), new SessionWindow(0, 0))));
//            Assert.Equal(
//                exception.Message,
//                "Inner serializer is `null`. User code must use constructor " +
//                    "`SessionWindowedSerializer(Serializer<T> inner)` instead of the no-arg constructor.");
//        }

//        [Fact]
//        public void sessionWindowedDeserializerShouldThrowNpeIfNotInitializedProperly()
//        {
//            SessionWindowedDeserializer<byte[]> deserializer = new SessionWindowedDeserializer<byte[]>();
//            var exception = Assert.Throws<NullReferenceException>(
//                () => deserializer.Deserialize("topic", System.Array.Empty<byte>()));

//            Assert.Equal(
//                exception.Message,
//                "Inner deserializer is `null`. User code must use constructor " +
//                    "`SessionWindowedDeserializer(Deserializer<T> inner)` instead of the no-arg constructor.");
//        }

//        [Fact]
//        public void timeWindowedSerializerShouldNotThrowOnCloseIfNotInitializedProperly()
//        {
//            new TimeWindowedSerializer<>().Close();
//        }

//        [Fact]
//        public void timeWindowedDeserializerShouldNotThrowOnCloseIfNotInitializedProperly()
//        {
//            new TimeWindowedDeserializer<>().Close();
//        }

//        [Fact]
//        public void sessionWindowedSerializerShouldNotThrowOnCloseIfNotInitializedProperly()
//        {
//            new SessionWindowedSerializer<>().Close();
//        }

//        [Fact]
//        public void sessionWindowedDeserializerShouldNotThrowOnCloseIfNotInitializedProperly()
//        {
//            new SessionWindowedDeserializer<>().Close();
//        }
//    }
//}
