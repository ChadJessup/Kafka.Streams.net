using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using Xunit;

namespace Kafka.Streams.Tests.Kstream
{
    public class WindowedSerdesTest
    {

        private string topic = "sample";

        [Fact]
        public void shouldWrapForTimeWindowedSerde()
        {
            ISerde<IWindowed<string>> serde = WindowedSerdes.TimeWindowedSerdeFrom(string));
            Assert.True(serde.Serializer is TimeWindowedSerializer);
            Assert.True(serde.deserializer() is TimeWindowedDeserializer);
            Assert.True(((TimeWindowedSerializer)serde.Serializer).innerSerializer() is Serdes.String().Serializer);
            Assert.True(((TimeWindowedDeserializer)serde.deserializer()).innerDeserializer() is Serdes.String().Deserializer);
        }

        [Fact]
        public void shouldWrapForSessionWindowedSerde()
        {
            ISerde<IWindowed<string>> serde = WindowedSerdes.sessionWindowedSerdeFrom<string>());
            Assert.True(serde.Serializer is SessionWindowedSerializer);
            Assert.True(serde.deserializer() is SessionWindowedDeserializer);
            Assert.True(((SessionWindowedSerializer)serde.Serializer).innerSerializer() is Serdes.String().Serializer);
            Assert.True(((SessionWindowedDeserializer)serde.deserializer()).innerDeserializer() is Serdes.String().Deserializer);
        }

        [Fact]
        public void testTimeWindowSerdeFrom()
        {
            IWindowed<int> timeWindowed = new Windowed<string>(10, new TimeWindow(0, long.MaxValue));
            ISerde<IWindowed<int>> timeWindowedSerde = WindowedSerdes.TimeWindowedSerdeFrom<int>();
            byte[] bytes = timeWindowedSerde.Serializer.Serialize(topic, timeWindowed);
            IWindowed<int> windowed = timeWindowedSerde.deserializer().Deserialize(topic, bytes);
            Assert.Equal(timeWindowed, windowed);
        }

        [Fact]
        public void testSessionWindowedSerdeFrom()
        {
            IWindowed<int> sessionWindowed = new Windowed<string>(10, new SessionWindow(0, 1));
            ISerde<IWindowed<int>> sessionWindowedSerde = WindowedSerdes.sessionWindowedSerdeFrom(int));
            byte[] bytes = sessionWindowedSerde.Serializer.Serialize(topic, sessionWindowed);
            IWindowed<int> windowed = sessionWindowedSerde.deserializer().Deserialize(topic, bytes);
            Assert.Equal(sessionWindowed, windowed);
        }

        //        [Fact]
        //        public void timeWindowedSerializerShouldThrowNpeIfNotInitializedProperly()
        //        {
        //            TimeWindowedSerializer<byte[]> serializer = new TimeWindowedSerializer<>();
        //            NullPointerException exception =Assert.Throws(
        //        NullPointerException),
        //                () => serializer.Serialize("topic", new Windowed<>(new byte[0], new TimeWindow(0, 1))));
        //            Assert.Equal(
        //                exception.getMessage(),
        //                equalTo("Inner serializer is `null`. User code must use constructor " +
        //                    "`TimeWindowedSerializer(Serializer<T> inner)` instead of the no-arg constructor."));
        //        }

        [Fact]
        public void timeWindowedSerializerShouldThrowNpeOnSerializingBaseKeyIfNotInitializedProperly()
        {
            TimeWindowedSerializer<byte[]> serializer = new TimeWindowedSerializer<>();
            NullPointerException exception = Assert.Throws(
        NullPointerException),
                () => serializer.Serialize.AseKey("topic", new Windowed<string>(System.Array.Empty<byte>(), new TimeWindow(0, 1))));
            Assert.Equal(
                exception.getMessage(),
                equalTo("Inner serializer is `null`. User code must use constructor " +
                    "`TimeWindowedSerializer(Serializer<T> inner)` instead of the no-arg constructor."));
        }

        [Fact]
        public void timeWindowedDeserializerShouldThrowNpeIfNotInitializedProperly()
        {
            TimeWindowedDeserializer<byte[]> deserializer = new TimeWindowedDeserializer<>();
            NullPointerException exception = Assert.Throws(
        NullPointerException),
                () => deserializer.Deserialize("topic", System.Array.Empty<byte>()));
            Assert.Equal(
                exception.getMessage(),
                equalTo("Inner deserializer is `null`. User code must use constructor " +
                    "`TimeWindowedDeserializer(Deserializer<T> inner)` instead of the no-arg constructor."));
        }

        [Fact]
        public void sessionWindowedSerializerShouldThrowNpeIfNotInitializedProperly()
        {
            SessionWindowedSerializer<byte[]> serializer = new SessionWindowedSerializer<>();
            NullPointerException exception = Assert.Throws(
        NullPointerException),
                () => serializer.Serialize("topic", new Windowed<string>(System.Array.Empty<byte>(), new SessionWindow(0, 0))));
            Assert.Equal(
                exception.getMessage(),
                equalTo("Inner serializer is `null`. User code must use constructor " +
                    "`SessionWindowedSerializer(Serializer<T> inner)` instead of the no-arg constructor."));
        }

        [Fact]
        public void sessionWindowedSerializerShouldThrowNpeOnSerializingBaseKeyIfNotInitializedProperly()
        {
            SessionWindowedSerializer<byte[]> serializer = new SessionWindowedSerializer<>();
            NullPointerException exception = Assert.Throws(
        NullPointerException),
                () => serializer.Serialize.AseKey("topic", new Windowed<string>(System.Array.Empty<byte>(), new SessionWindow(0, 0))));
            Assert.Equal(
                exception.getMessage(),
                equalTo("Inner serializer is `null`. User code must use constructor " +
                    "`SessionWindowedSerializer(Serializer<T> inner)` instead of the no-arg constructor."));
        }

        [Fact]
        public void sessionWindowedDeserializerShouldThrowNpeIfNotInitializedProperly()
        {
            SessionWindowedDeserializer<byte[]> deserializer = new SessionWindowedDeserializer<>();
            NullPointerException exception = Assert.Throws(
        NullPointerException),
                () => deserializer.Deserialize("topic", System.Array.Empty<byte>()));
            Assert.Equal(
                exception.getMessage(),
                equalTo("Inner deserializer is `null`. User code must use constructor " +
                    "`SessionWindowedDeserializer(Deserializer<T> inner)` instead of the no-arg constructor."));
        }

        [Fact]
        public void timeWindowedSerializerShouldNotThrowOnCloseIfNotInitializedProperly()
        {
            new TimeWindowedSerializer<>().Close();
        }

        [Fact]
        public void timeWindowedDeserializerShouldNotThrowOnCloseIfNotInitializedProperly()
        {
            new TimeWindowedDeserializer<>().Close();
        }

        [Fact]
        public void sessionWindowedSerializerShouldNotThrowOnCloseIfNotInitializedProperly()
        {
            new SessionWindowedSerializer<>().Close();
        }

        [Fact]
        public void sessionWindowedDeserializerShouldNotThrowOnCloseIfNotInitializedProperly()
        {
            new SessionWindowedDeserializer<>().Close();
        }
    }
}
