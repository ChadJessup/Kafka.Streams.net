
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream.Internals;

//namespace Kafka.Streams.KStream
//{
//    public class TimeWindowedSerde<T> : WrapperSerde<IWindowed<T>>
//    {
//        // Default constructor needed for reflection object creation

//        public TimeWindowedSerde()
//            : base(new TimeWindowedSerializer<>(), new TimeWindowedDeserializer<>())
//        {
//        }

//        public TimeWindowedSerde(Serde<T> inner)
//            : base(new TimeWindowedSerializer<>(inner.serializer()), new TimeWindowedDeserializer<>(inner.deserializer()))
//        {
//        }

//        // This constructor can be used for serialize/deserialize a windowed topic
//        public TimeWindowedSerde(ISerde<T> inner, long windowSize)
//            : base(new TimeWindowedSerializer<T>(inner.serializer()), new TimeWindowedDeserializer<>(inner.deserializer(), windowSize))
//        {
//        }

//        // Helper method for users to specify whether the input topic is a changelog topic for deserializing the key properly.
//        public TimeWindowedSerde<T> forChangelog(bool isChangelogTopic)
//        {
//            TimeWindowedDeserializer<T> deserializer = (TimeWindowedDeserializer<T>)this.deserializer;
//            deserializer.setIsChangelogTopic(isChangelogTopic);
//            return this;
//        }
//    }
//}
