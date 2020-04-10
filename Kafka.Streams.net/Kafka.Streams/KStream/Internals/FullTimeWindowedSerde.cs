
//using Kafka.Streams.Interfaces;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class FullTimeWindowedSerde<T> : WrapperSerde<IWindowed<T>>
//    {
//        public FullTimeWindowedSerde(ISerde<T> inner, long windowSize)
//            : base(
//                new TimeWindowedSerializer<T>(inner.Serializer),
//                new TimeWindowedDeserializer<T>(inner.Deserializer, windowSize))
//        {
//        }
//    }
//}
