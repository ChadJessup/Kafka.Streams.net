
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream.Internals;

//namespace Kafka.Streams.KStream
//{
//    public class SessionWindowedSerde<T> : WrapperSerde<IWindowed<T>>
//    {
//        // Default constructor needed for reflection object creation
//        public SessionWindowedSerde()
//            : base(
//                  new SessionWindowedSerializer<T>(),
//                  new SessionWindowedDeserializer<T>())
//        {
//        }

//        public SessionWindowedSerde(ISerde<T> inner)
//            : base(
//                  new SessionWindowedSerializer<T>(inner.Serializer),
//                  new SessionWindowedDeserializer<T>(inner.Deserializer))
//        {
//        }
//    }
//}
