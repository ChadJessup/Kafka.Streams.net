
//using Confluent.Kafka;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class WrapperSerde<T> : Serde<T>
//    {
//        public ISerializer<T> serializer { get; private set; }
//        public IDeserializer<T> deserializer { get; private set; }

//        public WrapperSerde(
//            ISerializer<T> serializer,
//            IDeserializer<T> deserializer)
//        {
//            this.serializer = serializer;
//            this.deserializer = deserializer;
//        }

//        public void configure(
//            Dictionary<string, object> configs,
//            bool isKey)
//        {
//            //serializer.configure(configs, isKey);
//            //deserializer.configure(configs, isKey);
//        }

//        public void close()
//        {
//            //serializer.close();
//            //deserializer.close();
//        }
//    }
//}