
//using Confluent.Kafka;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class ChangedDeserializer<T> : IDeserializer<Change<T>>
//    {

//        private static int NEWFLAG_SIZE = 1;

//        private IDeserializer<T> inner;

//        public ChangedDeserializer(IDeserializer<T> inner)
//        {
//            this.inner = inner;
//        }

//        public IDeserializer<T> inner()
//        {
//            return inner;
//        }

//        public void setInner(IDeserializer<T> inner)
//        {
//            this.inner = inner;
//        }


//        public Change<T> deserialize(string topic, Headers headers, byte[] data)
//        {

//            byte[] bytes = new byte[data.Length - NEWFLAG_SIZE];

//            System.arraycopy(data, 0, bytes, 0, bytes.Length);

//            if (new ByteBuffer().Wrap(data)[data.Length - NEWFLAG_SIZE] != 0)
//            {
//                return new Change<>(inner.Deserialize(topic, headers, bytes), null);
//            }
//            else
//            {

//                return new Change<>(null, inner.Deserialize(topic, headers, bytes));
//            }
//        }


//        public Change<T> deserialize(string topic, byte[] data)
//        {
//            return deserialize(topic, null, data);
//        }


//        public void close()
//        {
//            inner.close();
//        }
//    }
//}