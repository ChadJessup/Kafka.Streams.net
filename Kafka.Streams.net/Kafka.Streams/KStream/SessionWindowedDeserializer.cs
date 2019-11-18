
//    public class SessionWindowedDeserializer<T> : IDeserializer<Windowed<T>>
//    {

//        private IDeserializer<T> inner;

//        // Default constructor needed by Kafka
//        public SessionWindowedDeserializer() { }

//        public SessionWindowedDeserializer(IDeserializer<T> inner)
//        {
//            this.inner = inner;
//        }



//        public void configure(Dictionary<string, object> configs, bool isKey)
//        {
//            if (inner == null)
//            {
//                string propertyName = isKey ? StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS : StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS;
//                string value = (string)configs[propertyName];
//                try
//                {

//                    inner = Serde.cast(Utils.newInstance(value, Serde)).Deserializer();
//                    inner.configure(configs, isKey);
//                }
//                catch (ClassNotFoundException e)
//                {
//                    throw new ConfigException(propertyName, value, "Serde " + value + " could not be found.");
//                }
//            }
//        }


//        public Windowed<T> deserialize(string topic, byte[] data)
//        {
//            WindowedSerdes.verifyInnerDeserializerNotNull(inner, this);

//            if (data == null || data.Length == 0)
//            {
//                return null;
//            }

//            // for either key or value, their schema is the same hence we will just use session key schema
//            return SessionKeySchema.from(data, inner, topic);
//        }


//        public void close()
//        {
//            if (inner != null)
//            {
//                inner.close();
//            }
//        }

//        // Only for testing
//        IDeserializer<T> innerDeserializer()
//        {
//            return inner;
//        }
//    }
//}