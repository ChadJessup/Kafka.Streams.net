
//    public class SessionWindowedSerializer<T> : IWindowedSerializer<T>
//    {
//        private ISerializer<T> inner;

//        // Default constructor needed by Kafka
//        public SessionWindowedSerializer() { }

//        public SessionWindowedSerializer(ISerializer<T> inner)
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
//                    inner = Serde.cast(Utils.newInstance(value, Serde)).Serializer();
//                    inner.configure(configs, isKey);
//                }
//                catch (Exception e)
//                {
//                    throw new Exception(propertyName, value, "Serde " + value + " could not be found.");
//                }
//            }
//        }


//        public byte[] serialize(string topic, Windowed<T> data)
//        {
//            WindowedSerdes.verifyInnerSerializerNotNull<T>(inner, this);

//            if (data == null)
//            {
//                return null;
//            }

//            // for either key or value, their schema is the same hence we will just use session key schema
//            return SessionKeySchema.toBinary(data, inner, topic);
//        }


//        public void close()
//        {
//            if (inner != null)
//            {
//                inner.close();
//            }
//        }


//        public byte[] serializeBaseKey(string topic, Windowed<T> data)
//        {
//            WindowedSerdes.verifyInnerSerializerNotNull(inner, this);

//            return inner.Serialize(topic, data.key);
//        }

//        // Only for testing
//        ISerializer<T> innerSerializer()
//        {
//            return inner;
//        }
//    }
//}
