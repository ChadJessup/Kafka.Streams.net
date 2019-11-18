
//    public class TimeWindowedDeserializer<T> : IDeserializer<Windowed<T>>
//    {
//        private long windowSize;
//        private bool isChangelogTopic;

//        private IDeserializer<T> inner;

//        // Default constructor needed by Kafka
//        public TimeWindowedDeserializer()
//            : this(null, long.MaxValue)
//        {
//        }

//        // TODO: fix this part as last bits of KAFKA-4468
//        public TimeWindowedDeserializer(IDeserializer<T> inner)
//            : this(inner, long.MaxValue)
//        {
//        }

//        public TimeWindowedDeserializer(IDeserializer<T> inner, long windowSize)
//        {
//            this.inner = inner;
//            this.windowSize = windowSize;
//            this.isChangelogTopic = false;
//        }

//        public long getWindowSize()
//        {
//            return this.windowSize;
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
//                catch (System.TypeAccessException e)
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

//            // toStoreKeyBinary was used to serialize the data.
//            if (this.isChangelogTopic)
//            {
//                return WindowKeySchema.fromStoreKey(data, windowSize, inner, topic);
//            }

//            // toBinary was used to serialize the data
//            return WindowKeySchema.from(data, windowSize, inner, topic);
//        }


//        public void close()
//        {
//            if (inner != null)
//            {
//                inner.close();
//            }
//        }

//        public void setIsChangelogTopic(bool isChangelogTopic)
//        {
//            this.isChangelogTopic = isChangelogTopic;
//        }

//        // Only for testing
//        IDeserializer<T> innerDeserializer()
//        {
//            return inner;
//        }
//    }
//}