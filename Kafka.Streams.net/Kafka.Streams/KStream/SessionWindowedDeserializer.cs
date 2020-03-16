
using Confluent.Kafka;
using Kafka.Streams.Configs;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream
{
    public class SessionWindowedDeserializer<T> : IDeserializer<Windowed<T>>
    {

        private IDeserializer<T> inner;

        // Default constructor needed by Kafka
        public SessionWindowedDeserializer() { }

        public SessionWindowedDeserializer(IDeserializer<T> inner)
        {
            this.inner = inner;
        }

        public void configure(Dictionary<string, string?> configs, bool isKey)
        {
            if (configs is null)
            {
                throw new ArgumentNullException(nameof(configs));
            }

            if (inner == null)
            {
                string propertyName = isKey
                    ? StreamsConfigPropertyNames.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS
                    : StreamsConfigPropertyNames.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS;

                var value = configs[propertyName];
                // try
                // {
                // 
                //     inner = Serde.cast(Utils.newInstance(value, Serde)).Deserializer();
                //     inner.configure(configs, isKey);
                // }
                // catch (ClassNotFoundException e)
                // {
                //     throw new ConfigException(propertyName, value, "Serde " + value + " could not be found.");
                // }
            }
        }

        public Windowed<T> Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            var topic = context.Topic;

            //WindowedSerdes.verifyInnerDeserializerNotNull<T>(inner, this);

            if (data == null || data.Length == 0)
            {
                return null;
            }

            // for either key or value, their schema is the same hence we will just use session key schema
            return null; // SessionKeySchema.from(data, inner, topic);
        }


        public void close()
        {
            if (inner != null)
            {
                //inner.Close();
            }
        }

        // Only for testing
        public IDeserializer<T> innerDeserializer()
        {
            return inner;
        }
    }
}
