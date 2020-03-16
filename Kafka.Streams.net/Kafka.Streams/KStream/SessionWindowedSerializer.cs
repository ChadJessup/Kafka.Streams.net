using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.KStream.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream
{
    public class SessionWindowedSerializer<T> : IWindowedSerializer<T>
    {
        private ISerializer<T> inner;

        // Default constructor needed by Kafka
        public SessionWindowedSerializer() { }

        public SessionWindowedSerializer(ISerializer<T> inner)
        {
            this.inner = inner;
        }

        public void configure(Dictionary<string, string?> configs, bool isKey)
        {
            if (inner == null)
            {
                string propertyName = isKey
                    ? StreamsConfigPropertyNames.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS
                    : StreamsConfigPropertyNames.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS;

                string value = (string)configs[propertyName];
                try
                {
                    // inner = Serde.cast(Utils.newInstance(value, Serde)).Serializer();
                    // inner.configure(configs, isKey);
                }
                catch (Exception e)
                {
                    // throw new Exception(propertyName, value, "Serde " + value + " could not be found.");
                }
            }
        }

        public byte[]? serialize(string topic, Windowed<T> data)
        {
            // WindowedSerdes.verifyInnerSerializerNotNull<T>(inner, this);

            if (data == null)
            {
                return null;
            }

            // for either key or value, their schema is the same hence we will just use session key schema
            return null; // SessionKeySchema.toBinary(data, inner, topic);
        }

        public void close()
        {
            if (inner != null)
            {
                // inner.close();
            }
        }

        public byte[] SerializeBaseKey(string topic, Windowed<T> data)
        {
            WindowedSerdes.verifyInnerSerializerNotNull<T>(inner, (ISerializer<T>)this);

            return inner.Serialize(data.Key, new SerializationContext(MessageComponentType.Key, topic));
        }

        // Only for testing
        public ISerializer<T> innerSerializer()
        {
            return inner;
        }

        public byte[] Serialize(Windowed<T> data, SerializationContext context)
        {
            throw new NotImplementedException();
        }
    }
}
