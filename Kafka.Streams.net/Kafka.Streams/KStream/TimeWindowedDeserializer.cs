using Confluent.Kafka;
using Kafka.Streams.Configs;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream
{
    public class TimeWindowedDeserializer<T> : IDeserializer<Windowed<T>>
    {
        private long windowSize;
        private bool isChangelogTopic;

        private IDeserializer<T> inner;

        // Default constructor needed by Kafka
        public TimeWindowedDeserializer()
            : this(null, long.MaxValue)
        {
        }

        // TODO: fix this part as last bits of KAFKA-4468
        public TimeWindowedDeserializer(IDeserializer<T> inner)
            : this(inner, long.MaxValue)
        {
        }

        public TimeWindowedDeserializer(IDeserializer<T> inner, long windowSize)
        {
            this.inner = inner;
            this.windowSize = windowSize;
            this.isChangelogTopic = false;
        }

        public long getWindowSize()
        {
            return this.windowSize;
        }

        public void configure(Dictionary<string, string> configs, bool isKey)
        {
            if (inner == null)
            {
                string propertyName = isKey
                    ? StreamsConfigPropertyNames.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS
                    : StreamsConfigPropertyNames.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS;

                string value = (string)configs[propertyName];
                try
                {
                    //inner = Serde.cast(Utils.newInstance(value, Serde)).Deserializer();
                    //inner.configure(configs, isKey);
                }
                catch (TypeAccessException e)
                {
                    //throw new Exception(propertyName, value, "Serde " + value + " could not be found.");
                }
            }
        }


        public Windowed<T> deserialize(string topic, byte[] data)
        {
            // WindowedSerdes.verifyInnerDeserializerNotNull<T>(inner, this);

            if (data == null || data.Length == 0)
            {
                return null;
            }

            // toStoreKeyBinary was used to serialize the data.
            if (this.isChangelogTopic)
            {
                return null; // WindowKeySchema.fromStoreKey(data, windowSize, inner, topic);
            }

            // toBinary was used to serialize the data
            return null; // WindowKeySchema.from(data, windowSize, inner, topic);
        }

        public void close()
        {
            if (inner != null)
            {
                // inner.Close();
            }
        }

        public void setIsChangelogTopic(bool isChangelogTopic)
        {
            this.isChangelogTopic = isChangelogTopic;
        }

        // Only for testing
        public IDeserializer<T> innerDeserializer()
        {
            return inner;
        }

        public Windowed<T> Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            throw new NotImplementedException();
        }
    }
}
