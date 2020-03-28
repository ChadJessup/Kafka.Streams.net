using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream
{
    public class TimeWindowedDeserializer<T> : IDeserializer<Windowed<T>>
    {
        private long windowSize;
        private bool isChangelogTopic;

        private IDeserializer<T>? inner;
        private readonly IServiceProvider services;

        // Default constructor needed by Kafka
        public TimeWindowedDeserializer(IServiceProvider services)
            : this(services, null, long.MaxValue)
        {
        }

        // TODO: fix this part as last bits of KAFKA-4468
        public TimeWindowedDeserializer(
            IServiceProvider services,
            IDeserializer<T>? inner)
            : this(services, inner, long.MaxValue)
        {
        }

        public TimeWindowedDeserializer(
            IServiceProvider services,
            IDeserializer<T>? inner,
            long windowSize)
        {
            this.inner = inner;
            this.windowSize = windowSize;
            this.isChangelogTopic = false;
            this.services = services;
        }

        public long getWindowSize()
        {
            return this.windowSize;
        }

        public void Configure(Dictionary<string, string?> configs, bool isKey)
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

                string value = configs[propertyName] ?? throw new ArgumentNullException(nameof(configs));
                try
                {
                    Type serdeDeserializerType = Type.GetType(value);
                    if (serdeDeserializerType != typeof(Serde<T>))
                    {
                        throw new InvalidOperationException("Attempted to retrieve default deserializer, but type doesn't match.");
                    }

                    var serde = (ISerde<T>)ActivatorUtilities.CreateInstance(this.services, serdeDeserializerType);
                    serde.Configure(configs, isKey);
                    this.inner = serde.Deserializer;
                }
                catch (TypeAccessException)
                {
                    //throw new Exception(propertyName, value, "Serde " + value + " could not be found.");
                }
            }
        }

        public Windowed<T> Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            //WindowedSerdes.verifyInnerDeserializerNotNull<T>(inner, this);

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
            return this.inner;
        }
    }
}
