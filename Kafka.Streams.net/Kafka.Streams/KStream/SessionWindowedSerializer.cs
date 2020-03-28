using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream
{
    public class SessionWindowedSerializer<T> : IWindowedSerializer<T>
    {
        private readonly IServiceProvider services;
        private ISerializer<T>? inner;

        public SessionWindowedSerializer(IServiceProvider services)
        {
            this.services = services ?? throw new ArgumentNullException(nameof(services));
        }

        public SessionWindowedSerializer(IServiceProvider services, ISerializer<T>? inner)
            : this(services)
        {
            this.inner = inner;
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
                    Type serdeSerializerType = Type.GetType(value);
                    if (serdeSerializerType != typeof(Serde<T>))
                    {
                        throw new InvalidOperationException("Attempted to retrieve default serializer, but type doesn't match.");
                    }

                    var serde = (ISerde<T>)ActivatorUtilities.CreateInstance(this.services, serdeSerializerType);
                    serde.Configure(configs, isKey);
                    this.inner = serde.Serializer;
                }
                catch (TypeAccessException)
                {
                    //throw new Exception(propertyName, value, "Serde " + value + " could not be found.");
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
