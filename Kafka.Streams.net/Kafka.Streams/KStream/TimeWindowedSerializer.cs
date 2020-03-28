using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream
{
    /**
     *  The inner serde can be specified by setting the property
     *  {@link StreamsConfig#DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS} or
     *  {@link StreamsConfig#DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS}
     *  if the no-arg constructor is called and hence it is not passed during initialization.
     */
    public class TimeWindowedSerializer<T> : IWindowedSerializer<T>
    {
        private readonly IServiceProvider services;
        private ISerializer<T>? inner;

        public TimeWindowedSerializer(IServiceProvider services)
        {
            this.services = services ?? throw new ArgumentNullException(nameof(services));
        }

        public TimeWindowedSerializer(IServiceProvider services, ISerializer<T>? inner)
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

        public byte[]? Serialize(string topic, Windowed<T> data)
        {
            //WindowedSerdes.verifyInnerSerializerNotNull(inner, this);

            if (data == null)
            {
                return null;
            }

            return null; // WindowKeySchema.toBinary(data, inner, topic);
        }


        public void Close()
        {
            if (inner != null)
            {
                //inner.close();
            }
        }


        public byte[] SerializeBaseKey(string topic, Windowed<T> data)
        {
            return null;
            //WindowedSerdes.verifyInnerSerializerNotNull(inner, this);

            //return inner.Serialize(topic, data.key);
        }

        // Only for testing
        public ISerializer<T> InnerSerializer()
        {
            return inner;
        }

        public byte[] Serialize(Windowed<T> data, SerializationContext context)
        {
            throw new NotImplementedException();
        }
    }
}