
using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.State.Sessions;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream
{
    public class SessionWindowedDeserializer<T> : IDeserializer<IWindowed<T>>
    {
        private readonly IServiceProvider services;
        private IDeserializer<T>? inner;

        public SessionWindowedDeserializer(IServiceProvider services)
        {
            this.services = services ?? throw new ArgumentNullException(nameof(services));
        }

        public SessionWindowedDeserializer(IServiceProvider services, IDeserializer<T>? inner)
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

            if (this.inner == null)
            {
                string propertyName = isKey
                    ? StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS
                    : StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS;

                string value = configs[propertyName] ?? throw new ArgumentNullException(nameof(configs));
                try
                {
                    Type serdeSerializerType = Type.GetType(value);
                    if (serdeSerializerType != typeof(Serde<T>))
                    {
                        throw new InvalidOperationException("Attempted to retrieve default deserializer, but type doesn't match.");
                    }

                    var serde = (ISerde<T>)ActivatorUtilities.CreateInstance(this.services, serdeSerializerType);
                    serde.Configure(configs, isKey);
                    this.inner = serde.Deserializer;
                }
                catch (TypeAccessException)
                {
                    //throw new Exception(propertyName, value, "Serde " + value + " could not be found.");
                }
            }
        }

        public IWindowed<T> Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            var topic = context.Topic;

            //WindowedSerdes.verifyInnerDeserializerNotNull<T>(inner, this);

            if (data == null || data.Length == 0)
            {
                return null;
            }

            // for either key or value, their schema is the same hence we will just use session key schema
            return SessionKeySchema.From(data, this.inner, topic);
        }


        public void Close()
        {
            if (this.inner != null)
            {
                //inner.Close();
            }
        }

        // Only for testing
        public IDeserializer<T> InnerDeserializer()
        {
            return this.inner;
        }
    }
}
