using Confluent.Kafka;
using Kafka.Streams.Configs;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace Kafka.Streams.Factories
{
    public class DeserializerFactory
    {
        private readonly IServiceProvider services;

        public DeserializerFactory(StreamsConfig config, IServiceProvider services)
        {
            if (config is null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            this.services = services ?? throw new ArgumentNullException(nameof(services));
        }

        public IDeserializer<K> GetDeserializer<K>()
            => default(K) switch
            {
                int _ => (IDeserializer<K>)Deserializers.Int32,
                byte[] _ => (IDeserializer<K>)Deserializers.ByteArray,
                string _ => (IDeserializer<K>)Deserializers.Utf8,
                _ => (IDeserializer<K>)this.services.GetRequiredService(typeof(IDeserializer<K>)),
            };
    }
}
