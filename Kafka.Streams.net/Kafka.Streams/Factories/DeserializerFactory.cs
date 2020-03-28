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
        {
            var result = this.services.GetRequiredService(typeof(IDeserializer<K>));
            return (IDeserializer<K>)result;
        }
    }
}
