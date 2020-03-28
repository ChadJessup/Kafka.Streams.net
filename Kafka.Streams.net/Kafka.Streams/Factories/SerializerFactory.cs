using Confluent.Kafka;
using Kafka.Streams.Configs;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace Kafka.Streams.Factories
{
    public class SerializerFactory
    {
        private readonly IServiceProvider services;

        public SerializerFactory(StreamsConfig config, IServiceProvider services)
        {
            if (config is null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            this.services = services ?? throw new ArgumentNullException(nameof(services));
        }

        public ISerializer<K> GetSerializer<K>()
        {
            return (ISerializer<K>)this.services.GetRequiredService(typeof(ISerializer<K>));
        }
    }
}