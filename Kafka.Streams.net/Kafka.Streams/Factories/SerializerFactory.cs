using Confluent.Kafka;
using Kafka.Streams.Configs;
using System;

namespace Kafka.Streams.Factories
{
    public class SerializerFactory
    {
        private readonly IServiceProvider services;

        public SerializerFactory(StreamsConfig config, IServiceProvider services)
        {
            this.services = services;
        }

        public ISerializer<K> GetSerializer<K>()
        {
            return (ISerializer<K>)this.services.GetService(typeof(ISerializer<K>));
        }
    }
}