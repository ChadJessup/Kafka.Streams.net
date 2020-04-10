using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.KStream;
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
            => default(K) switch
            {
                int _ => (ISerializer<K>)Serializers.Int32,
                byte[] _ => (ISerializer<K>)Serializers.ByteArray,
                string _ => (ISerializer<K>)Serializers.Utf8,
                _ => (ISerializer<K>)this.services.GetRequiredService(typeof(ISerializer<K>)),
            };
    }
}
