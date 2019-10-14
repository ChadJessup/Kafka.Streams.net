using Confluent.Kafka;
using Kafka.Streams.Configs;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Clients
{
    public class DefaultKafkaClientSupplier : IKafkaClientSupplier
    {
        public IAdminClient GetAdminClient(StreamsConfig config)
            => this.GetAdminClient(config.GetAdminConfigs(config.ClientId));

        public IAdminClient GetAdminClient(AdminClientConfig config)
        {
            // create a new client upon each call; but expect this call to be only triggered once so this should be fine
            //var convertedConfig = config.ToDictionary(k => k.Key, v => v.Value.ToString());

            return new AdminClientBuilder(config)
                .Build();
        }

        public IProducer<byte[], byte[]> getProducer(ProducerConfig config)
        {
            //var convertedConfig = config.ToDictionary(k => k.Key, v => v.Value.ToString());

            return new ProducerBuilder<byte[], byte[]>(config)
                .Build();
        }

        public IConsumer<byte[], byte[]> getConsumer(ConsumerConfig config)
        {
            var convertedConfig = config
                .Where(kvp => kvp.Key != null && kvp.Value != null)
                .ToDictionary(k => k.Key, v => v.Value.ToString());

            return new ConsumerBuilder<byte[], byte[]>(convertedConfig)
                .Build();
        }

        public IConsumer<byte[], byte[]> GetRestoreConsumer(ConsumerConfig config)
        {
            //var convertedConfig = config.ToDictionary(k => k.Key, v => v.Value.ToString());

            return new ConsumerBuilder<byte[], byte[]>(config)
                .Build();
        }

        public IConsumer<byte[], byte[]> getGlobalConsumer(ConsumerConfig config)
        {
            //var convertedConfig = config.ToDictionary(k => k.Key, v => v.Value.ToString());

            return new ConsumerBuilder<byte[], byte[]>(config)
                .Build();
        }
    }
}