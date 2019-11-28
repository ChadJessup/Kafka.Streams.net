using Confluent.Kafka;
using Kafka.Streams.Clients.Consumers;
using Kafka.Streams.Configs;
using Kafka.Streams.Processors.Interfaces;
using System.Linq;

namespace Kafka.Streams.Clients
{
    public class DefaultKafkaClientSupplier : IKafkaClientSupplier
    {
        private readonly GlobalConsumer globalConsumer;

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

        public IConsumer<byte[], byte[]> getConsumer(ConsumerConfig config, IConsumerRebalanceListener rebalanceListener)
        {
            var convertedConfig = config
                .Where(kvp => kvp.Key != null && kvp.Value != null)
                .ToDictionary(k => k.Key, v => v.Value.ToString());

            var builder = new ConsumerBuilder<byte[], byte[]>(convertedConfig);

            if (rebalanceListener != null)
            {
                builder.SetPartitionsAssignedHandler(rebalanceListener.OnPartitionsAssigned);
                builder.SetPartitionsRevokedHandler(rebalanceListener.OnPartitionsRevoked);
            }

            return builder.Build();
        }

        public IConsumer<byte[], byte[]> GetRestoreConsumer(ConsumerConfig config)
        {
            //var convertedConfig = config.ToDictionary(k => k.Key, v => v.Value.ToString());

            return new ConsumerBuilder<byte[], byte[]>(config)
                .Build();
        }

        public GlobalConsumer GetGlobalConsumer()
            => this.globalConsumer;
    }
}