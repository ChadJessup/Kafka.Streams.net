using Confluent.Kafka;
using Kafka.Streams.Clients.Consumers;
using Kafka.Streams.Configs;
using Kafka.Streams.Processors.Interfaces;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;

namespace Kafka.Streams.Clients
{
    public class DefaultKafkaClientSupplier : IKafkaClientSupplier
    {
        private readonly GlobalConsumer globalConsumer;
        private readonly ILoggerFactory loggerFactory;
        private readonly ILogger<DefaultKafkaClientSupplier> logger;

        public DefaultKafkaClientSupplier(ILogger<DefaultKafkaClientSupplier> logger, ILoggerFactory loggerFactory)
        {
            this.logger = logger;
            this.loggerFactory = loggerFactory;
        }

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
                //.SetPartitioner((prod, partReq) =>
                //{
                //    return Partition.Any;
                //})
                .SetErrorHandler((p, r) =>
                {
                    Console.WriteLine(r.Reason);
                })
                .SetLogHandler((p, l) =>
                {
                    Console.WriteLine(l.Message);
                })
                .Build();
        }

        public IConsumer<byte[], byte[]> GetConsumer(ConsumerConfig config, IConsumerRebalanceListener rebalanceListener)
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

        public RestoreConsumer GetRestoreConsumer(ConsumerConfig config)
            => new RestoreConsumer(
                this.loggerFactory.CreateLogger<RestoreConsumer>(),
                config,
                new ConsumerBuilder<byte[], byte[]>(config));

        public GlobalConsumer GetGlobalConsumer()
            => this.globalConsumer;
    }
}