using Confluent.Kafka;
using Kafka.Streams.Configs;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Clients.Consumers
{
    public class RestoreConsumer : BaseConsumer<byte[], byte[]>
    {
        public RestoreConsumer(ILogger<RestoreConsumer> logger, RestoreConsumerConfig configs)
            : base(logger, configs)
        {
        }

        public RestoreConsumer(ILogger<RestoreConsumer> logger, IConsumer<byte[], byte[]> consumer)
            : base(logger, consumer)
        {
        }

        public RestoreConsumer(
            ILogger<RestoreConsumer> logger,
            RestoreConsumerConfig configs,
            ConsumerBuilder<byte[], byte[]>? builder = null)
            : base(logger, configs, builder)
        {
        }
    }

    public class RestoreConsumerConfig : ConsumerConfig
    {
        public RestoreConsumerConfig(ConsumerConfig config)
            : base(config)
        {
        }

        public RestoreConsumerConfig(StreamsConfig config)
            : base(config.GetRestoreConsumerConfigs())
        {
        }
    }
}
