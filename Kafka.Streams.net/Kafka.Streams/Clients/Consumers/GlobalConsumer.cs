using Confluent.Kafka;
using Kafka.Streams.Configs;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Clients.Consumers
{
    public class GlobalConsumer : BaseConsumer<byte[], byte[]>
    {
        public GlobalConsumer(
            ILogger<GlobalConsumer> logger,
            StreamsConfig config,
            ConsumerBuilder<byte[], byte[]>? builder = null)
            : base(logger, config.GetGlobalConsumerConfigs(config.ClientId), builder)
        {
        }
    }
}
