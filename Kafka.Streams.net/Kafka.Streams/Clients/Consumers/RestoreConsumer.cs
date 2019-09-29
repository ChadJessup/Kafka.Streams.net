using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Clients.Consumers
{
    public class RestoreConsumer : BaseConsumer<byte[], byte[]>
    {
        public RestoreConsumer(
            ILogger<RestoreConsumer> logger,
            ConsumerConfig configs,
            ConsumerBuilder<byte[], byte[]>? builder = null)
            : base(logger, configs, builder)
        {
        }
    }
}
