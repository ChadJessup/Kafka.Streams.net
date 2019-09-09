using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Consumers
{
    public class GlobalConsumer : BaseConsumer<byte[], byte[]>
    {
        public GlobalConsumer(
            ILogger<GlobalConsumer> logger,
            ConsumerConfig configs,
            ConsumerBuilder<byte[], byte[]>? builder = null)
            : base(logger, configs, builder)
        {
        }
    }
}
