using Confluent.Kafka;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Clients
{
    public class DefaultKafkaClientSupplier : IKafkaClientSupplier
    {
        public IAdminClient GetAdminClient(Dictionary<string, string> config)
        {
            // create a new client upon each call; but expect this call to be only triggered once so this should be fine
            var convertedConfig = config.ToDictionary(k => k.Key, v => v.Value.ToString());

            return new AdminClientBuilder(convertedConfig)
                .Build();
        }

        public IProducer<byte[], byte[]> getProducer(Dictionary<string, string> config)
        {
            var convertedConfig = config.ToDictionary(k => k.Key, v => v.Value.ToString());

            return new ProducerBuilder<byte[], byte[]>(convertedConfig)
                .Build();
        }

        public IConsumer<byte[], byte[]> getConsumer(Dictionary<string, string> config)
        {
            var convertedConfig = config.ToDictionary(k => k.Key, v => v.Value.ToString());

            return new ConsumerBuilder<byte[], byte[]>(convertedConfig)
                .Build();
        }

        public IConsumer<byte[], byte[]> getRestoreConsumer(Dictionary<string, string> config)
        {
            var convertedConfig = config.ToDictionary(k => k.Key, v => v.Value.ToString());

            return new ConsumerBuilder<byte[], byte[]>(convertedConfig)
                .Build();
        }

        public IConsumer<byte[], byte[]> getGlobalConsumer(Dictionary<string, string> config)
        {
            var convertedConfig = config.ToDictionary(k => k.Key, v => v.Value.ToString());

            return new ConsumerBuilder<byte[], byte[]>(convertedConfig)
                .Build();
        }
    }
}