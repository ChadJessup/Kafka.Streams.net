using Confluent.Kafka;
using Kafka.Streams.Clients;
using Kafka.Streams.Clients.Consumers;
using Kafka.Streams.Configs;
using Kafka.Streams.Processors.Interfaces;

namespace Kafka.Streams.Tests
{
    public class MockClientSupplier : IKafkaClientSupplier
    {
        public IAdminClient GetAdminClient(AdminClientConfig config)
        {
            throw new System.NotImplementedException();
        }

        public IAdminClient GetAdminClient(StreamsConfig config)
        {
            throw new System.NotImplementedException();
        }

        public IConsumer<byte[], byte[]> GetConsumer(ConsumerConfig config, IConsumerRebalanceListener rebalanceListener)
        {
            throw new System.NotImplementedException();
        }

        public GlobalConsumer GetGlobalConsumer()
        {
            throw new System.NotImplementedException();
        }

        public IProducer<byte[], byte[]> GetProducer(ProducerConfig config)
        {
            throw new System.NotImplementedException();
        }

        public RestoreConsumer GetRestoreConsumer(RestoreConsumerConfig config)
        {
            throw new System.NotImplementedException();
        }
    }
}