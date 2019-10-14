using Confluent.Kafka;

namespace Kafka.Streams.Clients
{
    public interface IProducerSupplier
    {
        IProducer<byte[], byte[]> Get();
    }
}