
using Confluent.Kafka;

namespace Kafka.Streams.KStream.Interfaces
{
    public interface IWindowedSerializer<T> : ISerializer<IWindowed<T>>
    {
        byte[] SerializeBaseKey(string topic, IWindowed<T> data);
    }
}
