
using Confluent.Kafka;

namespace Kafka.Streams.KStream.Interfaces
{
    public interface IWindowedSerializer<T> : ISerializer<Windowed<T>>
    {
        byte[] SerializeBaseKey(string topic, Windowed<T> data);
    }
}
