
namespace Kafka.Streams.KStream.Interfaces
{
    public interface IInternalNameProvider
    {
        string NewProcessorName(string prefix);

        string NewStoreName(string prefix);
    }
}
