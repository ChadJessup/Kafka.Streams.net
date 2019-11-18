using System.Collections.Generic;

namespace Kafka.Streams.Factories
{
    public interface ISourceNodeFactory : INodeFactory
    {
        List<string> Topics { get; }
        List<string> GetTopics(List<string> subscribedTopics);
    }
}