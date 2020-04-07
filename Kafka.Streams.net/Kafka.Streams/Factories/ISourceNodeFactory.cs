using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Kafka.Streams.Factories
{
    public interface ISourceNodeFactory : INodeFactory
    {
        List<string> Topics { get; }
        List<string> GetTopics(List<string> subscribedTopics);
        Regex Pattern { get; }
    }
}
