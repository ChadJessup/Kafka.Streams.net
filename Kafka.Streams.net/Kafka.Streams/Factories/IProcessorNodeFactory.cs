using System.Collections.Generic;

namespace Kafka.Streams.Factories
{
    public interface IProcessorNodeFactory : INodeFactory
    {
        HashSet<string> stateStoreNames { get; }
    }
}