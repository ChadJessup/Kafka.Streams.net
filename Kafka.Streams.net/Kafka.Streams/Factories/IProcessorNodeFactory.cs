using System.Collections.Generic;

namespace Kafka.Streams.Factories
{
    public interface IProcessorNodeFactory : INodeFactory
    {
        void AddStateStore(string stateStoreName);
        HashSet<string> stateStoreNames { get; }
    }
}