using Kafka.Streams.State;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Interfaces
{
    public interface IGlobalStateManager : IStateManager
    {
        void SetGlobalProcessorContext(IInternalProcessorContext processorContext);

        /**
         * @throws InvalidOperationException If store gets registered after initialized is already finished
         * @throws StreamsException if the store's change log does not contain the partition
         */
        HashSet<string> Initialize();
    }
}