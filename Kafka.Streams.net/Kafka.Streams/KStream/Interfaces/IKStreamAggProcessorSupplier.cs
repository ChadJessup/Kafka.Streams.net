using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Interfaces
{
    public interface IKStreamAggProcessorSupplier
    {
    }

    public interface IKStreamAggProcessorSupplier<K, RK, V, T> : IProcessorSupplier<K, V>, IKStreamAggProcessorSupplier
    {
        IProcessorSupplier<RK, T> GetSwappedProcessorSupplier();
        IKTableValueGetterSupplier<RK, T> view();
        void enableSendingOldValues();
    }
}