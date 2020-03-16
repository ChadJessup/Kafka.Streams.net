using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public interface IKTableProcessorSupplier<K, V, T> : IProcessorSupplier<K, Change<V>>
    {
        IKTableValueGetterSupplier<K, T> view();

        void enableSendingOldValues();
    }
}
