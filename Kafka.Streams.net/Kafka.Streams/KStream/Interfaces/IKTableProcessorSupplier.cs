using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public interface IKTableProcessorSupplier<K, V, T> : IProcessorSupplier<K, IChange<V>>
    {
        IKTableValueGetterSupplier<K, T> View();

        void EnableSendingOldValues();
    }
}
